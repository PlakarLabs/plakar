package snapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/packfile"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/repository/state"
	"github.com/PlakarLabs/plakar/snapshot/header"
	"github.com/PlakarLabs/plakar/snapshot/index"
	"github.com/PlakarLabs/plakar/snapshot/metadata"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type Snapshot struct {
	repository *repository.Repository
	stateDelta *state.State

	SkipDirs []string

	Header     *header.Header
	Index      *index.Index
	Filesystem *vfs.Filesystem

	Metadata *metadata.Metadata

	packerChan     chan interface{}
	packerChanDone chan bool
}

type PackerMsg struct {
	Timestamp time.Time
	Type      uint8
	Checksum  [32]byte
	Data      []byte
}

func New(repo *repository.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Create", time.Since(t0))
	}()

	fs, err := vfs.NewFilesystem()
	if err != nil {
		return nil, err
	}

	idx, err := index.NewIndex()
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{
		repository: repo,
		stateDelta: state.New(),

		Header:     header.NewHeader(indexID),
		Index:      idx,
		Filesystem: fs,
		Metadata:   metadata.New(),

		packerChan:     make(chan interface{}, runtime.NumCPU()*2+1),
		packerChanDone: make(chan bool),
	}
	snapshot.stateDelta.Metadata.Extends = repo.State().Metadata.Extends

	go func() {
		wg := sync.WaitGroup{}
		for i := 0; i < runtime.NumCPU(); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var pack *packfile.PackFile
				var chunks map[[32]byte]struct{}
				var objects map[[32]byte]struct{}
				var files map[[32]byte]struct{}
				var directories map[[32]byte]struct{}
				var datas map[[32]byte]struct{}

				for msg := range snapshot.packerChan {
					if pack == nil {
						pack = packfile.New()
						chunks = make(map[[32]byte]struct{})
						objects = make(map[[32]byte]struct{})
						files = make(map[[32]byte]struct{})
						directories = make(map[[32]byte]struct{})
						datas = make(map[[32]byte]struct{})
					}

					if msg, ok := msg.(*PackerMsg); !ok {
						panic("received data with unexpected type")
					} else {
						logger.Trace("packer", "%s: PackerMsg(%d, %064x), dt=%s", snapshot.Header.GetIndexShortID(), msg.Type, msg.Checksum, time.Since(msg.Timestamp))
						pack.AddBlob(msg.Type, msg.Checksum, msg.Data)
						switch msg.Type {
						case packfile.TYPE_CHUNK:
							chunks[msg.Checksum] = struct{}{}
						case packfile.TYPE_OBJECT:
							objects[msg.Checksum] = struct{}{}
						case packfile.TYPE_FILE:
							files[msg.Checksum] = struct{}{}
						case packfile.TYPE_DIRECTORY:
							directories[msg.Checksum] = struct{}{}
						case packfile.TYPE_DATA:
							datas[msg.Checksum] = struct{}{}
						default:
							panic("received msg with unexpected blob type")
						}
					}

					if pack.Size() > uint32(repo.Configuration().PackfileSize) {
						objectsList := make([][32]byte, len(objects))
						for objectChecksum := range objects {
							objectsList = append(objectsList, objectChecksum)
						}
						chunksList := make([][32]byte, len(chunks))
						for chunkChecksum := range chunks {
							chunksList = append(chunksList, chunkChecksum)
						}
						filesList := make([][32]byte, len(files))
						for fileChecksum := range files {
							filesList = append(filesList, fileChecksum)
						}
						directoriesList := make([][32]byte, len(directories))
						for directoryChecksum := range directories {
							directoriesList = append(directoriesList, directoryChecksum)
						}
						datasList := make([][32]byte, len(datas))
						for dataChecksum := range datas {
							datasList = append(datasList, dataChecksum)
						}
						err := snapshot.PutPackfile(pack, objectsList, chunksList, filesList, directoriesList, datasList)
						if err != nil {
							panic(err)
						}
						pack = nil
					}
				}

				if pack != nil {
					objectsList := make([][32]byte, len(objects))
					for objectChecksum := range objects {
						objectsList = append(objectsList, objectChecksum)
					}
					chunksList := make([][32]byte, len(chunks))
					for chunkChecksum := range chunks {
						chunksList = append(chunksList, chunkChecksum)
					}
					filesList := make([][32]byte, len(files))
					for fileChecksum := range files {
						filesList = append(filesList, fileChecksum)
					}
					directoriesList := make([][32]byte, len(directories))
					for fileChecksum := range directories {
						directoriesList = append(directoriesList, fileChecksum)
					}
					datasList := make([][32]byte, len(datas))
					for dataChecksum := range datas {
						datasList = append(datasList, dataChecksum)
					}
					err := snapshot.PutPackfile(pack, objectsList, chunksList, filesList, directoriesList, datasList)
					if err != nil {
						panic(err)
					}
					pack = nil
				}
			}()
		}
		wg.Wait()
		snapshot.packerChanDone <- true
		close(snapshot.packerChanDone)
	}()

	logger.Trace("snapshot", "%s: New()", snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func Load(repo *repository.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Load", time.Since(t0))
	}()

	hdr, _, err := GetSnapshot(repo, indexID)
	if err != nil {
		return nil, err
	}

	var indexChecksum32 [32]byte
	copy(indexChecksum32[:], hdr.Index.Checksum[:])

	index, verifyChecksum, err := GetIndex(repo, indexChecksum32)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(verifyChecksum[:], hdr.Index.Checksum[:]) {
		return nil, fmt.Errorf("index mismatches hdr checksum")
	}

	var filesystemChecksum32 [32]byte
	copy(filesystemChecksum32[:], hdr.VFS.Checksum[:])

	filesystem, verifyChecksum, err := GetFilesystem(repo, filesystemChecksum32)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(verifyChecksum[:], hdr.VFS.Checksum[:]) {
		return nil, fmt.Errorf("filesystem mismatches hdr checksum")
	}

	var metadataChecksum32 [32]byte
	copy(metadataChecksum32[:], hdr.Metadata.Checksum[:])

	md, verifyChecksum, err := GetMetadata(repo, metadataChecksum32)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(verifyChecksum[:], hdr.Metadata.Checksum[:]) {
		return nil, fmt.Errorf("metadata mismatches hdr checksum")
	}

	snapshot := &Snapshot{}
	snapshot.repository = repo
	snapshot.Header = hdr
	snapshot.Index = index
	snapshot.Filesystem = filesystem
	snapshot.Metadata = md

	logger.Trace("snapshot", "%s: Load()", snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func Fork(repo *repository.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Fork", time.Since(t0))
	}()

	hdr, _, err := GetSnapshot(repo, indexID)
	if err != nil {
		return nil, err
	}

	index, verifyChecksum, err := GetIndex(repo, hdr.Index.Checksum)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(verifyChecksum[:], hdr.Index.Checksum[:]) {
		return nil, fmt.Errorf("index mismatches hdr checksum")
	}

	filesystem, verifyChecksum, err := GetFilesystem(repo, hdr.VFS.Checksum)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(verifyChecksum[:], hdr.VFS.Checksum[:]) {
		return nil, fmt.Errorf("filesystem mismatches hdr checksum")
	}

	snapshot := &Snapshot{
		repository: repo,

		Header:     hdr,
		Index:      index,
		Filesystem: filesystem,
	}
	snapshot.Header.IndexID = uuid.Must(uuid.NewRandom())

	logger.Trace("snapshot", "%s: Fork(): %s", indexID, snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func GetSnapshot(repo *repository.Repository, indexID uuid.UUID) (*header.Header, bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetSnapshot", time.Since(t0))
	}()
	logger.Trace("snapshot", "repository.GetSnapshot(%s)", indexID)

	buffer, err := repo.GetSnapshot(indexID)
	if err != nil {
		return nil, false, err
	}

	hdr, err := header.NewFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	return hdr, false, nil
}

func GetIndex(repo *repository.Repository, checksum [32]byte) (*index.Index, [32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetIndex", time.Since(t0))
	}()

	buffer, err := repo.GetBlob(checksum)
	if err != nil {
		return nil, [32]byte{}, err
	}

	index, err := index.FromBytes(buffer)
	if err != nil {
		return nil, [32]byte{}, err
	}

	verifyChecksum := repo.Checksum(buffer)

	verifyChecksum32 := [32]byte{}
	copy(verifyChecksum32[:], verifyChecksum[:])

	return index, verifyChecksum32, nil
}

func GetFilesystem(repo *repository.Repository, checksum [32]byte) (*vfs.Filesystem, [32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetFilesystem", time.Since(t0))
	}()

	buffer, err := repo.GetBlob(checksum)
	if err != nil {
		return nil, [32]byte{}, err
	}

	filesystem, err := vfs.FromBytes(buffer)
	if err != nil {
		return nil, [32]byte{}, err
	}

	verifyChecksum := repo.Checksum(buffer)
	verifyChecksum32 := [32]byte{}
	copy(verifyChecksum32[:], verifyChecksum[:])

	return filesystem, verifyChecksum32, nil
}

func GetMetadata(repo *repository.Repository, checksum [32]byte) (*metadata.Metadata, [32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetMetadata", time.Since(t0))
	}()

	buffer, err := repo.GetBlob(checksum)
	if err != nil {
		return nil, [32]byte{}, err
	}

	md, err := metadata.NewFromBytes(buffer)
	if err != nil {
		return nil, [32]byte{}, err
	}

	verifyChecksum := repo.Checksum(buffer)
	verifyChecksum32 := [32]byte{}
	copy(verifyChecksum32[:], verifyChecksum[:])

	return md, verifyChecksum32, nil
}

func (snapshot *Snapshot) PutChunk(checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: PutChunk(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	encoded, err := snapshot.repository.Encode(data)
	if err != nil {
		return err
	}
	snapshot.packerChan <- &PackerMsg{Type: packfile.TYPE_CHUNK, Timestamp: time.Now(), Checksum: checksum, Data: encoded}
	return nil
}

func (snapshot *Snapshot) Repository() *repository.Repository {
	return snapshot.repository
}

func (snapshot *Snapshot) PutObject(object *objects.Object) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: PutObject(%064x)", snapshot.Header.GetIndexShortID(), object.Checksum)

	data, err := msgpack.Marshal(object)
	if err != nil {
		return err
	}

	encoded, err := snapshot.repository.Encode(data)
	if err != nil {
		return err
	}
	snapshot.packerChan <- &PackerMsg{Type: packfile.TYPE_OBJECT, Timestamp: time.Now(), Checksum: object.Checksum, Data: encoded}
	return nil
}

func (snapshot *Snapshot) PutFile(checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutFile", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: PutFile(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	encoded, err := snapshot.repository.Encode(data)
	if err != nil {
		return err
	}
	snapshot.packerChan <- &PackerMsg{Type: packfile.TYPE_FILE, Timestamp: time.Now(), Checksum: checksum, Data: encoded}
	return nil
}

func (snapshot *Snapshot) PutPackfile(pack *packfile.PackFile, objects [][32]byte, chunks [][32]byte, files [][32]byte, directories [][32]byte, datas [][32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutPackfile", time.Since(t0))
	}()

	repo := snapshot.repository

	serializedData, err := pack.SerializeData()
	if err != nil {
		panic("could not serialize pack file data" + err.Error())
	}
	serializedIndex, err := pack.SerializeIndex()
	if err != nil {
		panic("could not serialize pack file index" + err.Error())
	}
	serializedFooter, err := pack.SerializeFooter()
	if err != nil {
		panic("could not serialize pack file footer" + err.Error())
	}

	encryptedIndex, err := repo.Encode(serializedIndex)
	if err != nil {
		return err
	}

	encryptedFooter, err := repo.Encode(serializedFooter)
	if err != nil {
		return err
	}

	encryptedFooterLength := uint8(len(encryptedFooter))

	versionBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(versionBytes, pack.Footer.Version)

	serializedPackfile := append(serializedData, encryptedIndex...)
	serializedPackfile = append(serializedPackfile, encryptedFooter...)
	serializedPackfile = append(serializedPackfile, versionBytes...)
	serializedPackfile = append(serializedPackfile, byte(encryptedFooterLength))

	checksum := snapshot.repository.Checksum(serializedPackfile)

	var checksum32 [32]byte
	copy(checksum32[:], checksum[:])

	logger.Trace("snapshot", "%s: PutPackfile(%016x, ...)", snapshot.Header.GetIndexShortID(), checksum32)
	err = snapshot.repository.PutPackfile(checksum32, serializedPackfile)
	if err != nil {
		panic("could not write pack file")
	}

	for _, chunkChecksum := range chunks {
		for idx, chunk := range pack.Index {
			if chunk.Checksum == chunkChecksum {
				snapshot.Repository().State().SetPackfileForChunk(checksum32,
					chunkChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snapshot.stateDelta.SetPackfileForChunk(checksum32,
					chunkChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, objectChecksum := range objects {
		for idx, chunk := range pack.Index {
			if chunk.Checksum == objectChecksum {
				snapshot.Repository().State().SetPackfileForObject(checksum32,
					objectChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snapshot.stateDelta.SetPackfileForObject(checksum32,
					objectChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, fileChecksum := range files {
		for idx, chunk := range pack.Index {
			if chunk.Checksum == fileChecksum {
				snapshot.Repository().State().SetPackfileForFile(checksum32,
					fileChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snapshot.stateDelta.SetPackfileForFile(checksum32,
					fileChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, directoryChecksum := range directories {
		for idx, chunk := range pack.Index {
			if chunk.Checksum == directoryChecksum {
				snapshot.Repository().State().SetPackfileForDirectory(checksum32,
					directoryChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snapshot.stateDelta.SetPackfileForDirectory(checksum32,
					directoryChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, dataChecksum := range datas {
		for idx, chunk := range pack.Index {
			if chunk.Checksum == dataChecksum {
				snapshot.Repository().State().SetPackfileForData(checksum32,
					dataChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snapshot.stateDelta.SetPackfileForData(checksum32,
					dataChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}
	return nil
}

func (snapshot *Snapshot) GetChunk(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: GetChunk(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	packfileChecksum, offset, length, exists := snapshot.Repository().State().GetSubpartForChunk(checksum)
	if !exists {
		return nil, fmt.Errorf("packfile not found")
	}

	buffer, err := snapshot.repository.GetPackfileBlob(packfileChecksum, offset, length)
	if err != nil {
		return nil, err
	}
	return snapshot.repository.Decode(buffer)
}

func (snapshot *Snapshot) CheckChunk(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: CheckChunk(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	if exists, err := snapshot.Index.ChunkExists(checksum); err == nil && exists {
		return true
	} else {
		return snapshot.Repository().State().ChunkExists(checksum)
	}
}

func (snapshot *Snapshot) CheckObject(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: CheckObject(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	if exists, err := snapshot.Index.ObjectExists(checksum); err == nil && exists {
		return true
	} else {
		return snapshot.Repository().State().ObjectExists(checksum)
	}
}

func (snapshot *Snapshot) Commit() error {
	defer snapshot.Filesystem.Close()

	repo := snapshot.repository

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Commit", time.Since(t0))
	}()

	close(snapshot.packerChan)
	<-snapshot.packerChanDone

	if snapshot.stateDelta.Dirty() {
		serializedRepositoryIndex, err := snapshot.stateDelta.Serialize()
		if err != nil {
			logger.Warn("could not serialize repository index: %s", err)
			return err
		}
		indexChecksum := snapshot.repository.Checksum(serializedRepositoryIndex)
		indexChecksum32 := [32]byte{}
		copy(indexChecksum32[:], indexChecksum[:])
		_, err = repo.PutState(indexChecksum32, serializedRepositoryIndex)
		if err != nil {
			return err
		}
	}

	// there are three bits we can parallelize here:
	var serializedIndex []byte
	var serializedFilesystem []byte
	var serializedMetadata []byte
	var indexChecksum32 [32]byte
	var filesystemChecksum32 [32]byte
	var metadataChecksum32 [32]byte

	wg := sync.WaitGroup{}
	errc := make(chan error)
	errcDone := make(chan bool)
	var parallelError error
	go func() {
		for err := range errc {
			logger.Warn("commit error: %s", err)
			parallelError = err
		}
		close(errcDone)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var err error
		serializedIndex, err = snapshot.Index.Serialize()
		if err != nil {
			errc <- err
			return
		}

		indexChecksum := snapshot.repository.Checksum(serializedIndex)
		copy(indexChecksum32[:], indexChecksum[:])

		if exists, err := snapshot.repository.CheckBlob(indexChecksum32); err != nil {
			errc <- err
			return
		} else if !exists {
			_, err := repo.PutBlob(indexChecksum32, serializedIndex)
			if err != nil {
				errc <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var err error
		serializedFilesystem, err = snapshot.Filesystem.Serialize()
		if err != nil {
			errc <- err
			return
		}

		filesystemChecksum := snapshot.repository.Checksum(serializedFilesystem)
		copy(filesystemChecksum32[:], filesystemChecksum[:])

		if exists, err := snapshot.repository.CheckBlob(filesystemChecksum32); err != nil {
			errc <- err
			return
		} else if !exists {
			_, err = repo.PutBlob(filesystemChecksum32, serializedFilesystem)
			if err != nil {
				errc <- err
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		var err error
		serializedMetadata, err = snapshot.Metadata.Serialize()
		if err != nil {
			errc <- err
			return
		}

		metadataChecksum := snapshot.repository.Checksum(serializedMetadata)
		copy(metadataChecksum32[:], metadataChecksum[:])

		if exists, err := snapshot.repository.CheckBlob(metadataChecksum32); err != nil {
			errc <- err
			return
		} else if !exists {
			_, err := repo.PutBlob(metadataChecksum32, serializedMetadata)
			if err != nil {
				errc <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errc)
	<-errcDone

	if parallelError != nil {
		return parallelError
	}

	indexBlob := header.Blob{
		Type:     "index",
		Version:  index.VERSION,
		Checksum: indexChecksum32,
		Size:     uint64(len(serializedIndex)),
	}

	vfsBlob := header.Blob{
		Type:     "vfs",
		Version:  vfs.VERSION,
		Checksum: filesystemChecksum32,
		Size:     uint64(len(serializedFilesystem)),
	}

	metadataBlob := header.Blob{
		Type:     "content-type",
		Version:  metadata.VERSION,
		Checksum: metadataChecksum32,
		Size:     uint64(len(serializedMetadata)),
	}

	snapshot.Header.Index = indexBlob
	snapshot.Header.VFS = vfsBlob
	snapshot.Header.Metadata = metadataBlob

	serializedHdr, err := snapshot.Header.Serialize()
	if err != nil {
		return err
	}

	logger.Trace("snapshot", "%s: Commit()", snapshot.Header.GetIndexShortID())
	return snapshot.repository.Commit(snapshot.Header.IndexID, serializedHdr)
}

func (snapshot *Snapshot) NewReader(pathname string) (*Reader, error) {
	return NewReader(snapshot, pathname)
}
