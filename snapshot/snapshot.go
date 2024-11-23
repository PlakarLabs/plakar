package snapshot

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/profiler"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/metadata"
	"github.com/PlakarKorp/plakar/snapshot/statistics"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/google/uuid"
)

type Snapshot struct {
	repository *repository.Repository
	stateDelta *state.State

	filesystem *vfs.Filesystem

	SkipDirs []string

	Header *header.Header

	Metadata *metadata.Metadata

	statistics *statistics.Statistics

	packerChan     chan interface{}
	packerChanDone chan bool
}

type PackerMsg struct {
	Timestamp time.Time
	Type      packfile.BlobType
	Checksum  [32]byte
	Data      []byte
}

func New(repo *repository.Repository, snapshotID [32]byte) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Create", time.Since(t0))
	}()

	snapshot := &Snapshot{
		repository: repo,
		stateDelta: repo.NewStateDelta(),

		Header:   header.NewHeader(snapshotID),
		Metadata: metadata.New(),

		statistics: statistics.New(),

		packerChan:     make(chan interface{}, runtime.NumCPU()*2+1),
		packerChanDone: make(chan bool),
	}

	go func() {
		wg := sync.WaitGroup{}
		for i := 0; i < runtime.NumCPU(); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var pack *packfile.PackFile
				var snapshotHeaders map[[32]byte]struct{}
				var chunks map[[32]byte]struct{}
				var objects map[[32]byte]struct{}
				var files map[[32]byte]struct{}
				var directories map[[32]byte]struct{}
				var datas map[[32]byte]struct{}
				var signatures map[[32]byte]struct{}
				var errors map[[32]byte]struct{}

				for msg := range snapshot.packerChan {
					if pack == nil {
						pack = packfile.New()
						snapshotHeaders = make(map[[32]byte]struct{})
						chunks = make(map[[32]byte]struct{})
						objects = make(map[[32]byte]struct{})
						files = make(map[[32]byte]struct{})
						directories = make(map[[32]byte]struct{})
						datas = make(map[[32]byte]struct{})
						signatures = make(map[[32]byte]struct{})
						errors = make(map[[32]byte]struct{})
					}

					if msg, ok := msg.(*PackerMsg); !ok {
						panic("received data with unexpected type")
					} else {
						logger.Trace("packer", "%x: PackerMsg(%d, %064x), dt=%s", snapshot.Header.GetIndexShortID(), msg.Type, msg.Checksum, time.Since(msg.Timestamp))
						pack.AddBlob(msg.Type, msg.Checksum, msg.Data)
						switch msg.Type {
						case packfile.TYPE_SNAPSHOT:
							snapshotHeaders[msg.Checksum] = struct{}{}
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
						case packfile.TYPE_SIGNATURE:
							signatures[msg.Checksum] = struct{}{}
						case packfile.TYPE_ERROR:
							errors[msg.Checksum] = struct{}{}
						default:
							panic("received msg with unexpected blob type")
						}
					}

					if pack.Size() > uint32(repo.Configuration().Packfile.MaxSize) {
						snapshotHeadersList := make([][32]byte, len(snapshotHeaders))
						for snapshotID := range snapshotHeaders {
							snapshotHeadersList = append(snapshotHeadersList, snapshotID)
						}
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
						signaturesList := make([][32]byte, len(signatures))
						for signatureChecksum := range signatures {
							signaturesList = append(signaturesList, signatureChecksum)
						}
						errorsList := make([][32]byte, len(errors))
						for errorChecksum := range errors {
							errorsList = append(errorsList, errorChecksum)
						}
						err := snapshot.PutPackfile(pack, objectsList, chunksList, filesList, directoriesList, datasList, signaturesList, errorsList, snapshotHeadersList)
						if err != nil {
							panic(err)
						}
						pack = nil
					}
				}

				if pack != nil {
					snapshotHeadersList := make([][32]byte, len(snapshotHeaders))
					for snapshotID := range snapshotHeaders {
						snapshotHeadersList = append(snapshotHeadersList, snapshotID)
					}
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
					signaturesList := make([][32]byte, len(signatures))
					for signatureChecksum := range signatures {
						signaturesList = append(signaturesList, signatureChecksum)
					}
					errorsList := make([][32]byte, len(errors))
					for errorChecksum := range errors {
						errorsList = append(errorsList, errorChecksum)
					}
					err := snapshot.PutPackfile(pack, objectsList, chunksList, filesList, directoriesList, datasList, signaturesList, errorsList, snapshotHeadersList)
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

	logger.Trace("snapshot", "%x: New()", snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func Load(repo *repository.Repository, snapshotID [32]byte) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Load", time.Since(t0))
	}()

	hdr, _, err := GetSnapshot(repo, snapshotID)
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{}
	snapshot.repository = repo
	snapshot.Header = hdr

	logger.Trace("snapshot", "%x: Load()", snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func Fork(repo *repository.Repository, snapshotID [32]byte) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Fork", time.Since(t0))
	}()

	snap, err := Load(repo, snapshotID)
	if err != nil {
		return nil, err
	}
	snap.Header.CreationTime = time.Now()

	uuidBytes, err := uuid.Must(uuid.NewRandom()).MarshalBinary()
	if err != nil {
		return nil, err
	}

	snap.stateDelta = state.New()
	snap.statistics = statistics.New()

	snap.Header.SnapshotID = repo.Checksum(uuidBytes[:])
	snap.packerChan = make(chan interface{}, runtime.NumCPU()*2+1)
	snap.packerChanDone = make(chan bool)

	go func() {
		wg := sync.WaitGroup{}
		for i := 0; i < runtime.NumCPU(); i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var pack *packfile.PackFile
				var snapshotHeaders map[[32]byte]struct{}
				var chunks map[[32]byte]struct{}
				var objects map[[32]byte]struct{}
				var files map[[32]byte]struct{}
				var directories map[[32]byte]struct{}
				var datas map[[32]byte]struct{}
				var signatures map[[32]byte]struct{}
				var errors map[[32]byte]struct{}

				for msg := range snap.packerChan {
					if pack == nil {
						pack = packfile.New()
						snapshotHeaders = make(map[[32]byte]struct{})
						chunks = make(map[[32]byte]struct{})
						objects = make(map[[32]byte]struct{})
						files = make(map[[32]byte]struct{})
						directories = make(map[[32]byte]struct{})
						datas = make(map[[32]byte]struct{})
						signatures = make(map[[32]byte]struct{})
						errors = make(map[[32]byte]struct{})
					}

					if msg, ok := msg.(*PackerMsg); !ok {
						panic("received data with unexpected type")
					} else {
						logger.Trace("packer", "%x: PackerMsg(%d, %064x), dt=%s", snap.Header.GetIndexShortID(), msg.Type, msg.Checksum, time.Since(msg.Timestamp))
						pack.AddBlob(msg.Type, msg.Checksum, msg.Data)
						switch msg.Type {
						case packfile.TYPE_SNAPSHOT:
							snapshotHeaders[msg.Checksum] = struct{}{}
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
						case packfile.TYPE_SIGNATURE:
							signatures[msg.Checksum] = struct{}{}
						case packfile.TYPE_ERROR:
							errors[msg.Checksum] = struct{}{}
						default:
							panic("received msg with unexpected blob type")
						}
					}

					if pack.Size() > uint32(repo.Configuration().Packfile.MaxSize) {
						snapshotHeadersList := make([][32]byte, len(snapshotHeaders))
						for snapshotID := range snapshotHeaders {
							snapshotHeadersList = append(snapshotHeadersList, snapshotID)
						}
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
						signaturesList := make([][32]byte, len(signatures))
						for signatureChecksum := range signatures {
							signaturesList = append(signaturesList, signatureChecksum)
						}
						errorsList := make([][32]byte, len(errors))
						for errorChecksum := range errors {
							errorsList = append(errorsList, errorChecksum)
						}
						err := snap.PutPackfile(pack, objectsList, chunksList, filesList, directoriesList, datasList, signaturesList, errorsList, snapshotHeadersList)
						if err != nil {
							panic(err)
						}
						pack = nil
					}
				}

				if pack != nil {
					snapshotHeadersList := make([][32]byte, len(snapshotHeaders))
					for snapshotID := range snapshotHeaders {
						snapshotHeadersList = append(snapshotHeadersList, snapshotID)
					}
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
					signaturesList := make([][32]byte, len(signatures))
					for signatureChecksum := range signatures {
						signaturesList = append(signaturesList, signatureChecksum)
					}
					errorsList := make([][32]byte, len(errors))
					for errorChecksum := range errors {
						errorsList = append(errorsList, errorChecksum)
					}
					err := snap.PutPackfile(pack, objectsList, chunksList, filesList, directoriesList, datasList, signaturesList, errorsList, snapshotHeadersList)
					if err != nil {
						panic(err)
					}
					pack = nil
				}
			}()
		}
		wg.Wait()
		snap.packerChanDone <- true
		close(snap.packerChanDone)
	}()

	logger.Trace("snapshot", "%x: Fork(): %s", snap.Header.SnapshotID, snap.Header.GetIndexShortID())
	return snap, nil
}

func (snap *Snapshot) Event(evt events.Event) {
	snap.Repository().Context().Events().Send(evt)
}

func GetSnapshot(repo *repository.Repository, snapshotID [32]byte) (*header.Header, bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetSnapshot", time.Since(t0))
	}()
	logger.Trace("snapshot", "repository.GetSnapshot(%x)", snapshotID)

	rd, _, err := repo.GetBlob(packfile.TYPE_SNAPSHOT, snapshotID)
	if err != nil {
		return nil, false, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, false, err
	}

	hdr, err := header.NewFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	return hdr, false, nil
}

func GetMetadata(repo *repository.Repository, checksum [32]byte) (*metadata.Metadata, [32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetMetadata", time.Since(t0))
	}()

	rd, _, err := repo.GetBlob(packfile.TYPE_DATA, checksum)
	if err != nil {
		return nil, [32]byte{}, err
	}

	buffer, err := io.ReadAll(rd)
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

func (snap *Snapshot) PutBlob(blobType packfile.BlobType, checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutBlob", time.Since(t0))
	}()
	logger.Trace("snapshot", "%x: PutBlob(%064x)", snap.Header.GetIndexShortID(), checksum)

	encoded, err := snap.repository.Encode(data)
	if err != nil {
		return err
	}

	snap.packerChan <- &PackerMsg{Type: blobType, Timestamp: time.Now(), Checksum: checksum, Data: encoded}
	return nil
}

func (snapshot *Snapshot) Repository() *repository.Repository {
	return snapshot.repository
}

func (snap *Snapshot) PutPackfile(pack *packfile.PackFile, objects [][32]byte, chunks [][32]byte, files [][32]byte, directories [][32]byte, datas [][32]byte, signatures [][32]byte, errors [][32]byte, snapshots [][32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutPackfile", time.Since(t0))
	}()

	repo := snap.repository

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

	checksum := snap.repository.Checksum(serializedPackfile)

	var checksum32 [32]byte
	copy(checksum32[:], checksum[:])

	atomic.AddUint64(&snap.statistics.PackfilesCount, 1)
	atomic.AddUint64(&snap.statistics.PackfilesSize, uint64(len(serializedPackfile)))

	logger.Trace("snapshot", "%x: PutPackfile(%x, ...)", snap.Header.GetIndexShortID(), checksum32)
	err = snap.repository.PutPackfile(checksum32, bytes.NewBuffer(serializedPackfile), uint64(len(serializedPackfile)))
	if err != nil {
		panic("could not write pack file")
	}

	atomic.AddUint64(&snap.statistics.PackfilesTransferCount, 1)
	atomic.AddUint64(&snap.statistics.PackfilesTransferSize, uint64(len(serializedPackfile)))

	for _, chunkChecksum := range chunks {
		for idx, blob := range pack.Index {
			if blob.Checksum == chunkChecksum && blob.Type == packfile.TYPE_CHUNK {
				snap.Repository().SetPackfileForBlob(packfile.TYPE_CHUNK, checksum32,
					chunkChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snap.stateDelta.SetPackfileForBlob(packfile.TYPE_CHUNK, checksum32,
					chunkChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, objectChecksum := range objects {
		for idx, blob := range pack.Index {
			if blob.Checksum == objectChecksum && blob.Type == packfile.TYPE_OBJECT {
				snap.Repository().SetPackfileForBlob(packfile.TYPE_OBJECT, checksum32,
					objectChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snap.stateDelta.SetPackfileForBlob(packfile.TYPE_OBJECT, checksum32,
					objectChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, fileChecksum := range files {
		for idx, blob := range pack.Index {
			if blob.Checksum == fileChecksum && blob.Type == packfile.TYPE_FILE {
				snap.Repository().SetPackfileForBlob(packfile.TYPE_FILE, checksum32,
					fileChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snap.stateDelta.SetPackfileForBlob(packfile.TYPE_FILE, checksum32,
					fileChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, directoryChecksum := range directories {
		for idx, blob := range pack.Index {
			if blob.Checksum == directoryChecksum && blob.Type == packfile.TYPE_DIRECTORY {
				snap.Repository().SetPackfileForBlob(packfile.TYPE_DIRECTORY, checksum32,
					directoryChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snap.stateDelta.SetPackfileForBlob(packfile.TYPE_DIRECTORY, checksum32,
					directoryChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, dataChecksum := range datas {
		for idx, blob := range pack.Index {
			if blob.Checksum == dataChecksum && blob.Type == packfile.TYPE_DATA {
				snap.Repository().SetPackfileForBlob(packfile.TYPE_DATA, checksum32,
					dataChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snap.stateDelta.SetPackfileForBlob(packfile.TYPE_DATA, checksum32,
					dataChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, signatureChecksum := range signatures {
		for idx, blob := range pack.Index {
			if blob.Checksum == signatureChecksum && blob.Type == packfile.TYPE_SIGNATURE {
				snap.Repository().SetPackfileForBlob(packfile.TYPE_SIGNATURE, checksum32,
					signatureChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snap.stateDelta.SetPackfileForBlob(packfile.TYPE_SIGNATURE, checksum32,
					signatureChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, errorChecksum := range errors {
		for idx, blob := range pack.Index {
			if blob.Checksum == errorChecksum && blob.Type == packfile.TYPE_ERROR {
				snap.Repository().SetPackfileForBlob(packfile.TYPE_ERROR, checksum32,
					errorChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snap.stateDelta.SetPackfileForBlob(packfile.TYPE_ERROR, checksum32,
					errorChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	for _, snapshotID := range snapshots {
		for idx, blob := range pack.Index {
			if blob.Checksum == snapshotID && blob.Type == packfile.TYPE_SNAPSHOT {
				snap.Repository().SetPackfileForBlob(packfile.TYPE_SNAPSHOT, checksum32,
					snapshotID,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				snap.stateDelta.SetPackfileForBlob(packfile.TYPE_SNAPSHOT, checksum32,
					snapshotID,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}
	return nil
}

func (snapshot *Snapshot) GetBlob(blobType packfile.BlobType, checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetBlob", time.Since(t0))
	}()
	logger.Trace("snapshot", "%x: GetBlob(%x)", snapshot.Header.GetIndexShortID(), checksum)

	rd, _, err := snapshot.repository.GetBlob(blobType, checksum)
	if err != nil {
		return nil, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (snapshot *Snapshot) BlobExists(blobType packfile.BlobType, checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckBlob", time.Since(t0))
	}()
	logger.Trace("snapshot", "%x: CheckBlob(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	return snapshot.Repository().BlobExists(blobType, checksum)
}

func (snapshot *Snapshot) GetObject(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%x: GetObject(%x)", snapshot.Header.GetIndexShortID(), checksum)

	rd, _, err := snapshot.repository.GetBlob(packfile.TYPE_OBJECT, checksum)
	if err != nil {
		return nil, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (snapshot *Snapshot) Commit() error {

	repo := snapshot.repository

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Commit", time.Since(t0))
	}()

	serializedHdr, err := snapshot.Header.Serialize()
	if err != nil {
		return err
	}

	if kp := snapshot.repository.Context().GetKeypair(); kp != nil {
		serializedHdrChecksum := snapshot.repository.Checksum(serializedHdr)
		signature := kp.Sign(serializedHdrChecksum[:])
		if err := snapshot.PutBlob(packfile.TYPE_SIGNATURE, snapshot.Header.SnapshotID, signature); err != nil {
			return err
		}
	}

	if err := snapshot.PutBlob(packfile.TYPE_SNAPSHOT, snapshot.Header.SnapshotID, serializedHdr); err != nil {
		return err
	}

	close(snapshot.packerChan)
	<-snapshot.packerChanDone

	serializedRepositoryIndex, err := snapshot.stateDelta.Serialize()
	if err != nil {
		logger.Warn("could not serialize repository index: %s", err)
		return err
	}
	indexChecksum := snapshot.repository.Checksum(serializedRepositoryIndex)
	indexChecksum32 := [32]byte{}
	copy(indexChecksum32[:], indexChecksum[:])
	_, err = repo.PutState(indexChecksum32, bytes.NewBuffer(serializedRepositoryIndex), int64(len(serializedRepositoryIndex)))
	if err != nil {
		return err
	}

	logger.Trace("snapshot", "%x: Commit()", snapshot.Header.GetIndexShortID())
	return nil
}

func (snapshot *Snapshot) NewReader(pathname string) (*Reader, error) {
	return NewReader(snapshot, pathname)
}

func (snapshot *Snapshot) LookupObject(checksum [32]byte) (*objects.Object, error) {
	buffer, err := snapshot.GetObject(checksum)
	if err != nil {
		return nil, err
	}
	return objects.NewObjectFromBytes(buffer)
}

func (snap *Snapshot) ListChunks() (<-chan [32]byte, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan [32]byte)
	go func() {
		for filename := range fs.Files() {
			fsentry, err := fs.Stat(filename)
			if err != nil {
				break
			}
			for _, chunk := range fsentry.(*vfs.FileEntry).Object.Chunks {
				c <- chunk.Checksum
			}
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListObjects() (<-chan [32]byte, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan [32]byte)
	go func() {
		for filename := range fs.Files() {
			fsentry, err := fs.Stat(filename)
			if err != nil {
				break
			}
			c <- fsentry.(*vfs.FileEntry).Object.Checksum
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListFiles() (<-chan [32]byte, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan [32]byte)
	go func() {
		for checksum := range fs.FileChecksums() {
			c <- checksum
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListDirectories() (<-chan [32]byte, error) {
	fs, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}
	c := make(chan [32]byte)
	go func() {
		c <- snap.Header.Root
		for checksum := range fs.DirectoryChecksums() {
			c <- checksum
		}
		close(c)
	}()
	return c, nil
}

func (snap *Snapshot) ListDatas() (<-chan [32]byte, error) {
	c := make(chan [32]byte)

	go func() {
		c <- snap.Header.Metadata
		c <- snap.Header.Statistics
		close(c)
	}()

	return c, nil
}
