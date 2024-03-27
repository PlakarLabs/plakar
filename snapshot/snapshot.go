package snapshot

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/packfile"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/snapshot/header"
	"github.com/PlakarLabs/plakar/snapshot/index"
	"github.com/PlakarLabs/plakar/snapshot/metadata"
	"github.com/PlakarLabs/plakar/storage"
	storageIndex "github.com/PlakarLabs/plakar/storage/index"
	"github.com/PlakarLabs/plakar/storage/locking"
	"github.com/PlakarLabs/plakar/vfs"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type Snapshot struct {
	repository *storage.Repository

	SkipDirs []string

	Header     *header.Header
	Index      *index.Index
	Filesystem *vfs.Filesystem
	Metadata   *metadata.Metadata

	packerChan     chan interface{}
	packerChanDone chan bool
}

type PackerChunkMsg struct {
	Timestamp time.Time
	Checksum  [32]byte
	Data      []byte
}

type PackerObjectMsg struct {
	Timestamp time.Time
	Checksum  [32]byte
	Data      []byte
}

func New(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Create", time.Since(t0))
	}()

	snapshot := &Snapshot{
		repository: repository,

		Header:     header.NewHeader(indexID),
		Index:      index.NewIndex(),
		Filesystem: vfs.NewFilesystem(),
		Metadata:   metadata.New(),

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
				var chunks map[[32]byte]struct{}
				var objects map[[32]byte]struct{}

				for msg := range snapshot.packerChan {
					if pack == nil {
						pack = packfile.New()
						chunks = make(map[[32]byte]struct{})
						objects = make(map[[32]byte]struct{})
					}
					switch msg := msg.(type) {
					case *PackerObjectMsg:
						logger.Trace("packer", "%s: PackerObjectMsg(%064x), dt=%s", snapshot.Header.GetIndexShortID(), msg.Checksum, time.Since(msg.Timestamp))
						pack.AddData(packfile.TYPE_OBJECT, msg.Checksum, msg.Data)
						objects[msg.Checksum] = struct{}{}

					case *PackerChunkMsg:
						logger.Trace("packer", "%s: PackerChunkMsg(%064x), dt=%s", snapshot.Header.GetIndexShortID(), msg.Checksum, time.Since(msg.Timestamp))
						pack.AddData(packfile.TYPE_CHUNK, msg.Checksum, msg.Data)
						chunks[msg.Checksum] = struct{}{}

					default:
						panic("received data with unexpected type")
					}

					if pack.Size() > uint32(repository.Configuration().PackfileSize) {
						objectsList := make([][32]byte, len(objects))
						for objectChecksum := range objects {
							objectsList = append(objectsList, objectChecksum)
						}
						chunksList := make([][32]byte, len(chunks))
						for chunkChecksum := range chunks {
							chunksList = append(chunksList, chunkChecksum)
						}
						err := snapshot.PutPackfile(pack, objectsList, chunksList)
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
					err := snapshot.PutPackfile(pack, objectsList, chunksList)
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

func Load(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Load", time.Since(t0))
	}()

	hdr, _, err := GetSnapshot(repository, indexID)
	if err != nil {
		return nil, err
	}

	var indexChecksum32 [32]byte
	copy(indexChecksum32[:], hdr.Index[0].Checksum[:])

	index, verifyChecksum, err := GetIndex(repository, indexChecksum32)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(verifyChecksum[:], hdr.Index[0].Checksum[:]) {
		return nil, fmt.Errorf("index mismatches hdr checksum")
	}

	var filesystemChecksum32 [32]byte
	copy(filesystemChecksum32[:], hdr.VFS[0].Checksum[:])

	filesystem, verifyChecksum, err := GetFilesystem(repository, filesystemChecksum32)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(verifyChecksum[:], hdr.VFS[0].Checksum[:]) {
		return nil, fmt.Errorf("filesystem mismatches hdr checksum")
	}

	var metadataChecksum32 [32]byte
	copy(metadataChecksum32[:], hdr.Metadata[0].Checksum[:])

	md, verifyChecksum, err := GetMetadata(repository, metadataChecksum32)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(verifyChecksum[:], hdr.Metadata[0].Checksum[:]) {
		return nil, fmt.Errorf("metadata mismatches hdr checksum")
	}

	snapshot := &Snapshot{}
	snapshot.repository = repository
	snapshot.Header = hdr
	snapshot.Index = index
	snapshot.Filesystem = filesystem
	snapshot.Metadata = md

	logger.Trace("snapshot", "%s: Load()", snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func Fork(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Fork", time.Since(t0))
	}()

	hdr, _, err := GetSnapshot(repository, indexID)
	if err != nil {
		return nil, err
	}

	index, verifyChecksum, err := GetIndex(repository, hdr.Index[0].Checksum)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(verifyChecksum[:], hdr.Index[0].Checksum[:]) {
		return nil, fmt.Errorf("index mismatches hdr checksum")
	}

	filesystem, verifyChecksum, err := GetFilesystem(repository, hdr.VFS[0].Checksum)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(verifyChecksum[:], hdr.VFS[0].Checksum[:]) {
		return nil, fmt.Errorf("filesystem mismatches hdr checksum")
	}

	snapshot := &Snapshot{
		repository: repository,

		Header:     hdr,
		Index:      index,
		Filesystem: filesystem,
	}
	snapshot.Header.IndexID = uuid.Must(uuid.NewRandom())

	logger.Trace("snapshot", "%s: Fork(): %s", indexID, snapshot.Header.GetIndexShortID())
	return snapshot, nil
}

func GetSnapshot(repository *storage.Repository, indexID uuid.UUID) (*header.Header, bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetSnapshot", time.Since(t0))
	}()

	cache := repository.GetCache()

	var buffer []byte

	cacheMiss := false
	if cache != nil {
		tmp, err := cache.GetSnapshot(repository.Configuration().RepositoryID.String(), indexID.String())
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot", "repository.GetSnapshot(%s)", indexID)
			tmp, err = repository.GetSnapshot(indexID)
			if err != nil {
				return nil, false, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot", "repository.GetSnapshot(%s)", indexID)
		tmp, err := repository.GetSnapshot(indexID)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	if cache != nil && cacheMiss {
		cache.PutSnapshot(repository.Configuration().RepositoryID.String(), indexID.String(), buffer)
	}

	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	if compressionMethod != "" {
		tmp, err := compression.Inflate(compressionMethod, buffer)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	hdr, err := header.NewFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	return hdr, false, nil
}

func GetBlob(repository *storage.Repository, checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetBlob", time.Since(t0))
	}()
	cache := repository.GetCache()

	var buffer []byte

	cacheMiss := false
	if cache != nil {
		tmp, err := cache.GetBlob(repository.Configuration().RepositoryID.String(), checksum)
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot", "repository.GetBlob(%016x)", checksum)
			tmp, err = repository.GetBlob(checksum)
			if err != nil {
				return nil, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot", "repository.GetBlob(%016x)", checksum)
		tmp, err := repository.GetBlob(checksum)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if cache != nil && cacheMiss {
		cache.PutBlob(repository.Configuration().RepositoryID.String(), checksum, buffer)
	}

	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if compressionMethod != "" {
		tmp, err := compression.Inflate(compressionMethod, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	return buffer, nil
}

func GetRepositoryIndex(repository *storage.Repository, checksum [32]byte) (*storageIndex.Index, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetRepositoryIndex", time.Since(t0))
	}()

	cache := repository.GetCache()

	var buffer []byte

	cacheMiss := false
	if cache != nil {
		tmp, err := cache.GetIndex(repository.Configuration().RepositoryID.String(), checksum)
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot", "repository.GetIndex(%016x)", checksum)
			tmp, err = repository.GetIndex(checksum)
			if err != nil {
				return nil, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot", "repository.GetIndex(%016x)", checksum)
		tmp, err := repository.GetIndex(checksum)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if cache != nil && cacheMiss {
		cache.PutIndex(repository.Configuration().RepositoryID.String(), checksum, buffer)
	}

	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if compressionMethod != "" {
		tmp, err := compression.Inflate(compressionMethod, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	return storageIndex.NewFromBytes(buffer)
}

func GetIndex(repository *storage.Repository, checksum [32]byte) (*index.Index, [32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetIndex", time.Since(t0))
	}()

	buffer, err := GetBlob(repository, checksum)
	if err != nil {
		return nil, [32]byte{}, err
	}

	index, err := index.NewIndexFromBytes(buffer)
	if err != nil {
		return nil, [32]byte{}, err
	}

	indexHasher := encryption.GetHasher(repository.Configuration().Hashing)
	indexHasher.Write(buffer)
	verifyChecksum := indexHasher.Sum(nil)

	verifyChecksum32 := [32]byte{}
	copy(verifyChecksum32[:], verifyChecksum[:])

	return index, verifyChecksum32, nil
}

func GetFilesystem(repository *storage.Repository, checksum [32]byte) (*vfs.Filesystem, [32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetFilesystem", time.Since(t0))
	}()

	buffer, err := GetBlob(repository, checksum)
	if err != nil {
		return nil, [32]byte{}, err
	}

	filesystem, err := vfs.NewFilesystemFromBytes(buffer)
	if err != nil {
		return nil, [32]byte{}, err
	}

	fsHasher := encryption.GetHasher(repository.Configuration().Hashing)
	fsHasher.Write(buffer)
	verifyChecksum := fsHasher.Sum(nil)
	verifyChecksum32 := [32]byte{}
	copy(verifyChecksum32[:], verifyChecksum[:])

	return filesystem, verifyChecksum32, nil
}

func GetMetadata(repository *storage.Repository, checksum [32]byte) (*metadata.Metadata, [32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetMetadata", time.Since(t0))
	}()

	buffer, err := GetBlob(repository, checksum)
	if err != nil {
		return nil, [32]byte{}, err
	}

	md, err := metadata.NewFromBytes(buffer)
	if err != nil {
		return nil, [32]byte{}, err
	}

	mdHasher := encryption.GetHasher(repository.Configuration().Hashing)
	mdHasher.Write(buffer)
	verifyChecksum := mdHasher.Sum(nil)
	verifyChecksum32 := [32]byte{}
	copy(verifyChecksum32[:], verifyChecksum[:])

	return md, verifyChecksum32, nil
}

func GetLock(repository *storage.Repository, lockID uuid.UUID) (*locking.Lock, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetLock", time.Since(t0))
	}()

	buffer, err := repository.GetLock(lockID)
	if err != nil {
		return nil, err
	}

	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if compressionMethod != "" {
		tmp, err := compression.Inflate(compressionMethod, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	lock, err := locking.NewFromBytes(buffer)
	if err != nil {
		return nil, err
	}

	return lock, nil
}

func List(repository *storage.Repository) ([]uuid.UUID, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.List", time.Since(t0))
	}()
	return repository.GetSnapshots()
}

func (snapshot *Snapshot) PutChunk(checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutChunk", time.Since(t0))
	}()

	logger.Trace("snapshot", "%s: PutChunk(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	repository := snapshot.repository
	buffer := data
	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression
	var err error
	if compressionMethod != "" {
		buffer, err = compression.Deflate(compressionMethod, buffer)
		if err != nil {
			return err
		}
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	snapshot.packerChan <- &PackerChunkMsg{Timestamp: time.Now(), Checksum: checksum, Data: buffer}
	return nil
}

func (snapshot *Snapshot) Repository() *storage.Repository {
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

	secret := snapshot.repository.GetSecret()

	buffer := data
	if snapshot.repository.Configuration().Compression != "" {
		buffer, err = compression.Deflate(snapshot.repository.Configuration().Compression, buffer)
		if err != nil {
			return err
		}
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	snapshot.packerChan <- &PackerObjectMsg{Timestamp: time.Now(), Checksum: object.Checksum, Data: buffer}
	return nil
}

func (snapshot *Snapshot) PutPackfile(pack *packfile.PackFile, objects [][32]byte, chunks [][32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutPackfile", time.Since(t0))
	}()

	hasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)

	serializedPackfile, err := pack.Serialize()
	if err != nil {
		panic("could not serialize pack file")
	}
	hasher.Write(serializedPackfile)
	checksum := hasher.Sum(nil)
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
				snapshot.Repository().GetRepositoryIndex().SetPackfileForChunk(checksum32,
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
				snapshot.Repository().GetRepositoryIndex().SetPackfileForObject(checksum32,
					objectChecksum,
					pack.Index[idx].Offset,
					pack.Index[idx].Length)
				break
			}
		}
	}

	return nil
}

func (snapshot *Snapshot) prepareHeader(data []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.prepareHeader", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: prepareHeader()", snapshot.Header.GetIndexShortID())

	repository := snapshot.repository

	buffer := data
	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if compressionMethod != "" {
		tmp, err := compression.Deflate(compressionMethod, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if cache != nil {
		cache.PutSnapshot(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Header.GetIndexID().String(), buffer)
	}

	return buffer, nil
}

func (snapshot *Snapshot) PutBlob(checksum [32]byte, data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutBlob", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: PutBlob(%016x)", snapshot.Header.GetIndexShortID(), checksum)

	repository := snapshot.repository

	buffer := data
	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if compressionMethod != "" {
		tmp, err := compression.Deflate(compressionMethod, buffer)
		if err != nil {
			return 0, err
		}
		buffer = tmp
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return 0, err
		}
		buffer = tmp
	}

	if cache != nil {
		cache.PutBlob(snapshot.repository.Configuration().RepositoryID.String(), checksum, buffer)
	}

	return len(buffer), snapshot.repository.PutBlob(checksum, buffer)
}

func (snapshot *Snapshot) PutIndex(checksum [32]byte, data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutIndex", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: PutIndex(%016x)", snapshot.Header.GetIndexShortID(), checksum)

	repository := snapshot.repository

	buffer := data
	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if compressionMethod != "" {
		tmp, err := compression.Deflate(compressionMethod, buffer)
		if err != nil {
			return 0, err
		}
		buffer = tmp
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return 0, err
		}
		buffer = tmp
	}

	if cache != nil {
		//cache.PutIndex(snapshot.repository.Configuration().RepositoryID.String(), checksum, buffer)
	}

	return len(buffer), snapshot.repository.PutIndex(checksum, buffer)
}

func (snapshot *Snapshot) GetChunk(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: GetChunk(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	packfileChecksum, offset, length, exists := snapshot.Repository().GetRepositoryIndex().GetSubpartForChunk(checksum)
	if !exists {
		return nil, fmt.Errorf("packfile not found")
	}

	buffer, err := snapshot.repository.GetPackfileSubpart(packfileChecksum, offset, length)
	if err != nil {
		return nil, err
	}

	repository := snapshot.repository

	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if compressionMethod != "" {
		tmp, err := compression.Inflate(compressionMethod, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	return buffer, nil
}

func (snapshot *Snapshot) CheckChunk(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: CheckChunk(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	if snapshot.Index.ChunkExists(checksum) {
		return true
	} else {
		return snapshot.Repository().GetRepositoryIndex().ChunkExists(checksum)
	}
}

func (snapshot *Snapshot) CheckObject(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: CheckObject(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	if snapshot.Index.ObjectExists(checksum) {
		return true
	} else {
		return snapshot.Repository().GetRepositoryIndex().ObjectExists(checksum)
	}
}

func PutLock(repository storage.Repository, lock *locking.Lock) (uuid.UUID, error) {
	lockID := uuid.Must(uuid.NewRandom())

	buffer, err := lock.Serialize()
	if err != nil {
		return uuid.Nil, err
	}

	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if compressionMethod != "" {
		tmp, err := compression.Deflate(compressionMethod, buffer)
		if err != nil {
			return uuid.Nil, err
		}
		buffer = tmp
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return uuid.Nil, err
		}
		buffer = tmp
	}

	return lockID, repository.PutLock(lockID, buffer)

}

func (snapshot *Snapshot) Lock() error {
	lock := locking.New(snapshot.Header.Hostname,
		snapshot.Header.Username,
		snapshot.Header.MachineID,
		snapshot.Header.ProcessID,
		false)

	buffer, err := lock.Serialize()
	if err != nil {
		return err
	}

	repository := snapshot.repository
	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression

	if compressionMethod != "" {
		tmp, err := compression.Deflate(compressionMethod, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	return snapshot.repository.PutLock(snapshot.Header.IndexID, buffer)
}

func (snapshot *Snapshot) Unlock() error {
	return snapshot.repository.DeleteLock(snapshot.Header.IndexID)
}

func (snapshot *Snapshot) Commit() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Commit", time.Since(t0))
	}()

	close(snapshot.packerChan)
	<-snapshot.packerChanDone

	if snapshot.Repository().GetRepositoryIndex().IsDirty() {
		serializedRepositoryIndex, err := snapshot.Repository().GetRepositoryIndex().Serialize()
		if err != nil {
			logger.Warn("could not serialize repository index: %s", err)
			return err
		}
		indexHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
		indexHasher.Write(serializedRepositoryIndex)
		indexChecksum := indexHasher.Sum(nil)
		indexChecksum32 := [32]byte{}
		copy(indexChecksum32[:], indexChecksum[:])
		_, err = snapshot.PutIndex(indexChecksum32, serializedRepositoryIndex)
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

		indexHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
		indexHasher.Write(serializedIndex)
		indexChecksum := indexHasher.Sum(nil)
		copy(indexChecksum32[:], indexChecksum[:])

		if exists, err := snapshot.repository.CheckBlob(indexChecksum32); err != nil {
			errc <- err
			return
		} else if !exists {
			_, err := snapshot.PutBlob(indexChecksum32, serializedIndex)
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

		fsHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
		fsHasher.Write(serializedFilesystem)
		filesystemChecksum := fsHasher.Sum(nil)
		copy(filesystemChecksum32[:], filesystemChecksum[:])

		if exists, err := snapshot.repository.CheckBlob(filesystemChecksum32); err != nil {
			errc <- err
			return
		} else if !exists {
			_, err = snapshot.PutBlob(filesystemChecksum32, serializedFilesystem)
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

		mdHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
		mdHasher.Write(serializedMetadata)
		metadataChecksum := mdHasher.Sum(nil)
		copy(metadataChecksum32[:], metadataChecksum[:])

		if exists, err := snapshot.repository.CheckBlob(metadataChecksum32); err != nil {
			errc <- err
			return
		} else if !exists {
			_, err := snapshot.PutBlob(metadataChecksum32, serializedMetadata)
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

	snapshot.Header.Index = []header.Blob{indexBlob}
	snapshot.Header.VFS = []header.Blob{vfsBlob}
	snapshot.Header.Metadata = []header.Blob{metadataBlob}

	serializedHdr, err := snapshot.Header.Serialize()
	if err != nil {
		return err
	}

	snapshotBytes, err := snapshot.prepareHeader(serializedHdr)
	if err != nil {
		return err
	}

	logger.Trace("snapshot", "%s: Commit()", snapshot.Header.GetIndexShortID())
	return snapshot.repository.Commit(snapshot.Header.IndexID, snapshotBytes)
}

func (snapshot *Snapshot) NewReader(pathname string) (*Reader, error) {
	return NewReader(snapshot, pathname)
}
