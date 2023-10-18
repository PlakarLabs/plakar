package snapshot

import (
	"bytes"
	"fmt"
	"time"

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/index"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/metadata"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/PlakarLabs/plakar/vfs"
	"github.com/google/uuid"
)

type Snapshot struct {
	repository  *storage.Repository
	transaction *storage.Transaction

	SkipDirs []string

	Metadata   *metadata.Metadata
	Index      *index.Index
	Filesystem *vfs.Filesystem

	concurrentObjects    int64
	concurrentChunks     int64
	concurrentChunksSize int64
}

func New(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Create", time.Since(t0))
	}()

	tx, err := repository.Transaction(indexID)
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{
		repository:  repository,
		transaction: tx,

		Metadata:   metadata.NewMetadata(indexID),
		Index:      index.NewIndex(),
		Filesystem: vfs.NewFilesystem(),
	}

	logger.Trace("snapshot", "%s: New()", snapshot.Metadata.GetIndexShortID())
	return snapshot, nil
}

func Load(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Load", time.Since(t0))
	}()

	metadata, _, err := GetSnapshot(repository, indexID)
	if err != nil {
		return nil, err
	}

	var indexChecksum32 [32]byte
	copy(indexChecksum32[:], metadata.IndexChecksum[:])

	index, verifyChecksum, err := GetIndex(repository, indexChecksum32)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(verifyChecksum, metadata.IndexChecksum) {
		return nil, fmt.Errorf("index mismatches metadata checksum")
	}

	var filesystemChecksum32 [32]byte
	copy(filesystemChecksum32[:], metadata.FilesystemChecksum[:])

	filesystem, verifyChecksum, err := GetFilesystem(repository, filesystemChecksum32)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(verifyChecksum, metadata.FilesystemChecksum) {
		return nil, fmt.Errorf("filesystem mismatches metadata checksum")
	}

	snapshot := &Snapshot{}
	snapshot.repository = repository
	snapshot.Metadata = metadata
	snapshot.Index = index
	snapshot.Filesystem = filesystem

	logger.Trace("snapshot", "%s: Load()", snapshot.Metadata.GetIndexShortID())
	return snapshot, nil
}

func Fork(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Fork", time.Since(t0))
	}()

	metadata, _, err := GetSnapshot(repository, indexID)
	if err != nil {
		return nil, err
	}
	var indexChecksum32 [32]byte
	copy(indexChecksum32[:], metadata.IndexChecksum[:])

	index, verifyChecksum, err := GetIndex(repository, indexChecksum32)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(verifyChecksum, metadata.IndexChecksum) {
		return nil, fmt.Errorf("index mismatches metadata checksum")
	}

	var filesystemChecksum32 [32]byte
	copy(filesystemChecksum32[:], metadata.FilesystemChecksum[:])

	filesystem, verifyChecksum, err := GetFilesystem(repository, filesystemChecksum32)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(verifyChecksum, metadata.FilesystemChecksum) {
		return nil, fmt.Errorf("filesystem mismatches metadata checksum")
	}

	tx, err := repository.Transaction(uuid.Must(uuid.NewRandom()))
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{
		repository:  repository,
		transaction: tx,

		Metadata:   metadata,
		Index:      index,
		Filesystem: filesystem,
	}
	snapshot.Metadata.IndexID = tx.GetUuid()

	logger.Trace("snapshot", "%s: Fork(): %s", indexID, snapshot.Metadata.GetIndexShortID())
	return snapshot, nil
}

func GetSnapshot(repository *storage.Repository, indexID uuid.UUID) (*metadata.Metadata, bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetSnapshot", time.Since(t0))
	}()

	cache := repository.GetCache()

	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot", "cache.GetSnapshot(%s)", indexID)
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
		logger.Trace("snapshot", "cache.PutSnapshot(%s)", indexID)
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

	metadata, err := metadata.NewMetadataFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	return metadata, false, nil
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
		logger.Trace("snapshot", "cache.GetBlob(%016x)", checksum)
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
		logger.Trace("snapshot", "cache.PutBlob(%016x)", checksum)
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

func GetIndex(repository *storage.Repository, checksum [32]byte) (*index.Index, []byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetIndex", time.Since(t0))
	}()

	buffer, err := GetBlob(repository, checksum)
	if err != nil {
		return nil, nil, err
	}

	index, err := index.NewIndexFromBytes(buffer)
	if err != nil {
		return nil, nil, err
	}

	indexHasher := encryption.GetHasher(repository.Configuration().Hashing)
	indexHasher.Write(buffer)
	verifyChecksum := indexHasher.Sum(nil)

	return index, verifyChecksum[:], nil
}

func GetFilesystem(repository *storage.Repository, checksum [32]byte) (*vfs.Filesystem, []byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetFilesystem", time.Since(t0))
	}()

	buffer, err := GetBlob(repository, checksum)
	if err != nil {
		return nil, nil, err
	}

	filesystem, err := vfs.NewFilesystemFromBytes(buffer)
	if err != nil {
		return nil, nil, err
	}

	fsHasher := encryption.GetHasher(repository.Configuration().Hashing)
	fsHasher.Write(buffer)
	verifyChecksum := fsHasher.Sum(nil)

	return filesystem, verifyChecksum[:], nil
}

func List(repository *storage.Repository) ([]uuid.UUID, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.List", time.Since(t0))
	}()
	return repository.GetSnapshots()
}

func (snapshot *Snapshot) PutChunk(checksum [32]byte, data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutChunk", time.Since(t0))
	}()

	logger.Trace("snapshot", "%s: PutChunk(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)

	repository := snapshot.repository
	buffer := data
	secret := repository.GetSecret()
	compressionMethod := repository.Configuration().Compression
	var err error
	if compressionMethod != "" {
		buffer, err = compression.Deflate(compressionMethod, buffer)
		if err != nil {
			return 0, err
		}
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return 0, err
		}
		buffer = tmp
	}

	return snapshot.repository.PutChunk(checksum, buffer)
}

func (snapshot *Snapshot) Repository() *storage.Repository {
	return snapshot.repository
}

func (snapshot *Snapshot) PutObject(object *objects.Object) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: PutObject(%064x)", snapshot.Metadata.GetIndexShortID(), object.Checksum)
	return snapshot.repository.PutObject(object.Checksum)
}

func (snapshot *Snapshot) prepareMetadata(data []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.prepareMetadata", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: prepareMetadata()", snapshot.Metadata.GetIndexShortID())

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
		cache.PutSnapshot(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.GetIndexID().String(), buffer)
	}

	return buffer, nil
}

func (snapshot *Snapshot) PutBlob(checksum [32]byte, data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutBlob", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot", "%s: PutBlob(%016x)", snapshot.Metadata.GetIndexShortID(), checksum)

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

	return len(buffer), snapshot.transaction.PutBlob(checksum, buffer)
}

func (snapshot *Snapshot) GetChunk(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: GetChunk(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)

	buffer, err := snapshot.repository.GetChunk(checksum)
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

func (snapshot *Snapshot) CheckChunk(checksum [32]byte) (bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckChunk", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: CheckChunk(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)
	exists, err := snapshot.repository.CheckChunk(checksum)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (snapshot *Snapshot) CheckObject(checksum [32]byte) (bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckObject", time.Since(t0))
	}()
	logger.Trace("snapshot", "%s: CheckObject(%064x)", snapshot.Metadata.GetIndexShortID(), checksum)
	return snapshot.repository.CheckObject(checksum)
}

func (snapshot *Snapshot) Commit() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.Commit", time.Since(t0))
	}()

	serializedIndex, err := snapshot.Index.Serialize()
	if err != nil {
		return err
	}

	indexHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
	indexHasher.Write(serializedIndex)
	indexChecksum := indexHasher.Sum(nil)

	var indexChecksum32 [32]byte
	copy(indexChecksum32[:], indexChecksum[:])

	nbytes, err := snapshot.PutBlob(indexChecksum32, serializedIndex)
	if err != nil {
		return err
	}

	snapshot.Metadata.IndexChecksum = indexChecksum[:]
	snapshot.Metadata.IndexMemorySize = uint64(len(serializedIndex))
	snapshot.Metadata.IndexDiskSize = uint64(nbytes)

	serializedFilesystem, err := snapshot.Filesystem.Serialize()
	if err != nil {
		return err
	}

	fsHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
	fsHasher.Write(serializedFilesystem)
	filesystemChecksum := fsHasher.Sum(nil)
	var filesystemChecksum32 [32]byte
	copy(filesystemChecksum32[:], filesystemChecksum[:])

	nbytes, err = snapshot.PutBlob(filesystemChecksum32, serializedFilesystem)
	if err != nil {
		return err
	}

	snapshot.Metadata.FilesystemChecksum = filesystemChecksum[:]
	snapshot.Metadata.FilesystemMemorySize = uint64(len(serializedFilesystem))
	snapshot.Metadata.FilesystemDiskSize = uint64(nbytes)

	serializedMetadata, err := snapshot.Metadata.Serialize()
	if err != nil {
		return err
	}

	snapshotBytes, err := snapshot.prepareMetadata(serializedMetadata)
	if err != nil {
		return err
	}

	logger.Trace("snapshot", "%s: Commit()", snapshot.Metadata.GetIndexShortID())
	return snapshot.transaction.Commit(snapshotBytes)
}

func (snapshot *Snapshot) NewReader(pathname string) (*Reader, error) {
	return NewReader(snapshot, pathname)
}
