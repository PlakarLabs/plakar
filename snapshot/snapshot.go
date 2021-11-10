package snapshot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"
)

func New(store *storage.Store, localCache *cache.Cache) (*Snapshot, error) {
	tx, err := store.Transaction()
	if err != nil {
		return nil, err
	}
	snapshot := &Snapshot{
		store:       store,
		transaction: tx,

		Uuid:         tx.GetUuid(),
		CreationTime: time.Now(),
		Version:      "0.1.0",
		Hostname:     "",
		Username:     "",
		CommandLine:  "",

		Roots:       make([]string, 0),
		Directories: make(map[string]*FileInfo),
		Files:       make(map[string]*FileInfo),
		NonRegular:  make(map[string]*FileInfo),
		Pathnames:   make(map[string]string),
		Objects:     make(map[string]*Object),
		Chunks:      make(map[string]*Chunk),

		WrittenChunks:   make(map[string]bool),
		WrittenObjects:  make(map[string]bool),
		InflightChunks:  make(map[string]*Chunk),
		InflightObjects: make(map[string]*Object),
	}

	logger.Trace("%s: New()", snapshot.Uuid)
	return snapshot, nil
}

func Load(store *storage.Store, Uuid string) (*Snapshot, error) {
	cache := store.GetCache()
	keypair := store.GetKeypair()

	var buffer []byte
	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot: cache.GetIndex(%s)", Uuid)
		tmp, err := cache.GetSnapshot(Uuid)
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot: GetIndex(%s)", Uuid)
			tmp, err = store.GetIndex(Uuid)
			if err != nil {
				return nil, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot: GetIndex(%s)", Uuid)
		tmp, err := store.GetIndex(Uuid)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if keypair != nil {
		tmp, err := encryption.Decrypt(keypair.MasterKey, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	data, err := compression.Inflate(buffer)
	if err != nil {
		return nil, err
	}

	var snapshotStorage SnapshotStorage
	if err := json.Unmarshal(data, &snapshotStorage); err != nil {
		return nil, err
	}

	snapshot := &Snapshot{}
	snapshot.Uuid = snapshotStorage.Uuid
	snapshot.CreationTime = snapshotStorage.CreationTime
	snapshot.Version = snapshotStorage.Version
	snapshot.Hostname = snapshotStorage.Hostname
	snapshot.Username = snapshotStorage.Username
	snapshot.CommandLine = snapshotStorage.CommandLine
	snapshot.Roots = snapshotStorage.Roots
	snapshot.Directories = snapshotStorage.Directories
	snapshot.Files = snapshotStorage.Files
	snapshot.NonRegular = snapshotStorage.NonRegular
	snapshot.Pathnames = snapshotStorage.Pathnames
	snapshot.Objects = snapshotStorage.Objects
	snapshot.Chunks = snapshotStorage.Chunks
	snapshot.Size = snapshotStorage.Size
	snapshot.store = store

	if cache != nil && cacheMiss {
		snapshot.PutIndexCache(data)
	}

	return snapshot, nil
}

func List(store *storage.Store) ([]string, error) {
	return store.GetIndexes()
}

func (snapshot *Snapshot) PutChunk(checksum string, data []byte) error {
	keypair := snapshot.store.GetKeypair()

	buffer := compression.Deflate(data)
	if keypair != nil {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutChunk(%s)", snapshot.Uuid, checksum)
	return snapshot.transaction.PutChunk(checksum, buffer)
}

func (snapshot *Snapshot) PutObject(checksum string, data []byte) error {
	keypair := snapshot.store.GetKeypair()

	buffer := compression.Deflate(data)
	if keypair != nil {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutObject(%s)", snapshot.Uuid, checksum)
	return snapshot.transaction.PutObject(checksum, buffer)
}

func (snapshot *Snapshot) PutIndex(data []byte) error {
	keypair := snapshot.store.GetKeypair()

	buffer := compression.Deflate(data)
	if keypair != nil {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutIndex()", snapshot.Uuid)
	return snapshot.transaction.PutIndex(buffer)
}

func (snapshot *Snapshot) ReferenceChunks(keys []string) ([]bool, error) {
	logger.Trace("%s: ReferenceChunks([%d keys])", snapshot.Uuid, len(keys))
	return snapshot.transaction.ReferenceChunks(keys)
}

func (snapshot *Snapshot) ReferenceObjects(keys []string) ([]bool, error) {
	logger.Trace("%s: ReferenceObjects([%d keys])", snapshot.Uuid, len(keys))
	return snapshot.transaction.ReferenceObjects(keys)
}

func (snapshot *Snapshot) PutIndexCache(data []byte) error {
	cache := snapshot.store.GetCache()
	keypair := snapshot.store.GetKeypair()

	buffer := compression.Deflate(data)
	if keypair != nil {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot: cache.PutIndex(%s)", snapshot.Uuid)
	return cache.PutSnapshot(snapshot.Uuid, buffer)
}

func (snapshot *Snapshot) GetChunk(checksum string) ([]byte, error) {
	keypair := snapshot.store.GetKeypair()

	logger.Trace("%s: GetChunk(%s)", snapshot.Uuid, checksum)
	buffer, err := snapshot.store.GetChunk(checksum)
	if err != nil {
		return nil, err
	}

	if keypair != nil {
		tmp, err := encryption.Decrypt(keypair.MasterKey, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	return compression.Inflate(buffer)
}

func (snapshot *Snapshot) CheckChunk(checksum string) (bool, error) {
	logger.Trace("%s: CheckChunk(%s)", snapshot.Uuid, checksum)
	exists, err := snapshot.store.CheckChunk(checksum)
	if err != nil {
		return false, err
	}
	return exists == true, nil
}

func (snapshot *Snapshot) GetObject(checksum string) (*Object, error) {
	keypair := snapshot.store.GetKeypair()

	logger.Trace("%s: GetObject(%s)", snapshot.Uuid, checksum)
	buffer, err := snapshot.store.GetObject(checksum)
	if err != nil {
		return nil, err
	}

	if keypair != nil {
		tmp, err := encryption.Decrypt(keypair.MasterKey, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	data, err := compression.Inflate(buffer)
	if err != nil {
		return nil, err
	}

	object := &Object{}
	err = json.Unmarshal(data, &object)
	return object, err
}

func (snapshot *Snapshot) CheckObject(checksum string) (bool, error) {
	logger.Trace("%s: CheckObject(%s)", snapshot.Uuid, checksum)
	exists, err := snapshot.store.CheckObject(checksum)
	if err != nil {
		return false, err
	}
	return exists == true, nil
}

func (snapshot *Snapshot) Commit() error {
	cache := snapshot.store.GetCache()

	snapshotStorage := SnapshotStorage{}
	snapshotStorage.Uuid = snapshot.Uuid
	snapshotStorage.CreationTime = snapshot.CreationTime
	snapshotStorage.Version = snapshot.Version
	snapshotStorage.Hostname = snapshot.Hostname
	snapshotStorage.Username = snapshot.Username
	snapshotStorage.CommandLine = snapshot.CommandLine
	snapshotStorage.Roots = snapshot.Roots
	snapshotStorage.Directories = snapshot.Directories
	snapshotStorage.Files = snapshot.Files
	snapshotStorage.NonRegular = snapshot.NonRegular
	snapshotStorage.Pathnames = snapshot.Pathnames
	snapshotStorage.Objects = snapshot.Objects
	snapshotStorage.Chunks = snapshot.Chunks
	snapshotStorage.Size = snapshot.Size

	serialized, err := json.Marshal(snapshotStorage)
	if err != nil {
		return err
	}

	err = snapshot.PutIndex(serialized)
	if err != nil {
		return err
	}

	if cache != nil {
		snapshot.PutIndexCache(serialized)
	}

	logger.Trace("%s: Commit()", snapshot.Uuid)
	return snapshot.transaction.Commit()
}

func (snapshot *Snapshot) GetCachedObject(pathname string) (*CachedObject, error) {
	keypair := snapshot.store.GetKeypair()
	cache := snapshot.store.GetCache()

	pathHash := sha256.New()
	pathHash.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHash.Sum(nil))

	data, err := cache.GetPath(hashedPath)
	if err != nil {
		logger.Trace("%s: cache.GetPath(%s): KO", snapshot.Uuid, pathname)
		return nil, err
	}
	logger.Trace("%s: cache.GetPath(%s): OK", snapshot.Uuid, pathname)

	if snapshot.store.Configuration().Encrypted != "" {
		tmp, err := encryption.Decrypt(keypair.MasterKey, data)
		if err != nil {
			return nil, err
		}
		data = tmp
	}

	data, err = compression.Inflate(data)
	if err != nil {
		return nil, err
	}

	cacheObject := CachedObject{}
	err = json.Unmarshal(data, &cacheObject)
	if err != nil {
		return nil, err
	}
	cacheObject.Info.path = pathname
	return &cacheObject, nil
}

func (snapshot *Snapshot) PutCachedObject(object Object, fi FileInfo) error {
	keypair := snapshot.store.GetKeypair()
	cache := snapshot.store.GetCache()

	pathHash := sha256.New()
	pathHash.Write([]byte(object.path))
	hashedPath := fmt.Sprintf("%032x", pathHash.Sum(nil))

	cacheObject := CachedObject{}
	cacheObject.Checksum = object.Checksum
	cacheObject.Chunks = object.Chunks
	cacheObject.ContentType = object.ContentType
	cacheObject.Info = fi

	jobject, err := json.Marshal(cacheObject)
	if err != nil {
		return err
	}

	jobject = compression.Deflate(jobject)
	if snapshot.store.Configuration().Encrypted != "" {
		tmp, err := encryption.Encrypt(keypair.MasterKey, jobject)
		if err != nil {
			return err
		}
		jobject = tmp
	}

	logger.Trace("%s: cache.PutPath(%s)", snapshot.Uuid, fi.path)
	cache.PutPath(hashedPath, jobject)
	return nil
}

func (snapshot *Snapshot) HumanSize() string {
	return humanize.Bytes(snapshot.Size)
}

func (snapshot *Snapshot) StateAddRoot(pathname string) {
	snapshot.muRoots.Lock()
	defer snapshot.muRoots.Unlock()

	for _, path := range snapshot.Roots {
		if pathname == path {
			return
		}
	}
	snapshot.Roots = append(snapshot.Roots, pathname)
}

func (snapshot *Snapshot) StateSetDirectory(pathname string, fileinfo *FileInfo) {
	snapshot.muDirectories.Lock()
	defer snapshot.muDirectories.Unlock()

	snapshot.Directories[pathname] = fileinfo
}

func (snapshot *Snapshot) StateSetFile(pathname string, fileinfo *FileInfo) {
	snapshot.muFiles.Lock()
	defer snapshot.muFiles.Unlock()

	snapshot.Files[pathname] = fileinfo
}

func (snapshot *Snapshot) StateSetNonRegular(pathname string, fileinfo *FileInfo) {
	snapshot.muNonRegular.Lock()
	defer snapshot.muNonRegular.Unlock()

	snapshot.NonRegular[pathname] = fileinfo
}

func (snapshot *Snapshot) StateGetPathname(pathname string) (string, bool) {
	snapshot.muPathnames.Lock()
	defer snapshot.muPathnames.Unlock()

	value, exists := snapshot.Pathnames[pathname]
	return value, exists
}

func (snapshot *Snapshot) StateSetPathname(pathname string, checksum string) {
	snapshot.muPathnames.Lock()
	defer snapshot.muPathnames.Unlock()

	snapshot.Pathnames[pathname] = checksum
}

func (snapshot *Snapshot) StateGetObject(checksum string) (*Object, bool) {
	snapshot.muObjects.Lock()
	defer snapshot.muObjects.Unlock()

	value, exists := snapshot.Objects[checksum]
	return value, exists
}

func (snapshot *Snapshot) StateSetObject(checksum string, object *Object) {
	snapshot.muObjects.Lock()
	defer snapshot.muObjects.Unlock()

	snapshot.Objects[checksum] = object
}

func (snapshot *Snapshot) StateGetChunk(checksum string) (*Chunk, bool) {
	snapshot.muChunks.Lock()
	defer snapshot.muChunks.Unlock()

	value, exists := snapshot.Chunks[checksum]
	return value, exists
}

func (snapshot *Snapshot) StateSetChunk(checksum string, chunk *Chunk) {
	snapshot.muChunks.Lock()
	defer snapshot.muChunks.Unlock()

	snapshot.Chunks[checksum] = chunk
}

func (snapshot *Snapshot) StateGetWrittenChunk(checksum string) (bool, bool) {
	snapshot.muWrittenChunks.Lock()
	defer snapshot.muWrittenChunks.Unlock()

	value, exists := snapshot.WrittenChunks[checksum]
	return value, exists
}

func (snapshot *Snapshot) StateSetWrittenChunk(checksum string, written bool) {
	snapshot.muWrittenChunks.Lock()
	defer snapshot.muWrittenChunks.Unlock()

	snapshot.WrittenChunks[checksum] = written
}

func (snapshot *Snapshot) StateGetInflightChunk(checksum string) (*Chunk, bool) {
	snapshot.muInflightChunks.Lock()
	defer snapshot.muInflightChunks.Unlock()

	value, exists := snapshot.InflightChunks[checksum]
	return value, exists
}

func (snapshot *Snapshot) StateSetInflightChunk(checksum string, chunk *Chunk) {
	snapshot.muInflightChunks.Lock()
	defer snapshot.muInflightChunks.Unlock()

	snapshot.InflightChunks[checksum] = chunk
}

func (snapshot *Snapshot) StateDeleteInflightChunk(checksum string) {
	snapshot.muInflightChunks.Lock()
	defer snapshot.muInflightChunks.Unlock()

	delete(snapshot.InflightChunks, checksum)
}

func (snapshot *Snapshot) StateGetWrittenObject(checksum string) (bool, bool) {
	snapshot.muWrittenObjects.Lock()
	defer snapshot.muWrittenObjects.Unlock()

	value, exists := snapshot.WrittenObjects[checksum]
	return value, exists
}

func (snapshot *Snapshot) StateSetWrittenObject(checksum string, written bool) {
	snapshot.muWrittenObjects.Lock()
	defer snapshot.muWrittenObjects.Unlock()

	snapshot.WrittenObjects[checksum] = written
}

func (snapshot *Snapshot) StateGetInflightObject(checksum string) (*Object, bool) {
	snapshot.muInflightObjects.Lock()
	defer snapshot.muInflightObjects.Unlock()

	value, exists := snapshot.InflightObjects[checksum]
	return value, exists
}

func (snapshot *Snapshot) StateSetInflightObject(checksum string, object *Object) {
	snapshot.muInflightObjects.Lock()
	defer snapshot.muInflightObjects.Unlock()

	snapshot.InflightObjects[checksum] = object
}

func (snapshot *Snapshot) StateDeleteInflightObject(checksum string) {
	snapshot.muInflightObjects.Lock()
	defer snapshot.muInflightObjects.Unlock()

	delete(snapshot.InflightObjects, checksum)
}

func (fi *FileInfo) HumanSize() string {
	return humanize.Bytes(uint64(fi.Size))
}
