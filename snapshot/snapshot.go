package snapshot

import (
	"encoding/json"
	"time"

	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/filesystem"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"
)

func New(store *storage.Store) (*Snapshot, error) {
	tx, err := store.Transaction()
	if err != nil {
		return nil, err
	}
	snapshot := &Snapshot{
		store:       store,
		transaction: tx,

		Uuid:         tx.GetUuid(),
		CreationTime: time.Now(),
		Version:      storage.VERSION,
		Hostname:     store.GetHostname(),
		Username:     store.GetUsername(),
		CommandLine:  store.GetCommandLine(),
		MachineID:    store.GetMachineID(),

		Filesystem: filesystem.NewFilesystem(),

		Pathnames: make(map[string]string),
		Objects:   make(map[string]*Object),
		Chunks:    make(map[string]*Chunk),

		ChunkToObjects:       make(map[string][]string),
		ObjectToPathnames:    make(map[string][]string),
		ContentTypeToObjects: make(map[string][]string),
	}

	logger.Trace("%s: New()", snapshot.Uuid)
	return snapshot, nil
}

func Load(store *storage.Store, Uuid string) (*Snapshot, error) {
	cache := store.GetCache()
	secret := store.GetSecret()

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

	data := buffer
	if secret != nil {
		tmp, err := encryption.Decrypt(secret.Key, data)
		if err != nil {
			return nil, err
		}
		data = tmp
	}

	if store.Configuration().Compression != "" {
		tmp, err := compression.Inflate(data)
		if err != nil {
			return nil, err
		}
		data = tmp
	}

	snapshot, err := snapshotFromBytes(data)
	if err != nil {
		return nil, err
	}
	snapshot.store = store

	if cache != nil && cacheMiss {
		snapshot.PutIndexCache(data)
	}

	return snapshot, nil
}

func List(store *storage.Store) ([]string, error) {
	return store.GetIndexes()
}

func (snapshot *Snapshot) GetChunkInfo(checksum string) (*Chunk, bool) {
	snapshot.muChunks.Lock()
	chunk, exists := snapshot.Chunks[checksum]
	snapshot.muChunks.Unlock()
	return chunk, exists
}

func (snapshot *Snapshot) PutChunk(checksum string, data []byte) error {
	secret := snapshot.store.GetSecret()

	buffer := data
	if snapshot.store.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret.Key, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutChunk(%s)", snapshot.Uuid, checksum)
	return snapshot.transaction.PutChunk(checksum, buffer)
}

func (snapshot *Snapshot) PutObject(checksum string, data []byte) error {
	secret := snapshot.store.GetSecret()

	buffer := data
	if snapshot.store.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret.Key, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutObject(%s)", snapshot.Uuid, checksum)
	return snapshot.transaction.PutObject(checksum, buffer)
}

func (snapshot *Snapshot) PutIndex(data []byte) error {
	secret := snapshot.store.GetSecret()

	buffer := data
	if snapshot.store.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret.Key, buffer)
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
	secret := snapshot.store.GetSecret()

	buffer := data
	if snapshot.store.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret.Key, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot: cache.PutIndex(%s)", snapshot.Uuid)
	return cache.PutSnapshot(snapshot.Uuid, buffer)
}

func (snapshot *Snapshot) GetChunk(checksum string) ([]byte, error) {
	secret := snapshot.store.GetSecret()

	logger.Trace("%s: GetChunk(%s)", snapshot.Uuid, checksum)
	buffer, err := snapshot.store.GetChunk(checksum)
	if err != nil {
		return nil, err
	}

	if secret != nil {
		tmp, err := encryption.Decrypt(secret.Key, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if snapshot.store.Configuration().Compression != "" {
		return compression.Inflate(buffer)
	}
	return buffer, nil
}

func (snapshot *Snapshot) CheckChunk(checksum string) (bool, error) {
	logger.Trace("%s: CheckChunk(%s)", snapshot.Uuid, checksum)
	exists, err := snapshot.store.CheckChunk(checksum)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (snapshot *Snapshot) GetObject(checksum string) (*Object, error) {
	secret := snapshot.store.GetSecret()

	logger.Trace("%s: GetObject(%s)", snapshot.Uuid, checksum)
	buffer, err := snapshot.store.GetObject(checksum)
	if err != nil {
		return nil, err
	}

	if secret != nil {
		tmp, err := encryption.Decrypt(secret.Key, buffer)
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
	return exists, nil
}

func (snapshot *Snapshot) Commit() error {
	cache := snapshot.store.GetCache()

	serialized, err := snapshotToBytes(snapshot)
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

func (snapshot *Snapshot) StateSetChunkToObject(chunkChecksum string, objectChecksum string) {
	snapshot.muChunkToObjects.Lock()
	defer snapshot.muChunkToObjects.Unlock()

	if _, exists := snapshot.ChunkToObjects[chunkChecksum]; !exists {
		snapshot.ChunkToObjects[chunkChecksum] = make([]string, 0)
	}

	for _, value := range snapshot.ChunkToObjects[chunkChecksum] {
		if value == objectChecksum {
			return
		}
	}
	snapshot.ChunkToObjects[chunkChecksum] = append(snapshot.ChunkToObjects[chunkChecksum], objectChecksum)
}

func (snapshot *Snapshot) StateSetObjectToPathname(objectChecksum string, pathname string) {
	snapshot.muObjectToPathnames.Lock()
	defer snapshot.muObjectToPathnames.Unlock()

	if _, exists := snapshot.ObjectToPathnames[objectChecksum]; !exists {
		snapshot.ObjectToPathnames[objectChecksum] = make([]string, 0)
	}

	for _, value := range snapshot.ObjectToPathnames[objectChecksum] {
		if value == pathname {
			return
		}
	}
	snapshot.ObjectToPathnames[objectChecksum] = append(snapshot.ObjectToPathnames[objectChecksum], pathname)
}

func (snapshot *Snapshot) StateSetContentTypeToObjects(contentType string, objectChecksum string) {
	snapshot.muContentTypeToObjects.Lock()
	defer snapshot.muContentTypeToObjects.Unlock()

	if _, exists := snapshot.ContentTypeToObjects[contentType]; !exists {
		snapshot.ContentTypeToObjects[contentType] = make([]string, 0)
	}

	for _, value := range snapshot.ContentTypeToObjects[contentType] {
		if value == objectChecksum {
			return
		}
	}
	snapshot.ContentTypeToObjects[contentType] = append(snapshot.ContentTypeToObjects[contentType], objectChecksum)
}
