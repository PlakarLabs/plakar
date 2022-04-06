package snapshot

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

	keypair := store.GetKeypair()
	pubkey := []byte("")
	if keypair != nil {
		pubkey = keypair.PublicKey
	}

	snapshot := &Snapshot{
		store:       store,
		transaction: tx,

		Metadata: Metadata{
			Uuid:         tx.GetUuid(),
			CreationTime: time.Now(),
			Version:      storage.VERSION,
			Hostname:     store.GetHostname(),
			Username:     store.GetUsername(),
			CommandLine:  store.GetCommandLine(),
			MachineID:    store.GetMachineID(),
			PublicKey:    base64.StdEncoding.EncodeToString(pubkey),

			Statistics: Statistics{
				Chunks:      0,
				Objects:     0,
				Files:       0,
				Directories: 0,

				Kind:      make(map[string]uint64),
				Type:      make(map[string]uint64),
				Extension: make(map[string]uint64),

				PercentKind:      make(map[string]float64),
				PercentType:      make(map[string]float64),
				PercentExtension: make(map[string]float64),
			},
		},

		Index: Index{
			Filesystem: filesystem.NewFilesystem(),

			Pathnames: make(map[string]string),
			Objects:   make(map[string]*Object),
			Chunks:    make(map[string]*Chunk),

			ChunkToObjects:       make(map[string][]string),
			ObjectToPathnames:    make(map[string][]string),
			ContentTypeToObjects: make(map[string][]string),
		},
	}

	logger.Trace("%s: New()", snapshot.Metadata.Uuid)
	return snapshot, nil
}

func Load(store *storage.Store, Uuid string) (*Snapshot, error) {
	metadata, verified, err := GetMetadata(store, Uuid)
	if err != nil {
		return nil, err
	}

	if store.Keypair != nil && !verified {
		return nil, fmt.Errorf("signature mismatches for metadata")
	}

	index, checksum, err := GetIndex(store, Uuid)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(checksum, metadata.Checksum) {
		return nil, fmt.Errorf("index mismatches metadata checksum")
	}

	snapshot := &Snapshot{}
	snapshot.store = store

	snapshot.Metadata = *metadata
	snapshot.Index = *index

	return snapshot, nil
}

func GetMetadata(store *storage.Store, Uuid string) (*Metadata, bool, error) {
	cache := store.GetCache()
	secret := store.GetSecret()
	keypair := store.GetKeypair()

	var orig_buffer []byte
	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot: cache.GetMetadata(%s)", Uuid)
		tmp, err := cache.GetMetadata(Uuid)
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot: store.GetMetadata(%s)", Uuid)
			tmp, err = store.GetMetadata(Uuid)
			if err != nil {
				return nil, false, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot: GetMetadata(%s)", Uuid)
		tmp, err := store.GetMetadata(Uuid)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}
	orig_buffer = buffer

	if secret != nil {
		tmp, err := encryption.Decrypt(secret.Key, buffer)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	if store.Configuration().Compression != "" {
		tmp, err := compression.Inflate(buffer)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	signature := []byte("")
	if keypair != nil {
		tmp, sigbuf := buffer[0:len(buffer)-64], buffer[len(buffer)-64:]
		buffer = tmp
		signature = append(signature, sigbuf...)
	}

	metadata, err := metadataFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	verified := false
	if keypair != nil {
		publicKey, err := base64.StdEncoding.DecodeString(metadata.PublicKey)
		if err != nil {
			return nil, false, err
		}

		verified = ed25519.Verify(ed25519.PublicKey(publicKey), buffer, signature)
	}

	if cache != nil && cacheMiss {
		putMetadataCache(store, metadata.Uuid, orig_buffer)
	}

	return metadata, verified, nil
}

func GetIndex(store *storage.Store, Uuid string) (*Index, []byte, error) {
	cache := store.GetCache()
	secret := store.GetSecret()

	var orig_buffer []byte
	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot: cache.GetIndex(%s)", Uuid)
		tmp, err := cache.GetIndex(Uuid)
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot: store.GetIndex(%s)", Uuid)
			tmp, err = store.GetIndex(Uuid)
			if err != nil {
				return nil, nil, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot: GetIndex(%s)", Uuid)
		tmp, err := store.GetIndex(Uuid)
		if err != nil {
			return nil, nil, err
		}
		buffer = tmp
	}
	orig_buffer = buffer

	if secret != nil {
		tmp, err := encryption.Decrypt(secret.Key, buffer)
		if err != nil {
			return nil, nil, err
		}
		buffer = tmp
	}

	if store.Configuration().Compression != "" {
		tmp, err := compression.Inflate(buffer)
		if err != nil {
			return nil, nil, err
		}
		buffer = tmp
	}

	index, err := indexFromBytes(buffer)
	if err != nil {
		return nil, nil, err
	}

	checksum := sha256.Sum256(buffer)

	if cache != nil && cacheMiss {
		putIndexCache(store, Uuid, orig_buffer)
	}

	return index, checksum[:], nil
}

func List(store *storage.Store) ([]string, error) {
	return store.GetIndexes()
}

func (snapshot *Snapshot) GetChunkInfo(checksum string) (*Chunk, bool) {
	snapshot.Index.muChunks.Lock()
	chunk, exists := snapshot.Index.Chunks[checksum]
	snapshot.Index.muChunks.Unlock()
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

	logger.Trace("%s: PutChunk(%s)", snapshot.Metadata.Uuid, checksum)
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

	logger.Trace("%s: PutObject(%s)", snapshot.Metadata.Uuid, checksum)
	return snapshot.transaction.PutObject(checksum, buffer)
}

func (snapshot *Snapshot) PutMetadata(data []byte) error {
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

	logger.Trace("%s: PutMetadata()", snapshot.Metadata.Uuid)
	return snapshot.transaction.PutMetadata(buffer)
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

	logger.Trace("%s: PutIndex()", snapshot.Metadata.Uuid)
	return snapshot.transaction.PutIndex(buffer)
}

func (snapshot *Snapshot) ReferenceChunks(keys []string) ([]bool, error) {
	logger.Trace("%s: ReferenceChunks([%d keys])", snapshot.Metadata.Uuid, len(keys))
	return snapshot.transaction.ReferenceChunks(keys)
}

func (snapshot *Snapshot) ReferenceObjects(keys []string) ([]bool, error) {
	logger.Trace("%s: ReferenceObjects([%d keys])", snapshot.Metadata.Uuid, len(keys))
	return snapshot.transaction.ReferenceObjects(keys)
}

func (snapshot *Snapshot) PutMetadataCache(data []byte) error {
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

	logger.Trace("snapshot: cache.PutMetadata(%s)", snapshot.Metadata.Uuid)
	return cache.PutMetadata(snapshot.Metadata.Uuid, buffer)
}

func putIndexCache(store *storage.Store, Uuid string, data []byte) error {
	cache := store.GetCache()
	secret := store.GetSecret()

	buffer := data
	if store.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret.Key, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot: cache.PutIndex(%s)", Uuid)
	return cache.PutIndex(Uuid, buffer)
}

func putMetadataCache(store *storage.Store, Uuid string, data []byte) error {
	cache := store.GetCache()
	secret := store.GetSecret()

	buffer := data
	if store.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret.Key, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot: cache.PutMetadata(%s)", Uuid)
	return cache.PutMetadata(Uuid, buffer)
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

	logger.Trace("snapshot: cache.PutIndex(%s)", snapshot.Metadata.Uuid)
	return cache.PutIndex(snapshot.Metadata.Uuid, buffer)
}

func (snapshot *Snapshot) GetChunk(checksum string) ([]byte, error) {
	secret := snapshot.store.GetSecret()

	logger.Trace("%s: GetChunk(%s)", snapshot.Metadata.Uuid, checksum)
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
	logger.Trace("%s: CheckChunk(%s)", snapshot.Metadata.Uuid, checksum)
	exists, err := snapshot.store.CheckChunk(checksum)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (snapshot *Snapshot) GetObject(checksum string) (*Object, error) {
	secret := snapshot.store.GetSecret()

	logger.Trace("%s: GetObject(%s)", snapshot.Metadata.Uuid, checksum)
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
	logger.Trace("%s: CheckObject(%s)", snapshot.Metadata.Uuid, checksum)
	exists, err := snapshot.store.CheckObject(checksum)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (snapshot *Snapshot) Commit() error {
	cache := snapshot.store.GetCache()
	keypair := snapshot.store.GetKeypair()

	serializedIndex, err := indexToBytes(&snapshot.Index)
	if err != nil {
		return err
	}
	indexChecksum := sha256.Sum256(serializedIndex)
	snapshot.Metadata.Checksum = indexChecksum[:]

	serializedMetadata, err := metadataToBytes(&snapshot.Metadata)
	if err != nil {
		return err
	}
	if keypair != nil {
		tmp, err := keypair.Sign(serializedMetadata)
		if err != nil {
			return err
		}
		serializedMetadata = append(serializedMetadata, tmp...)
	}

	err = snapshot.PutMetadata(serializedMetadata)
	if err != nil {
		return err
	}

	err = snapshot.PutIndex(serializedIndex)
	if err != nil {
		return err
	}

	if cache != nil {
		snapshot.PutMetadataCache(serializedMetadata)
		snapshot.PutIndexCache(serializedIndex)
	}

	logger.Trace("%s: Commit()", snapshot.Metadata.Uuid)
	return snapshot.transaction.Commit()
}

func (snapshot *Snapshot) StateSetChunkToObject(chunkChecksum string, objectChecksum string) {
	snapshot.Index.muChunkToObjects.Lock()
	defer snapshot.Index.muChunkToObjects.Unlock()

	if _, exists := snapshot.Index.ChunkToObjects[chunkChecksum]; !exists {
		snapshot.Index.ChunkToObjects[chunkChecksum] = make([]string, 0)
	}

	for _, value := range snapshot.Index.ChunkToObjects[chunkChecksum] {
		if value == objectChecksum {
			return
		}
	}
	snapshot.Index.ChunkToObjects[chunkChecksum] = append(snapshot.Index.ChunkToObjects[chunkChecksum], objectChecksum)
}

func (snapshot *Snapshot) StateSetObjectToPathname(objectChecksum string, pathname string) {
	snapshot.Index.muObjectToPathnames.Lock()
	defer snapshot.Index.muObjectToPathnames.Unlock()

	if _, exists := snapshot.Index.ObjectToPathnames[objectChecksum]; !exists {
		snapshot.Index.ObjectToPathnames[objectChecksum] = make([]string, 0)
	}

	for _, value := range snapshot.Index.ObjectToPathnames[objectChecksum] {
		if value == pathname {
			return
		}
	}
	snapshot.Index.ObjectToPathnames[objectChecksum] = append(snapshot.Index.ObjectToPathnames[objectChecksum], pathname)
}

func (snapshot *Snapshot) StateSetContentTypeToObjects(contentType string, objectChecksum string) {
	snapshot.Index.muContentTypeToObjects.Lock()
	defer snapshot.Index.muContentTypeToObjects.Unlock()

	if _, exists := snapshot.Index.ContentTypeToObjects[contentType]; !exists {
		snapshot.Index.ContentTypeToObjects[contentType] = make([]string, 0)
	}

	for _, value := range snapshot.Index.ContentTypeToObjects[contentType] {
		if value == objectChecksum {
			return
		}
	}
	snapshot.Index.ContentTypeToObjects[contentType] = append(snapshot.Index.ContentTypeToObjects[contentType], objectChecksum)
}
