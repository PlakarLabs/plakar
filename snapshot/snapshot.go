package snapshot

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

func New(repository *storage.Repository) (*Snapshot, error) {
	tx, err := repository.Transaction()
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{
		repository:  repository,
		transaction: tx,

		Metadata: NewMetadata(tx.GetUuid()),
		Index:    NewIndex(),
	}

	logger.Trace("%s: New()", snapshot.Metadata.IndexID)
	return snapshot, nil
}

func Load(repository *storage.Repository, indexID uuid.UUID) (*Snapshot, error) {
	metadata, _, err := GetMetadata(repository, indexID)
	if err != nil {
		return nil, err
	}

	//if repository.Keypair != nil && !verified {
	//	return nil, fmt.Errorf("signature mismatches for metadata")
	//}

	index, checksum, err := GetIndex(repository, indexID)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(checksum, metadata.Checksum) {
		return nil, fmt.Errorf("index mismatches metadata checksum")
	}

	snapshot := &Snapshot{}
	snapshot.repository = repository
	snapshot.Metadata = metadata
	snapshot.Index = index

	return snapshot, nil
}

func GetMetadata(repository *storage.Repository, indexID uuid.UUID) (*Metadata, bool, error) {
	cache := repository.GetCache()
	secret := repository.GetSecret()
	//keypair := repository.GetKeypair()

	var orig_buffer []byte
	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot: cache.GetMetadata(%s)", indexID)
		tmp, err := cache.GetMetadata(repository.Configuration().RepositoryID.String(), indexID.String())
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot: repository.GetMetadata(%s)", indexID)
			tmp, err = repository.GetMetadata(indexID)
			if err != nil {
				return nil, false, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot: repository.GetMetadata(%s)", indexID)
		tmp, err := repository.GetMetadata(indexID)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}
	orig_buffer = buffer

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	if repository.Configuration().Compression != "" {
		tmp, err := compression.Inflate(buffer)
		if err != nil {
			return nil, false, err
		}
		buffer = tmp
	}

	//signature := []byte("")
	//if keypair != nil {
	//	tmp, sigbuf := buffer[0:len(buffer)-64], buffer[len(buffer)-64:]
	//	buffer = tmp
	//	signature = append(signature, sigbuf...)
	//}

	metadata, err := NewMetadataFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	//verified := false
	//if keypair != nil {
	//	publicKey, err := base64.StdEncoding.DecodeString(metadata.PublicKey)
	//	if err != nil {
	//		return nil, false, err
	//	}
	//
	//	verified = ed25519.Verify(ed25519.PublicKey(publicKey), buffer, signature)
	//}

	if cache != nil && cacheMiss {
		logger.Trace("snapshot: cache.PutMetadata(%s)", indexID)
		cache.PutMetadata(repository.Configuration().RepositoryID.String(), metadata.IndexID.String(), orig_buffer)
	}

	return metadata, false, nil
}

func GetIndex(repository *storage.Repository, indexID uuid.UUID) (*Index, []byte, error) {
	cache := repository.GetCache()
	secret := repository.GetSecret()

	var orig_buffer []byte
	var buffer []byte

	cacheMiss := false
	if cache != nil {
		logger.Trace("snapshot: cache.GetIndex(%s)", indexID)
		tmp, err := cache.GetIndex(repository.Configuration().RepositoryID.String(), indexID.String())
		if err != nil {
			cacheMiss = true
			logger.Trace("snapshot: repository.GetIndex(%s)", indexID)
			tmp, err = repository.GetIndex(indexID)
			if err != nil {
				return nil, nil, err
			}
		}
		buffer = tmp
	} else {
		logger.Trace("snapshot: repository.GetIndex(%s)", indexID)
		tmp, err := repository.GetIndex(indexID)
		if err != nil {
			return nil, nil, err
		}
		buffer = tmp
	}
	orig_buffer = buffer

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, nil, err
		}
		buffer = tmp
	}

	if repository.Configuration().Compression != "" {
		tmp, err := compression.Inflate(buffer)
		if err != nil {
			return nil, nil, err
		}
		buffer = tmp
	}

	index, err := NewIndexFromBytes(buffer)
	if err != nil {
		return nil, nil, err
	}

	checksum := sha256.Sum256(buffer)

	if cache != nil && cacheMiss {
		logger.Trace("snapshot: cache.PutIndex(%s)", indexID)
		cache.PutIndex(repository.Configuration().RepositoryID.String(), indexID.String(), orig_buffer)
	}

	return index, checksum[:], nil
}

func List(repository *storage.Repository) ([]uuid.UUID, error) {
	return repository.GetIndexes()
}

func (snapshot *Snapshot) PutChunk(checksum [32]byte, data []byte) error {
	secret := snapshot.repository.GetSecret()

	buffer := data
	if snapshot.repository.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutChunk(%064x)", snapshot.Metadata.IndexID, checksum)
	return snapshot.transaction.PutChunk(checksum, buffer)
}

func (snapshot *Snapshot) PutObject(checksum [32]byte, data []byte) error {
	secret := snapshot.repository.GetSecret()

	buffer := data
	if snapshot.repository.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutObject(%064x)", snapshot.Metadata.IndexID, checksum)
	return snapshot.transaction.PutObject(checksum, buffer)
}

func (snapshot *Snapshot) PutMetadata(data []byte) error {
	secret := snapshot.repository.GetSecret()

	buffer := data

	if snapshot.repository.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutMetadata()", snapshot.Metadata.IndexID)
	return snapshot.transaction.PutMetadata(buffer)
}

func (snapshot *Snapshot) PutIndex(data []byte) error {
	secret := snapshot.repository.GetSecret()

	buffer := data

	if snapshot.repository.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("%s: PutIndex()", snapshot.Metadata.IndexID)
	return snapshot.transaction.PutIndex(buffer)
}

func (snapshot *Snapshot) ReferenceChunks(keys [][32]byte) ([]bool, error) {
	logger.Trace("%s: ReferenceChunks([%d keys])", snapshot.Metadata.IndexID, len(keys))
	return snapshot.transaction.ReferenceChunks(keys)
}

func (snapshot *Snapshot) ReferenceObjects(keys [][32]byte) ([]bool, error) {
	logger.Trace("%s: ReferenceObjects([%d keys])", snapshot.Metadata.IndexID, len(keys))
	return snapshot.transaction.ReferenceObjects(keys)
}

func (snapshot *Snapshot) PutMetadataCache(data []byte) error {
	cache := snapshot.repository.GetCache()
	secret := snapshot.repository.GetSecret()

	buffer := data
	if snapshot.repository.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot: cache.PutMetadata(%s)", snapshot.Metadata.IndexID)
	return cache.PutMetadata(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.IndexID.String(), buffer)
}

func (snapshot *Snapshot) PutIndexCache(data []byte) error {
	cache := snapshot.repository.GetCache()
	secret := snapshot.repository.GetSecret()

	buffer := data
	if snapshot.repository.Configuration().Compression != "" {
		buffer = compression.Deflate(buffer)
	}

	if secret != nil {
		tmp, err := encryption.Encrypt(secret, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot: cache.PutIndex(%s)", snapshot.Metadata.IndexID)
	return cache.PutIndex(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.IndexID.String(), buffer)
}

func (snapshot *Snapshot) GetChunk(checksum [32]byte) ([]byte, error) {
	secret := snapshot.repository.GetSecret()

	logger.Trace("%s: GetChunk(%064x)", snapshot.Metadata.IndexID, checksum)
	buffer, err := snapshot.repository.GetChunk(checksum)
	if err != nil {
		return nil, err
	}

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if snapshot.repository.Configuration().Compression != "" {
		return compression.Inflate(buffer)
	}
	return buffer, nil
}

func (snapshot *Snapshot) CheckChunk(checksum [32]byte) (bool, error) {
	logger.Trace("%s: CheckChunk(%064x)", snapshot.Metadata.IndexID, checksum)
	exists, err := snapshot.repository.CheckChunk(checksum)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (snapshot *Snapshot) GetObject(checksum [32]byte) (*Object, error) {
	secret := snapshot.repository.GetSecret()

	logger.Trace("%s: GetObject(%064x)", snapshot.Metadata.IndexID, checksum)
	buffer, err := snapshot.repository.GetObject(checksum)
	if err != nil {
		return nil, err
	}

	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
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
	err = msgpack.Unmarshal(data, &object)
	return object, err
}

func (snapshot *Snapshot) CheckObject(checksum [32]byte) (bool, error) {
	logger.Trace("%s: CheckObject(%064x)", snapshot.Metadata.IndexID, checksum)
	exists, err := snapshot.repository.CheckObject(checksum)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (snapshot *Snapshot) Commit() error {
	cache := snapshot.repository.GetCache()
	//keypair := snapshot.repository.GetKeypair()

	serializedIndex, err := snapshot.Index.Serialize()
	if err != nil {
		return err
	}
	indexChecksum := sha256.Sum256(serializedIndex)
	snapshot.Metadata.Checksum = indexChecksum[:]

	snapshot.Metadata.IndexSize = uint64(len(serializedIndex))
	serializedMetadata, err := snapshot.Metadata.Serialize()
	if err != nil {
		return err
	}
	//if keypair != nil {
	//	tmp, err := keypair.Sign(serializedMetadata)
	//	if err != nil {
	//		return err
	//	}
	//	serializedMetadata = append(serializedMetadata, tmp...)
	//}

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

	logger.Trace("%s: Commit()", snapshot.Metadata.IndexID)
	return snapshot.transaction.Commit()
}

func (snapshot *Snapshot) StateSetChunkToObject(chunkChecksum [32]byte, objectChecksum [32]byte) {
	snapshot.Index.muChunkToObjects.Lock()
	defer snapshot.Index.muChunkToObjects.Unlock()

	if _, exists := snapshot.Index.ChunkToObjects[chunkChecksum]; !exists {
		snapshot.Index.ChunkToObjects[chunkChecksum] = make([][32]byte, 0)
	}

	for _, value := range snapshot.Index.ChunkToObjects[chunkChecksum] {
		if value == objectChecksum {
			return
		}
	}
	snapshot.Index.ChunkToObjects[chunkChecksum] = append(snapshot.Index.ChunkToObjects[chunkChecksum], objectChecksum)
}

func (snapshot *Snapshot) StateSetObjectToPathname(objectChecksum [32]byte, pathname string) {
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

func (snapshot *Snapshot) StateSetContentTypeToObjects(contentType string, objectChecksum [32]byte) {
	snapshot.Index.muContentTypeToObjects.Lock()
	defer snapshot.Index.muContentTypeToObjects.Unlock()

	if _, exists := snapshot.Index.ContentTypeToObjects[contentType]; !exists {
		snapshot.Index.ContentTypeToObjects[contentType] = make([][32]byte, 0)
	}

	for _, value := range snapshot.Index.ContentTypeToObjects[contentType] {
		if value == objectChecksum {
			return
		}
	}
	snapshot.Index.ContentTypeToObjects[contentType] = append(snapshot.Index.ContentTypeToObjects[contentType], objectChecksum)
}
