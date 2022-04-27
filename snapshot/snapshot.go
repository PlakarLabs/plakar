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

type Snapshot struct {
	repository  *storage.Repository
	transaction *storage.Transaction

	SkipDirs []string

	Metadata *Metadata
	Index    *Index
}

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

	if cache != nil && cacheMiss {
		logger.Trace("snapshot: cache.PutMetadata(%s)", indexID)
		cache.PutMetadata(repository.Configuration().RepositoryID.String(), indexID.String(), buffer)
	}

	secret := repository.GetSecret()
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

	metadata, err := NewMetadataFromBytes(buffer)
	if err != nil {
		return nil, false, err
	}

	return metadata, false, nil
}

func GetIndex(repository *storage.Repository, indexID uuid.UUID) (*Index, []byte, error) {
	cache := repository.GetCache()

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

	if cache != nil && cacheMiss {
		logger.Trace("snapshot: cache.PutIndex(%s)", indexID)
		cache.PutIndex(repository.Configuration().RepositoryID.String(), indexID.String(), buffer)
	}

	secret := repository.GetSecret()
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

	return index, checksum[:], nil
}

func List(repository *storage.Repository) ([]uuid.UUID, error) {
	return repository.GetIndexes()
}

func (snapshot *Snapshot) PutChunk(checksum [32]byte, data []byte) error {
	logger.Trace("snapshot: %s: PutChunk(%064x)", snapshot.Metadata.IndexID, checksum)
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
	return snapshot.repository.PutChunk(checksum, buffer)
}

func (snapshot *Snapshot) PutObject(checksum [32]byte, data []byte) error {
	logger.Trace("snapshot: %s: PutObject(%064x)", snapshot.Metadata.IndexID, checksum)

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
	return snapshot.repository.PutObject(checksum, buffer)
}

func (snapshot *Snapshot) PutMetadata(data []byte) error {
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot: %s: PutMetadata()", snapshot.Metadata.IndexID)
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

	if cache != nil {
		cache.PutMetadata(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.IndexID.String(), buffer)
	}
	return snapshot.transaction.PutMetadata(buffer)
}

func (snapshot *Snapshot) PutIndex(data []byte) error {
	cache := snapshot.repository.GetCache()
	logger.Trace("snapshot: %s: PutIndex()", snapshot.Metadata.IndexID)

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

	if cache != nil {
		cache.PutIndex(snapshot.repository.Configuration().RepositoryID.String(), snapshot.Metadata.IndexID.String(), buffer)
	}

	return snapshot.transaction.PutIndex(buffer)
}

func (snapshot *Snapshot) GetChunk(checksum [32]byte) ([]byte, error) {
	logger.Trace("snapshot: %s: GetChunk(%064x)", snapshot.Metadata.IndexID, checksum)
	buffer, err := snapshot.repository.GetChunk(checksum)
	if err != nil {
		return nil, err
	}

	secret := snapshot.repository.GetSecret()
	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if snapshot.repository.Configuration().Compression != "" {
		tmp, err := compression.Inflate(buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}
	return buffer, nil
}

func (snapshot *Snapshot) CheckChunk(checksum [32]byte) (bool, error) {
	logger.Trace("snapshot: %s: CheckChunk(%064x)", snapshot.Metadata.IndexID, checksum)
	exists, err := snapshot.repository.CheckChunk(checksum)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (snapshot *Snapshot) GetObject(checksum [32]byte) (*Object, error) {
	logger.Trace("snapshot: %s: GetObject(%064x)", snapshot.Metadata.IndexID, checksum)
	buffer, err := snapshot.repository.GetObject(checksum)
	if err != nil {
		return nil, err
	}

	secret := snapshot.repository.GetSecret()
	if secret != nil {
		tmp, err := encryption.Decrypt(secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if snapshot.repository.Configuration().Compression != "" {
		tmp, err := compression.Inflate(buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	object := &Object{}
	err = msgpack.Unmarshal(buffer, &object)
	return object, err
}

func (snapshot *Snapshot) CheckObject(checksum [32]byte) (bool, error) {
	logger.Trace("snapshot: %s: CheckObject(%064x)", snapshot.Metadata.IndexID, checksum)
	return snapshot.repository.CheckObject(checksum)
}

func (snapshot *Snapshot) Commit() error {

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

	err = snapshot.PutMetadata(serializedMetadata)
	if err != nil {
		return err
	}

	err = snapshot.PutIndex(serializedIndex)
	if err != nil {
		return err
	}

	logger.Trace("%s: Commit()", snapshot.Metadata.IndexID)
	return snapshot.transaction.Commit()
}
