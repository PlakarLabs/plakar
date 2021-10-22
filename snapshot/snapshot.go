package snapshot

import (
	"encoding/json"
	"time"

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage/fs"
)

func New(store *fs.FSStore, localCache *cache.Cache) Snapshot {
	tx := store.Transaction()
	snapshot := Snapshot{
		store:       store,
		transaction: &tx,

		Uuid:         tx.GetUuid(),
		CreationTime: time.Now(),
		Version:      "0.1.0",
		Hostname:     "",
		Username:     "",

		Directories: make(map[string]*FileInfo),
		Files:       make(map[string]*FileInfo),
		NonRegular:  make(map[string]*FileInfo),
		Pathnames:   make(map[string]string),
		Objects:     make(map[string]*Object),
		Chunks:      make(map[string]*Chunk),

		Cache: localCache,

		WrittenChunks:   make(map[string]bool),
		WrittenObjects:  make(map[string]bool),
		InflightChunks:  make(map[string]*Chunk),
		InflightObjects: make(map[string]*Object),
	}

	logger.Trace("snapshot %s: New()", snapshot.Uuid)
	return snapshot
}

func Load(store *fs.FSStore, Uuid string) (*Snapshot, error) {
	keypair := store.Keypair

	logger.Trace("snapshot: GetIndex(%s)", Uuid)
	buffer, err := store.GetIndex(Uuid)
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
	snapshot.Directories = snapshotStorage.Directories
	snapshot.Files = snapshotStorage.Files
	snapshot.NonRegular = snapshotStorage.NonRegular
	snapshot.Pathnames = snapshotStorage.Pathnames
	snapshot.Objects = snapshotStorage.Objects
	snapshot.Chunks = snapshotStorage.Chunks
	snapshot.Size = snapshotStorage.Size
	snapshot.store = store

	return snapshot, nil
}

func List(store *fs.FSStore) ([]string, error) {
	return store.GetIndexes()
}

func (snapshot *Snapshot) PutChunk(checksum string, data []byte) error {
	keypair := snapshot.store.Keypair

	buffer := compression.Deflate(data)
	if keypair != nil {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot %s: PutChunk(%s)", snapshot.Uuid, checksum)
	return snapshot.transaction.PutChunk(checksum, buffer)
}

func (snapshot *Snapshot) PutObject(checksum string, data []byte) error {
	keypair := snapshot.store.Keypair

	buffer := compression.Deflate(data)
	if keypair != nil {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot %s: PutObject(%s)", snapshot.Uuid, checksum)
	return snapshot.transaction.PutObject(checksum, buffer)
}

func (snapshot *Snapshot) PutIndex(data []byte) error {
	keypair := snapshot.store.Keypair

	buffer := compression.Deflate(data)
	if keypair != nil {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buffer)
		if err != nil {
			return err
		}
		buffer = tmp
	}

	logger.Trace("snapshot %s: PutIndex()", snapshot.Uuid)
	return snapshot.transaction.PutIndex(buffer)
}

func (snapshot *Snapshot) GetChunk(checksum string) ([]byte, error) {
	keypair := snapshot.store.Keypair

	logger.Trace("snapshot %s: GetChunk(%s)", snapshot.Uuid, checksum)
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

func (snapshot *Snapshot) GetObject(checksum string) (*Object, error) {
	keypair := snapshot.store.Keypair

	logger.Trace("snapshot %s: GetObject(%s)", snapshot.Uuid, checksum)
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

func (snapshot *Snapshot) Commit() error {
	snapshotStorage := SnapshotStorage{}
	snapshotStorage.Uuid = snapshot.Uuid
	snapshotStorage.CreationTime = snapshot.CreationTime
	snapshotStorage.Version = snapshot.Version
	snapshotStorage.Hostname = snapshot.Hostname
	snapshotStorage.Username = snapshot.Username
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

	logger.Trace("snapshot %s: Commit()", snapshot.Uuid)
	return snapshot.transaction.Commit()
}

func SnapshotToSummary(snapshot *Snapshot) *SnapshotSummary {
	ss := &SnapshotSummary{}
	ss.Uuid = snapshot.Uuid
	ss.CreationTime = snapshot.CreationTime
	ss.Version = snapshot.Version
	ss.Hostname = snapshot.Hostname
	ss.Username = snapshot.Username
	ss.Directories = uint64(len(snapshot.Directories))
	ss.Files = uint64(len(snapshot.Files))
	ss.NonRegular = uint64(len(snapshot.NonRegular))
	ss.Pathnames = uint64(len(snapshot.Pathnames))
	ss.Objects = uint64(len(snapshot.Objects))
	ss.Chunks = uint64(len(snapshot.Chunks))
	ss.Size = snapshot.Size
	return ss
}
