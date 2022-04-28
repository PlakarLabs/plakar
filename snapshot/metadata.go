package snapshot

import (
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type Metadata struct {
	IndexID         uuid.UUID
	CreationTime    time.Time
	Version         string
	Hostname        string
	Username        string
	CommandLine     string
	OperatingSystem string
	MachineID       string
	PublicKey       string
	Size            uint64

	IndexChecksum      []byte
	FilesystemChecksum []byte

	ScannedDirectories []string

	IndexSize      uint64
	FilesystemSize uint64

	Statistics *Statistics
}

func NewMetadata(indexID uuid.UUID) *Metadata {
	return &Metadata{
		IndexID:      indexID,
		CreationTime: time.Now(),
		Version:      storage.VERSION,
		Hostname:     "",
		Username:     "",
		CommandLine:  "",
		MachineID:    "",
		PublicKey:    "",

		Statistics: NewStatistics(),
	}
}

func NewMetadataFromBytes(serialized []byte) (*Metadata, error) {
	var metadata Metadata
	if err := msgpack.Unmarshal(serialized, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (metadata *Metadata) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}

func (metadata *Metadata) GetIndexID() uuid.UUID {
	return metadata.IndexID
}

func (metadata *Metadata) GetIndexShortID() string {
	return metadata.IndexID.String()[:8]
}
