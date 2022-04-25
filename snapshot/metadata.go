package snapshot

import (
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type Statistics struct {
	Duration    time.Duration
	Chunks      uint64
	Objects     uint64
	Files       uint64
	Directories uint64
	NonRegular  uint64
	Pathnames   uint64

	Kind      map[string]uint64
	Type      map[string]uint64
	Extension map[string]uint64

	PercentKind      map[string]float64
	PercentType      map[string]float64
	PercentExtension map[string]float64
}

type Metadata struct {
	IndexID      uuid.UUID
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string
	CommandLine  string
	MachineID    string
	PublicKey    string
	Size         uint64
	Checksum     []byte

	ScannedDirectories []string

	IndexSize uint64

	Statistics Statistics
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
