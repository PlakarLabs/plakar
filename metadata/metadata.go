package metadata

import (
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type Metadata struct {
	IndexID          uuid.UUID
	Version          string
	CreationTime     time.Time
	CreationDuration time.Duration
	PublicKey        string
	Tags             []string

	Hostname        string
	Username        string
	OperatingSystem string
	MachineID       string
	CommandLine     string

	ScanSize          uint64
	ScanProcessedSize uint64

	ScannedDirectories []string

	IndexChecksum   []byte
	IndexDiskSize   uint64
	IndexMemorySize uint64

	FilesystemChecksum   []byte
	FilesystemDiskSize   uint64
	FilesystemMemorySize uint64

	ChunksCount  uint64
	ChunksSize   uint64
	ObjectsCount uint64

	FilesCount       uint64
	DirectoriesCount uint64
	NonRegularCount  uint64
	PathnamesCount   uint64

	ObjectsTransferCount uint64
	ObjectsTransferSize  uint64

	ChunksTransferCount uint64
	ChunksTransferSize  uint64

	FileKind      map[string]uint64
	FileType      map[string]uint64
	FileExtension map[string]uint64

	FilePercentKind      map[string]float64
	FilePercentType      map[string]float64
	FilePercentExtension map[string]float64
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

		FileKind:      make(map[string]uint64),
		FileType:      make(map[string]uint64),
		FileExtension: make(map[string]uint64),

		FilePercentKind:      make(map[string]float64),
		FilePercentType:      make(map[string]float64),
		FilePercentExtension: make(map[string]float64),
	}
}

func NewMetadataFromBytes(serialized []byte) (*Metadata, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("metadata.NewIndexFromBytes", time.Since(t0))
		logger.Trace("metadata", "NewMetadataFromBytes(...): %s", time.Since(t0))
	}()

	var metadata Metadata
	if err := msgpack.Unmarshal(serialized, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (metadata *Metadata) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("metadata.Serialize", time.Since(t0))
		logger.Trace("metadata", "Serialize(): %s", time.Since(t0))
	}()

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
