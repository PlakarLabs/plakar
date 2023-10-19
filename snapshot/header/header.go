package header

import (
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type Header struct {
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

func NewHeader(indexID uuid.UUID) *Header {
	return &Header{
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

func NewFromBytes(serialized []byte) (*Header, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("header.NewIndexFromBytes", time.Since(t0))
		logger.Trace("header", "NewMetadataFromBytes(...): %s", time.Since(t0))
	}()

	var header Header
	if err := msgpack.Unmarshal(serialized, &header); err != nil {
		return nil, err
	}

	return &header, nil
}

func (h *Header) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("header.Serialize", time.Since(t0))
		logger.Trace("header", "Serialize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(h)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}

func (h *Header) GetIndexID() uuid.UUID {
	return h.IndexID
}

func (h *Header) GetIndexShortID() string {
	return h.IndexID.String()[:8]
}
