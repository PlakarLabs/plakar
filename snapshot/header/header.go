package header

import (
	"time"

	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/profiler"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type Blob struct {
	Type     string
	Version  string
	Checksum [32]byte
	Size     uint64
}

type Header struct {
	SnapshotID       objects.Checksum
	Version          string
	CreationTime     time.Time
	CreationDuration time.Duration
	PublicKey        string
	Tags             []string

	Hostname        string
	Username        string
	OperatingSystem string
	Architecture    string
	NumCPU          int
	MachineID       string
	ProcessID       int
	Client          string
	CommandLine     string

	Type   string
	Origin string

	ScanSize          uint64
	ScanProcessedSize uint64

	Root       objects.Checksum
	Metadata   objects.Checksum
	Statistics objects.Checksum

	ScannedDirectory string

	FilesCount       uint64
	DirectoriesCount uint64
}

func NewHeader(indexID [32]byte) *Header {
	return &Header{
		SnapshotID:   indexID,
		CreationTime: time.Now(),
		Version:      storage.VERSION,
		Hostname:     "",
		Username:     "",
		CommandLine:  "",
		MachineID:    "",
		PublicKey:    "",

		Root:       [32]byte{},
		Metadata:   [32]byte{},
		Statistics: [32]byte{},

		//FileKind:      make(map[string]uint64),
		//FileType:      make(map[string]uint64),
		//FileExtension: make(map[string]uint64),

		//FilePercentKind:      make(map[string]float64),
		//FilePercentType:      make(map[string]float64),
		//FilePercentExtension: make(map[string]float64),
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
	} else {
		return &header, nil
	}
}

func (h *Header) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("header.Serialize", time.Since(t0))
		logger.Trace("header", "Serialize(): %s", time.Since(t0))
	}()

	if serialized, err := msgpack.Marshal(h); err != nil {
		return nil, err
	} else {
		return serialized, nil
	}
}

func (h *Header) GetIndexID() [32]byte {
	return h.SnapshotID
}

func (h *Header) GetIndexShortID() []byte {
	return h.SnapshotID[:4]
}

func (h *Header) GetRoot() [32]byte {
	return h.Root
}
