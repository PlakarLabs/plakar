package header

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/logger"
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
	SnapshotID       [32]byte
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

	ImporterType   string
	ImporterOrigin string

	ScanSize          uint64
	ScanProcessedSize uint64

	Root       [32]byte
	Metadata   [32]byte
	Statistics [32]byte

	ScannedDirectory string

	FilesCount       uint64
	DirectoriesCount uint64

	//FileKind      map[string]uint64
	//FileType      map[string]uint64
	//FileExtension map[string]uint64

	//FilePercentKind      map[string]float64
	//FilePercentType      map[string]float64
	//FilePercentExtension map[string]float64
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

func ParseSortKeys(sortKeysStr string) ([]string, error) {
	if sortKeysStr == "" {
		return nil, nil
	}
	keys := strings.Split(sortKeysStr, ",")
	uniqueKeys := make(map[string]bool)
	validKeys := []string{}

	headerType := reflect.TypeOf(Header{})

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if uniqueKeys[key] {
			return nil, errors.New("duplicate sort key: " + key)
		}
		uniqueKeys[key] = true

		if _, found := headerType.FieldByName(key); !found {
			return nil, errors.New("invalid sort key: " + key)
		}
		validKeys = append(validKeys, key)
	}

	return validKeys, nil
}

func SortHeaders(headers []Header, sortKeys []string, reversed bool) error {
	var err error
	sort.Slice(headers, func(i, j int) bool {
		for _, key := range sortKeys {
			switch key {
			case "CreationTime":
				if !headers[i].CreationTime.Equal(headers[j].CreationTime) {
					return headers[i].CreationTime.Before(headers[j].CreationTime)
				}
			case "Hostname":
				if headers[i].Hostname != headers[j].Hostname {
					return headers[i].Hostname < headers[j].Hostname
				}
			case "FilesCount":
				if headers[i].FilesCount != headers[j].FilesCount {
					return headers[i].FilesCount < headers[j].FilesCount
				}
			case "SnapshotID":
				for k := 0; k < len(headers[i].SnapshotID); k++ {
					if headers[i].SnapshotID[k] != headers[j].SnapshotID[k] {
						return headers[i].SnapshotID[k] < headers[j].SnapshotID[k]
					}
				}
			case "Version":
				if headers[i].Version != headers[j].Version {
					return headers[i].Version < headers[j].Version
				}
			case "PublicKey":
				if headers[i].PublicKey != headers[j].PublicKey {
					return headers[i].PublicKey < headers[j].PublicKey
				}
			case "Tags":
				// Compare Tags lexicographically, element by element
				for k := 0; k < len(headers[i].Tags) && k < len(headers[j].Tags); k++ {
					if headers[i].Tags[k] != headers[j].Tags[k] {
						return headers[i].Tags[k] < headers[j].Tags[k]
					}
				}
				if len(headers[i].Tags) != len(headers[j].Tags) {
					return len(headers[i].Tags) < len(headers[j].Tags)
				}
			case "OperatingSystem":
				if headers[i].OperatingSystem != headers[j].OperatingSystem {
					return headers[i].OperatingSystem < headers[j].OperatingSystem
				}
			case "Architecture":
				if headers[i].Architecture != headers[j].Architecture {
					return headers[i].Architecture < headers[j].Architecture
				}
			case "MachineID":
				if headers[i].MachineID != headers[j].MachineID {
					return headers[i].MachineID < headers[j].MachineID
				}
			case "ProcessID":
				if headers[i].ProcessID != headers[j].ProcessID {
					return headers[i].ProcessID < headers[j].ProcessID
				}
			case "Client":
				if headers[i].Client != headers[j].Client {
					return headers[i].Client < headers[j].Client
				}
			case "CommandLine":
				if headers[i].CommandLine != headers[j].CommandLine {
					return headers[i].CommandLine < headers[j].CommandLine
				}
			case "ImporterType":
				if headers[i].ImporterType != headers[j].ImporterType {
					return headers[i].ImporterType < headers[j].ImporterType
				}
			case "ImporterOrigin":
				if headers[i].ImporterOrigin != headers[j].ImporterOrigin {
					return headers[i].ImporterOrigin < headers[j].ImporterOrigin
				}
			case "ScanSize":
				if headers[i].ScanSize != headers[j].ScanSize {
					return headers[i].ScanSize < headers[j].ScanSize
				}
			case "ScanProcessedSize":
				if headers[i].ScanProcessedSize != headers[j].ScanProcessedSize {
					return headers[i].ScanProcessedSize < headers[j].ScanProcessedSize
				}
			case "ScannedDirectory":
				if headers[i].ScannedDirectory != headers[j].ScannedDirectory {
					return headers[i].ScannedDirectory < headers[j].ScannedDirectory
				}
			case "DirectoriesCount":
				if headers[i].DirectoriesCount != headers[j].DirectoriesCount {
					return headers[i].DirectoriesCount < headers[j].DirectoriesCount
				}
			default:
				err = errors.New("invalid sort key: " + key)
				return false
			}
		}
		return false
	})

	if err == nil && reversed {
		for i, j := 0, len(headers)-1; i < j; i, j = i+1, j-1 {
			headers[i], headers[j] = headers[j], headers[i]
		}
	}
	return err
}
