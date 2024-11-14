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

		// Check for duplicate keys
		if uniqueKeys[key] {
			return nil, errors.New("duplicate sort key: " + key)
		}
		uniqueKeys[key] = true

		// Check if key exists in Header struct
		if _, found := headerType.FieldByName(key); !found {
			return nil, errors.New("invalid sort key: " + key)
		}
		validKeys = append(validKeys, key)
	}

	return validKeys, nil
}

func SortHeaders(headers []Header, sortKeys []string) error {
	var err error
	sort.Slice(headers, func(i, j int) bool {
		for _, key := range sortKeys {
			fieldI := reflect.ValueOf(headers[i]).FieldByName(key)
			fieldJ := reflect.ValueOf(headers[j]).FieldByName(key)

			if !fieldI.IsValid() || !fieldJ.IsValid() {
				err = errors.New("invalid sort key: " + key)
				return false
			}

			switch fieldI.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if fieldI.Int() != fieldJ.Int() {
					return fieldI.Int() < fieldJ.Int()
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if fieldI.Uint() != fieldJ.Uint() {
					return fieldI.Uint() < fieldJ.Uint()
				}
			case reflect.Float32, reflect.Float64:
				if fieldI.Float() != fieldJ.Float() {
					return fieldI.Float() < fieldJ.Float()
				}
			case reflect.String:
				if fieldI.String() != fieldJ.String() {
					return fieldI.String() < fieldJ.String()
				}
			case reflect.Array:
				if fieldI.Type() == reflect.TypeOf([32]byte{}) {
					byteI := fieldI.Interface().([32]byte)
					byteJ := fieldJ.Interface().([32]byte)
					for k := 0; k < 32; k++ {
						if byteI[k] != byteJ[k] {
							return byteI[k] < byteJ[k]
						}
					}
					continue
				}
			case reflect.Struct:
				if fieldI.Type() == reflect.TypeOf(headers[i].CreationTime) {
					if fieldI.Interface().(time.Time).Equal(fieldJ.Interface().(time.Time)) {
						continue
					}
					return fieldI.Interface().(time.Time).Before(fieldJ.Interface().(time.Time))
				}
			default:
				err = errors.New("unsupported sort key type: " + fieldI.Kind().String())
				return false
			}
		}
		return false
	})
	return err
}
