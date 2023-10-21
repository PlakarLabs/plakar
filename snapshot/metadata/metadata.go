package metadata

import (
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/profiler"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/vmihailenco/msgpack/v5"
)

type Mapping struct {
	muMapping      sync.Mutex
	Mapping        map[uint32][]uint32
	ReverseMapping map[uint32]uint32
}

type Metadata struct {
	muChecksums      sync.Mutex
	checksumID       uint32
	Checksums        map[[32]byte]uint32
	checksumsInverse map[uint32][32]byte

	muStrings      sync.Mutex
	stringID       uint32
	Strings        map[string]uint32
	stringsInverse map[uint32]string

	muMappings sync.Mutex
	Mappings   map[uint32]*Mapping
}

func New() *Metadata {
	return &Metadata{
		Checksums:        make(map[[32]byte]uint32),
		checksumsInverse: make(map[uint32][32]byte),
		Strings:          make(map[string]uint32),
		stringsInverse:   make(map[uint32]string),
		Mappings:         make(map[uint32]*Mapping),
	}
}

func NewFromBytes(serialized []byte) (*Metadata, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("md.NewFromBytes", time.Since(t0))
		logger.Trace("metadata", "NewFromBytes(...): %s", time.Since(t0))
	}()

	var md Metadata
	if err := msgpack.Unmarshal(serialized, &md); err != nil {
		return nil, err
	}

	md.stringsInverse = make(map[uint32]string)
	for value, stringID := range md.Strings {
		md.stringsInverse[stringID] = value
	}

	return &md, nil
}

func (md *Metadata) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("md.Serialize", time.Since(t0))
		logger.Trace("metadata", "Serialize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(md)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

// checksums
func (md *Metadata) addChecksum(checksum [32]byte) uint32 {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if checksumID, exists := md.Checksums[checksum]; !exists {
		md.Checksums[checksum] = md.checksumID
		md.checksumsInverse[md.checksumID] = checksum
		checksumID = md.checksumID
		md.checksumID++
		return checksumID
	} else {
		return checksumID
	}
}

func (md *Metadata) lookupChecksum(checksum [32]byte) (uint32, bool) {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if checksumID, exists := md.Checksums[checksum]; !exists {
		return checksumID, false
	} else {
		return checksumID, true
	}
}

func (md *Metadata) lookupChecksumID(checksumID uint32) ([32]byte, bool) {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if checksum, exists := md.checksumsInverse[checksumID]; !exists {
		return [32]byte{}, false
	} else {
		return checksum, true
	}
}

// strings
func (md *Metadata) addString(value string) (uint32, bool) {
	md.muStrings.Lock()
	defer md.muStrings.Unlock()

	if stringID, exists := md.Strings[value]; !exists {
		md.Strings[value] = md.stringID
		md.stringsInverse[md.stringID] = value
		stringID = md.stringID
		md.stringID++
		return stringID, true
	} else {
		return stringID, false
	}
}

func (md *Metadata) lookupString(stringID uint32) (string, bool) {
	md.muStrings.Lock()
	defer md.muStrings.Unlock()

	if value, exists := md.stringsInverse[stringID]; !exists {
		return "", false
	} else {
		return value, true
	}
}

// md
func (md *Metadata) AddMetadata(mdType string, mdKey string, value [32]byte) {
	mdTypeID, _ := md.addString(mdType)
	mdKeyID, _ := md.addString(mdKey)
	externalID := md.addChecksum(value)

	var mapping *Mapping
	md.muMappings.Lock()
	if tmp, exists := md.Mappings[mdTypeID]; !exists {
		md.Mappings[mdTypeID] = &Mapping{
			Mapping:        make(map[uint32][]uint32),
			ReverseMapping: make(map[uint32]uint32),
		}
		mapping = md.Mappings[mdTypeID]
	} else {
		mapping = tmp
	}
	md.muMappings.Unlock()

	mapping.muMapping.Lock()
	mapping.Mapping[mdKeyID] = append(mapping.Mapping[mdKeyID], externalID)
	mapping.ReverseMapping[externalID] = mdKeyID
	mapping.muMapping.Unlock()
}

func (md *Metadata) ListKeys(mdType string) []string {
	mdTypeID, _ := md.addString(mdType)

	var mapping *Mapping
	md.muMappings.Lock()
	if tmp, exists := md.Mappings[mdTypeID]; !exists {
		mapping = nil
	} else {
		mapping = tmp
	}
	md.muMappings.Unlock()

	if mapping == nil {
		return nil
	}

	ret := make([]string, 0)
	mapping.muMapping.Lock()
	for keyID := range mapping.Mapping {
		key, _ := md.lookupString(keyID)
		ret = append(ret, key)
	}
	mapping.muMapping.Unlock()

	return ret
}

func (md *Metadata) ListValues(mdType string, mdKey string) [][32]byte {
	mdTypeID, _ := md.addString(mdType)

	var mapping *Mapping
	md.muMappings.Lock()
	if tmp, exists := md.Mappings[mdTypeID]; !exists {
		mapping = nil
	} else {
		mapping = tmp
	}
	md.muMappings.Unlock()

	if mapping == nil {
		return nil
	}

	ret := make([][32]byte, 0)
	mapping.muMapping.Lock()
	for keyID := range mapping.Mapping {
		for _, valueID := range mapping.Mapping[keyID] {
			value, _ := md.lookupChecksumID(valueID)
			ret = append(ret, value)
		}
	}
	mapping.muMapping.Unlock()

	return ret
}

func (md *Metadata) LookupKeyForValue(mdType string, value [32]byte) (string, bool) {
	mdTypeID, _ := md.addString(mdType)

	var mapping *Mapping
	md.muMappings.Lock()
	if tmp, exists := md.Mappings[mdTypeID]; !exists {
		mapping = nil
	} else {
		mapping = tmp
	}
	md.muMappings.Unlock()

	if mapping == nil {
		return "", false
	}

	valueId, exists := md.lookupChecksum(value)
	if !exists {
		return "", false
	}

	mapping.muMapping.Lock()
	defer mapping.muMapping.Unlock()
	if keyID, exists := mapping.ReverseMapping[valueId]; !exists {
		return "", false
	} else {
		key, _ := md.lookupString(keyID)
		return key, true
	}
}
