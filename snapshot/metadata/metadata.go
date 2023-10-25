package metadata

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
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
	buffer := bytes.NewReader(serialized)
	metadata := &Metadata{
		Checksums:        make(map[[32]byte]uint32),
		checksumsInverse: make(map[uint32][32]byte),
		Strings:          make(map[string]uint32),
		stringsInverse:   make(map[uint32]string),
		Mappings:         make(map[uint32]*Mapping),
	}

	// Read Checksums
	var nChecksums uint32
	if err := binary.Read(buffer, binary.LittleEndian, &nChecksums); err != nil {
		return nil, err
	}
	for i := uint32(0); i < nChecksums; i++ {
		var checksum [32]byte
		var id uint32
		if err := binary.Read(buffer, binary.LittleEndian, &checksum); err != nil {
			return nil, err
		}
		if err := binary.Read(buffer, binary.LittleEndian, &id); err != nil {
			return nil, err
		}
		metadata.Checksums[checksum] = id
		metadata.checksumsInverse[id] = checksum
	}

	// Read Strings
	var nStrings uint32
	if err := binary.Read(buffer, binary.LittleEndian, &nStrings); err != nil {
		return nil, err
	}

	for i := uint32(0); i < nStrings; i++ {
		var strSize uint32
		if err := binary.Read(buffer, binary.LittleEndian, &strSize); err != nil {
			return nil, err
		}

		strBytes := make([]byte, strSize)
		if err := binary.Read(buffer, binary.LittleEndian, &strBytes); err != nil {
			return nil, err
		}
		str := string(strBytes)
		var id uint32
		if err := binary.Read(buffer, binary.LittleEndian, &id); err != nil {
			return nil, err
		}
		metadata.Strings[str] = id
		metadata.stringsInverse[id] = str
	}

	// Read Mappings
	var nMappings uint32
	if err := binary.Read(buffer, binary.LittleEndian, &nMappings); err != nil {
		return nil, err
	}
	for i := uint32(0); i < nMappings; i++ {
		var key uint32
		if err := binary.Read(buffer, binary.LittleEndian, &key); err != nil {
			return nil, err
		}
		mapping := &Mapping{
			Mapping:        make(map[uint32][]uint32),
			ReverseMapping: make(map[uint32]uint32),
		}

		var nSubMappings uint32
		if err := binary.Read(buffer, binary.LittleEndian, &nSubMappings); err != nil {
			return nil, err
		}
		for j := uint32(0); j < nSubMappings; j++ {
			var subKey uint32
			if err := binary.Read(buffer, binary.LittleEndian, &subKey); err != nil {
				return nil, err
			}

			var nnSubMappings uint32
			if err := binary.Read(buffer, binary.LittleEndian, &nnSubMappings); err != nil {
				return nil, err
			}

			var subsubMappingList []uint32
			for j := uint32(0); j < nnSubMappings; j++ {
				var subsubMappingEntry uint32
				if err := binary.Read(buffer, binary.LittleEndian, &subsubMappingEntry); err != nil {
					return nil, err
				}
				subsubMappingList = append(subsubMappingList, subsubMappingEntry)
			}
			mapping.Mapping[subKey] = subsubMappingList
			for _, v := range subsubMappingList {
				mapping.ReverseMapping[v] = subKey
			}
		}
		metadata.Mappings[key] = mapping
	}

	return metadata, nil
}

func (metadata *Metadata) Serialize() ([]byte, error) {

	// Sort and write Checksums
	checksumsList := make([][32]byte, 0, len(metadata.Checksums))
	for checksum := range metadata.Checksums {
		i := sort.Search(len(checksumsList), func(i int) bool {
			return bytes.Compare(checksumsList[i][:], checksum[:]) >= 0
		})
		checksumsList = append(checksumsList, [32]byte{})
		copy(checksumsList[i+1:], checksumsList[i:])
		checksumsList[i] = checksum
	}

	newChecksums := make(map[[32]byte]uint32)
	newChecksumsInverse := make(map[uint32][32]byte)
	for idx, checksum := range checksumsList {
		newChecksums[checksum] = uint32(idx)
		newChecksumsInverse[uint32(idx)] = checksum
	}

	stringsList := make([]string, 0)
	for str := range metadata.Strings {
		i := sort.Search(len(stringsList), func(i int) bool {
			return stringsList[i] > str
		})
		stringsList = append(stringsList, "")
		copy(stringsList[i+1:], stringsList[i:])
		stringsList[i] = str
	}

	newStrings := make(map[string]uint32)
	newStringsInverse := make(map[uint32]string)
	for strID, str := range stringsList {
		newStrings[str] = uint32(strID)
		newStringsInverse[uint32(strID)] = str
	}

	mappingsList := make([]uint32, 0)
	for mdTypeID := range metadata.Mappings {
		i := sort.Search(len(mappingsList), func(i int) bool {
			return mappingsList[i] > mdTypeID
		})
		mappingsList = append(mappingsList, 0)
		copy(mappingsList[i+1:], mappingsList[i:])
		mappingsList[i] = mdTypeID
	}

	newMappings := make(map[uint32]*Mapping)
	for _, mdTypeID := range mappingsList {
		mapping := metadata.Mappings[mdTypeID]
		newMapping := &Mapping{
			Mapping:        make(map[uint32][]uint32),
			ReverseMapping: make(map[uint32]uint32),
		}
		for keyID, values := range mapping.Mapping {
			newKeyID := newStrings[metadata.stringsInverse[keyID]]
			newMapping.Mapping[newKeyID] = make([]uint32, 0)
			for _, valueID := range values {
				newValueID := newChecksums[metadata.checksumsInverse[valueID]]
				newMapping.Mapping[newKeyID] = append(newMapping.Mapping[newKeyID], newValueID)
				newMapping.ReverseMapping[newValueID] = newKeyID
			}
		}
		newMappings[newStrings[metadata.stringsInverse[mdTypeID]]] = newMapping
	}

	metadata.Mappings = newMappings
	metadata.Strings = newStrings
	metadata.stringsInverse = newStringsInverse
	metadata.Checksums = newChecksums
	metadata.checksumsInverse = newChecksumsInverse

	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, uint32(len(checksumsList))); err != nil {
		return nil, err
	}
	for _, checksum := range checksumsList {
		if err := binary.Write(&buffer, binary.LittleEndian, checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, metadata.Checksums[checksum]); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(&buffer, binary.LittleEndian, uint32(len(stringsList))); err != nil {
		return nil, err
	}
	for _, key := range stringsList {
		strSize := uint32(len(key))
		if err := binary.Write(&buffer, binary.LittleEndian, strSize); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, []byte(key)); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, metadata.Strings[key]); err != nil {
			return nil, err
		}
	}

	// Sort and write Mappings
	if err := binary.Write(&buffer, binary.LittleEndian, uint32(len(mappingsList))); err != nil {
		return nil, err
	}
	for _, key := range mappingsList {
		mapping := newMappings[key]
		if err := binary.Write(&buffer, binary.LittleEndian, key); err != nil {
			return nil, err
		}
		subMappingList := make([]uint32, 0, len(mapping.Mapping))
		for k := range mapping.Mapping {
			subMappingList = append(subMappingList, k)
		}
		sort.Slice(subMappingList, func(i, j int) bool {
			return subMappingList[i] < subMappingList[j]
		})
		if err := binary.Write(&buffer, binary.LittleEndian, uint32(len(subMappingList))); err != nil {
			return nil, err
		}
		for _, k := range subMappingList {
			if err := binary.Write(&buffer, binary.LittleEndian, k); err != nil {
				return nil, err
			}

			if err := binary.Write(&buffer, binary.LittleEndian, uint32(len(mapping.Mapping[k]))); err != nil {
				return nil, err
			}

			subsubMappingList := append([]uint32(nil), mapping.Mapping[k]...)
			sort.Slice(subsubMappingList, func(i, j int) bool {
				return subsubMappingList[i] < subsubMappingList[j]
			})
			for j := uint32(0); j < uint32(len(subsubMappingList)); j++ {
				if err := binary.Write(&buffer, binary.LittleEndian, subsubMappingList[j]); err != nil {
					return nil, err
				}
			}
		}
	}
	return buffer.Bytes(), nil
}

/*
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
*/

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
