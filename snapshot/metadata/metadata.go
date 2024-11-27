package metadata

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/PlakarKorp/plakar/logger"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION string = "0.0.1"

type Metadata struct {
	muChecksums   sync.Mutex
	checksumsMap  map[[32]byte]uint32
	ChecksumsList [][32]byte

	muStrings   sync.Mutex
	stringsMap  map[string]uint32
	StringsList []string

	muItems sync.Mutex

	itemsMap        map[uint32][]uint32
	itemsReverseMap map[uint32]uint32
	ItemsList       [][]uint32
}

func New() *Metadata {
	return &Metadata{
		checksumsMap:  make(map[[32]byte]uint32),
		ChecksumsList: make([][32]byte, 0),

		stringsMap:  make(map[string]uint32),
		StringsList: make([]string, 0),

		itemsMap:        make(map[uint32][]uint32),
		itemsReverseMap: make(map[uint32]uint32),
	}
}

func NewFromBytes(serialized []byte) (*Metadata, error) {
	t0 := time.Now()
	defer func() {
		logger.Trace("metadata", "NewFromBytes(...): %s", time.Since(t0))
	}()

	var md Metadata
	if err := msgpack.Unmarshal(serialized, &md); err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		md.checksumsMap = make(map[[32]byte]uint32)
		for checksumID, checksum := range md.ChecksumsList {
			md.checksumsMap[checksum] = uint32(checksumID)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		md.stringsMap = make(map[string]uint32)
		for stringID, str := range md.StringsList {
			md.stringsMap[str] = uint32(stringID)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		md.itemsMap = make(map[uint32][]uint32)
		md.itemsReverseMap = make(map[uint32]uint32)
		for itemID, values := range md.ItemsList {
			md.itemsMap[uint32(itemID)] = values
			for _, valueID := range values {
				md.itemsReverseMap[valueID] = uint32(itemID)
			}
		}
	}()

	wg.Wait()

	return &md, nil
}

func (md *Metadata) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Trace("metadata", "Serialize(): %s", time.Since(t0))
	}()

	newMd := &Metadata{
		checksumsMap:  make(map[[32]byte]uint32),
		ChecksumsList: make([][32]byte, len(md.ChecksumsList)),

		stringsMap:  make(map[string]uint32),
		StringsList: make([]string, len(md.StringsList)),

		itemsReverseMap: make(map[uint32]uint32),
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		copy(newMd.ChecksumsList, md.ChecksumsList)
		sort.Slice(newMd.ChecksumsList, func(i, j int) bool {
			return bytes.Compare(newMd.ChecksumsList[i][:], newMd.ChecksumsList[j][:]) < 0
		})
		for offset, checksum := range newMd.ChecksumsList {
			newMd.checksumsMap[checksum] = uint32(offset)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		copy(newMd.StringsList, md.StringsList)
		sort.Slice(newMd.StringsList, func(i, j int) bool {
			return newMd.StringsList[i] < newMd.StringsList[j]
		})
		for offset, stringID := range newMd.StringsList {
			newMd.stringsMap[stringID] = uint32(offset)
		}
	}()
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		newMd.ItemsList = make([][]uint32, len(md.itemsMap))
		for itemID, values := range md.itemsMap {
			newValues := make([]uint32, len(values))
			copy(newValues, values)
			for offset, valueID := range newValues {
				newValues[offset] = newMd.checksumsMap[md.ChecksumsList[valueID]]
			}
			sort.Slice(newValues, func(i, j int) bool {
				return newValues[i] < newValues[j]
			})
			newItemID := newMd.stringsMap[md.StringsList[itemID]]
			newMd.ItemsList[newItemID] = newValues
		}
	}()
	wg.Wait()

	serialized, err := msgpack.Marshal(newMd)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

// checksums
func (md *Metadata) addChecksum(checksum [32]byte) uint32 {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if checksumID, exists := md.checksumsMap[checksum]; !exists {
		checksumID = uint32(len(md.ChecksumsList))
		md.ChecksumsList = append(md.ChecksumsList, checksum)
		md.checksumsMap[checksum] = checksumID
		return checksumID
	} else {
		return checksumID
	}
}

func (md *Metadata) lookupChecksum(checksum [32]byte) (uint32, bool) {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if checksumID, exists := md.checksumsMap[checksum]; !exists {
		return checksumID, false
	} else {
		return checksumID, true
	}
}

func (md *Metadata) lookupChecksumID(checksumID uint32) ([32]byte, bool) {
	md.muChecksums.Lock()
	defer md.muChecksums.Unlock()

	if int(checksumID) >= len(md.ChecksumsList) {
		return [32]byte{}, false
	}

	return md.ChecksumsList[int(checksumID)], true
}

// strings
func (md *Metadata) addString(value string) (uint32, bool) {
	md.muStrings.Lock()
	defer md.muStrings.Unlock()

	if checksumID, exists := md.stringsMap[value]; !exists {
		checksumID = uint32(len(md.stringsMap))
		md.StringsList = append(md.StringsList, value)
		md.stringsMap[value] = checksumID
		return checksumID, true
	} else {
		return checksumID, false
	}
}

func (md *Metadata) lookupString(stringID uint32) (string, bool) {
	md.muStrings.Lock()
	defer md.muStrings.Unlock()

	if int(stringID) >= len(md.StringsList) {
		return "", false
	}
	return md.StringsList[stringID], true
}

func (md *Metadata) lookupStringID(str string) (uint32, bool) {
	md.muStrings.Lock()
	defer md.muStrings.Unlock()

	value, exists := md.stringsMap[str]
	return value, exists
}

// md
func (md *Metadata) AddMetadata(key string, value [32]byte) {
	md.muItems.Lock()
	defer md.muItems.Unlock()

	keyID, _ := md.addString(key)
	valueID := md.addChecksum(value)

	if _, exists := md.itemsMap[keyID]; !exists {
		md.itemsMap[keyID] = make([]uint32, 0)
	}
	md.itemsMap[keyID] = append(md.itemsMap[keyID], valueID)
	md.itemsReverseMap[valueID] = keyID
}

func (md *Metadata) ListKeys() []string {
	md.muItems.Lock()
	defer md.muItems.Unlock()

	ret := make([]string, 0)
	for keyID, _ := range md.itemsMap {
		key, _ := md.lookupString(keyID)
		ret = append(ret, key)
	}
	return ret
}

func (md *Metadata) ListValues(mdKey string) [][32]byte {
	md.muItems.Lock()
	defer md.muItems.Unlock()

	keyID, _ := md.lookupStringID(mdKey)

	ret := make([][32]byte, 0)
	for _, valueID := range md.itemsMap[keyID] {
		value, _ := md.lookupChecksumID(valueID)
		ret = append(ret, value)
	}
	return ret
}

func (md *Metadata) LookupKeyForValue(value [32]byte) (string, bool) {
	md.muItems.Lock()
	defer md.muItems.Unlock()

	valueId, exists := md.lookupChecksum(value)
	if !exists {
		return "", false
	}

	if keyID, exists := md.itemsReverseMap[valueId]; !exists {
		return "", false
	} else {
		key, _ := md.lookupString(keyID)
		return key, true
	}
}
