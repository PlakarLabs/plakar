package index

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"

	"github.com/PlakarLabs/plakar/logger"
)

type Index struct {
	muChecksums      sync.Mutex
	checksumID       uint32
	Checksums        map[[32]byte]uint32
	checksumsInverse map[uint32][32]byte

	muPathnameToObject sync.Mutex
	PathnameToObject   map[uint32]uint32

	// Object checksum -> chunks checksums
	muObjects sync.Mutex
	Objects   map[uint32][]uint32

	// Chunk checksum -> length
	muChunks sync.Mutex
	Chunks   map[uint32]uint32
}

func NewIndex() *Index {
	return &Index{
		Checksums:        make(map[[32]byte]uint32),
		checksumsInverse: make(map[uint32][32]byte),

		PathnameToObject: make(map[uint32]uint32),

		Objects: make(map[uint32][]uint32),

		Chunks: make(map[uint32]uint32),
	}
}

func NewIndexFromBytes(serialized []byte) (*Index, error) {
	buffer := bytes.NewReader(serialized)
	index := &Index{
		Checksums:        make(map[[32]byte]uint32),
		checksumsInverse: make(map[uint32][32]byte),
		PathnameToObject: make(map[uint32]uint32),
		Objects:          make(map[uint32][]uint32),
		Chunks:           make(map[uint32]uint32),
	}

	// Read checksumsList
	var nChecksums uint32
	if err := binary.Read(buffer, binary.LittleEndian, &nChecksums); err != nil {
		return nil, err
	}
	for i := uint32(0); i < nChecksums; i++ {
		var checksum [32]byte
		if err := binary.Read(buffer, binary.LittleEndian, &checksum); err != nil {
			return nil, err
		}
		index.Checksums[checksum] = i
		index.checksumsInverse[i] = checksum
	}

	// Read pathnameToObject
	var nPathnameToObject uint32
	if err := binary.Read(buffer, binary.LittleEndian, &nPathnameToObject); err != nil {
		return nil, err
	}
	for i := uint32(0); i < nPathnameToObject; i++ {
		var pathnameID, objectID uint32
		if err := binary.Read(buffer, binary.LittleEndian, &pathnameID); err != nil {
			return nil, err
		}
		if err := binary.Read(buffer, binary.LittleEndian, &objectID); err != nil {
			return nil, err
		}
		index.PathnameToObject[pathnameID] = objectID
	}

	// Read objects
	var nObjects uint32
	if err := binary.Read(buffer, binary.LittleEndian, &nObjects); err != nil {
		return nil, err
	}
	for i := uint32(0); i < nObjects; i++ {
		var objectID, nChunks uint32
		if err := binary.Read(buffer, binary.LittleEndian, &objectID); err != nil {
			return nil, err
		}
		if err := binary.Read(buffer, binary.LittleEndian, &nChunks); err != nil {
			return nil, err
		}
		chunksList := make([]uint32, nChunks)
		for j := uint32(0); j < nChunks; j++ {
			var chunkID uint32
			if err := binary.Read(buffer, binary.LittleEndian, &chunkID); err != nil {
				return nil, err
			}
			chunksList[j] = chunkID
		}
		index.Objects[objectID] = chunksList
	}

	// Read chunks
	var nChunks uint32
	if err := binary.Read(buffer, binary.LittleEndian, &nChunks); err != nil {
		return nil, err
	}
	for i := uint32(0); i < nChunks; i++ {
		var chunkID, chunkLength uint32
		if err := binary.Read(buffer, binary.LittleEndian, &chunkID); err != nil {
			return nil, err
		}
		if err := binary.Read(buffer, binary.LittleEndian, &chunkLength); err != nil {
			return nil, err
		}
		index.Chunks[chunkID] = chunkLength
	}

	return index, nil
}

func (index *Index) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("index.normalize", time.Since(t0))
		logger.Trace("index", "normalize(): %s", time.Since(t0))
	}()

	checksumsList := make([][32]byte, 0)
	for checksum := range index.Checksums {
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

	newChunks := make(map[uint32]uint32)
	chunksList := make([]uint32, 0)
	for chunkChecksum, chunkLength := range index.Chunks {
		newChunkID := newChecksums[index.checksumsInverse[chunkChecksum]]
		newChunks[newChunkID] = chunkLength

		i := sort.Search(len(chunksList), func(i int) bool {
			return chunksList[i] > newChunkID
		})
		chunksList = append(chunksList, 0)
		copy(chunksList[i+1:], chunksList[i:])
		chunksList[i] = newChunkID
	}

	newObjects := make(map[uint32][]uint32)
	objectsList := make([]uint32, 0)
	for objectChecksum, chunksList := range index.Objects {
		newChunksList := make([]uint32, 0)
		for _, chunkChecksum := range chunksList {
			newChunksList = append(newChunksList, newChecksums[index.checksumsInverse[chunkChecksum]])
		}
		newObjectID := newChecksums[index.checksumsInverse[objectChecksum]]
		newObjects[newObjectID] = newChunksList

		i := sort.Search(len(objectsList), func(i int) bool {
			return objectsList[i] > newObjectID
		})
		objectsList = append(objectsList, 0)
		copy(objectsList[i+1:], objectsList[i:])
		objectsList[i] = newObjectID
	}

	newPathnameToObject := make(map[uint32]uint32)
	pathnamesList := make([]uint32, 0)
	for pathnameChecksum, objectChecksum := range index.PathnameToObject {
		newPathnameID := newChecksums[index.checksumsInverse[pathnameChecksum]]
		newPathnameToObject[newPathnameID] = newChecksums[index.checksumsInverse[objectChecksum]]

		i := sort.Search(len(pathnamesList), func(i int) bool {
			return pathnamesList[i] > newPathnameID
		})
		pathnamesList = append(pathnamesList, 0)
		copy(pathnamesList[i+1:], pathnamesList[i:])
		pathnamesList[i] = newPathnameID
	}

	index.Chunks = newChunks
	index.Objects = newObjects
	index.PathnameToObject = newPathnameToObject
	index.Checksums = newChecksums
	index.checksumsInverse = newChecksumsInverse

	var buffer bytes.Buffer

	// first, write the checksumsList
	var nChecksums uint32 = uint32(len(checksumsList))
	if err := binary.Write(&buffer, binary.LittleEndian, nChecksums); err != nil {
		return nil, err
	}
	for _, checksum := range checksumsList {
		if err := binary.Write(&buffer, binary.LittleEndian, checksum); err != nil {
			return nil, err
		}
	}
	checksumsList = nil

	// then write, the pathnameToObject
	var nPathnameToObject uint32 = uint32(len(pathnamesList))
	if err := binary.Write(&buffer, binary.LittleEndian, nPathnameToObject); err != nil {
		return nil, err
	}
	for _, pathnameID := range pathnamesList {
		if err := binary.Write(&buffer, binary.LittleEndian, pathnameID); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, newPathnameToObject[pathnameID]); err != nil {
			return nil, err
		}
	}
	pathnamesList = nil

	// then write the objects
	var nObjects uint32 = uint32(len(objectsList))
	if err := binary.Write(&buffer, binary.LittleEndian, nObjects); err != nil {
		return nil, err
	}
	for _, objectID := range objectsList {
		if err := binary.Write(&buffer, binary.LittleEndian, objectID); err != nil {
			return nil, err
		}
		var nChunks uint32 = uint32(len(index.Objects[objectID]))
		if err := binary.Write(&buffer, binary.LittleEndian, nChunks); err != nil {
			return nil, err
		}
		for _, chunkID := range index.Objects[objectID] {
			if err := binary.Write(&buffer, binary.LittleEndian, chunkID); err != nil {
				return nil, err
			}
		}
	}
	objectsList = nil

	// then write the chunks
	var nChunks uint32 = uint32(len(chunksList))
	if err := binary.Write(&buffer, binary.LittleEndian, nChunks); err != nil {
		return nil, err
	}
	for _, chunkID := range chunksList {
		if err := binary.Write(&buffer, binary.LittleEndian, chunkID); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, index.Chunks[chunkID]); err != nil {
			return nil, err
		}
	}
	chunksList = nil

	return buffer.Bytes(), nil
}

// checksums
func (index *Index) addChecksum(checksum [32]byte) uint32 {
	index.muChecksums.Lock()
	defer index.muChecksums.Unlock()

	if checksumID, exists := index.Checksums[checksum]; !exists {
		index.Checksums[checksum] = index.checksumID
		index.checksumsInverse[index.checksumID] = checksum
		checksumID = index.checksumID
		index.checksumID++
		return checksumID
	} else {
		return checksumID
	}
}

func (index *Index) ChecksumToId(checksum [32]byte) (uint32, bool) {
	index.muChecksums.Lock()
	defer index.muChecksums.Unlock()

	checksumID, exists := index.Checksums[checksum]
	return checksumID, exists
}

func (index *Index) IdToChecksum(checksumID uint32) ([32]byte, bool) {
	index.muChecksums.Lock()
	defer index.muChecksums.Unlock()

	checksum, exists := index.checksumsInverse[checksumID]
	return checksum, exists
}

func (index *Index) ListObjects() [][32]byte {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	ret := make([][32]byte, 0)
	for checksumID := range index.Objects {
		checksum, exists := index.IdToChecksum(checksumID)
		if !exists {
			panic("ListObjects: corrupted index")
		}
		ret = append(ret, checksum)
	}
	return ret
}

func (index *Index) ListChunks() [][32]byte {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	ret := make([][32]byte, 0)
	for checksumID := range index.Chunks {
		checksum, exists := index.IdToChecksum(checksumID)
		if !exists {
			panic("ListChunks: corrupted index")
		}
		ret = append(ret, checksum)
	}
	return ret
}

// Public
func (index *Index) AddChunk(chunk *objects.Chunk) uint32 {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()
	logger.Trace("index", "AddChunk(%064x)", chunk.Checksum)

	checksumID := index.addChecksum(chunk.Checksum)
	index.Chunks[checksumID] = chunk.Length
	return checksumID
}

func (index *Index) AddObject(object *objects.Object) uint32 {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()
	logger.Trace("index", "AddObject(%064x)", object.Checksum)

	objectChecksumID := index.addChecksum(object.Checksum)

	//	index.addContentType(object.ContentType)
	//	contentTypeID, exists := index.getContentTypeID(object.ContentType)
	//	if !exists {
	//		panic("AddObject: corrupted index: could not find content type")
	//	}
	//	index.linkObjectToContentType(objectChecksumID, contentTypeID)

	chunks := make([]uint32, 0)
	for _, checksum := range object.Chunks {
		chunkChecksumID, exists := index.ChecksumToId(checksum)
		if !exists {
			panic("AddObject: corrupted index: could not find chunk checksum")
		}
		//index.linkChunkToObject(chunkChecksumID, objectChecksumID)
		chunks = append(chunks, chunkChecksumID)
	}

	index.Objects[objectChecksumID] = chunks
	//index.ObjectToContentType[objectChecksumID] = contentTypeID

	return objectChecksumID
}

func (index *Index) LinkPathnameToObject(pathnameChecksum [32]byte, object *objects.Object) {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

	checksumID, exists := index.ChecksumToId(object.Checksum)
	if !exists {
		panic("LinkPathnameToObject: corrupted index: could not find object checksum")
	}

	pathnameID := index.addChecksum(pathnameChecksum)
	index.PathnameToObject[pathnameID] = checksumID
	//	if _, exists := index.ObjectToPathnames[checksumID]; !exists {
	//		index.ObjectToPathnames[checksumID] = make([]uint32, 0)
	//	}
	//
	// index.ObjectToPathnames[checksumID] = append(index.ObjectToPathnames[checksumID], pathnameID)
}

func (index *Index) LookupChunk(checksum [32]byte) *objects.Chunk {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	checksumID, exists := index.ChecksumToId(checksum)
	if !exists {
		return nil
	}

	if chunkLength, ok := index.Chunks[checksumID]; !ok {
		return nil
	} else {
		return &objects.Chunk{
			Checksum: checksum,
			Length:   chunkLength,
		}
	}
}

func (index *Index) ChunkExists(checksum [32]byte) bool {
	return index.LookupChunk(checksum) != nil
}

func (index *Index) LookupObject(checksum [32]byte) *objects.Object {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	checksumID, exists := index.ChecksumToId(checksum)
	if !exists {
		return nil
	}

	objectChunks, exists := index.Objects[checksumID]
	if !exists {
		return nil
	}

	chunks := make([][32]byte, 0)
	for _, checksumID := range objectChunks {
		checksum, exists := index.IdToChecksum(checksumID)
		if !exists {
			panic("LookupObject: corrupted index: could not find chunk checksum")
		}
		chunks = append(chunks, checksum)
	}

	return &objects.Object{
		Checksum:    checksum,
		Chunks:      chunks,
		ContentType: "",
	}
}

func (index *Index) ObjectExists(checksum [32]byte) bool {
	return index.LookupObject(checksum) != nil
}

func (index *Index) LookupObjectForPathnameChecksum(pathnameChecksum [32]byte) *objects.Object {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

	pathnameID, exists := index.ChecksumToId(pathnameChecksum)
	if !exists {
		return nil
	}

	checksumID, exists := index.PathnameToObject[pathnameID]
	if !exists {
		return nil
	}

	checksum, exists := index.IdToChecksum(checksumID)
	if !exists {
		panic("LookupObjectForPathname: corrupted index: could not find object checksum")
	}
	return index.LookupObject(checksum)
}

func (index *Index) GetObject(checksum [32]byte) *objects.Object {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	checksumID, exists := index.ChecksumToId(checksum)
	if !exists {
		return nil
	}

	objectChunks, exists := index.Objects[checksumID]
	if !exists {
		return nil
	}

	chunks := make([][32]byte, 0)
	for _, checksumID := range objectChunks {
		checksum, exists := index.IdToChecksum(checksumID)
		if !exists {
			panic("GetObject: corrupted index: could not find chunk checksum")
		}
		chunks = append(chunks, checksum)
	}

	return &objects.Object{
		Checksum:    checksum,
		Chunks:      chunks,
		ContentType: "",
	}
}
