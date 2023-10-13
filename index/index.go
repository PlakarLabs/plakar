package index

import (
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/vmihailenco/msgpack/v5"
)

type IndexObject struct {
	Chunks      []uint32
	ContentType uint32
}

type IndexChunk struct {
	Start  uint64
	Length uint
}

type Index struct {
	muChecksums      sync.Mutex
	checksumID       uint32
	Checksums        map[[32]byte]uint32
	checksumsInverse map[uint32][32]byte

	muPathnames sync.Mutex
	Pathnames   map[uint32]uint64

	muContentType       sync.Mutex
	ContentTypes        map[string]uint32
	contentTypesInverse map[uint32]string

	muPathnameToObject sync.Mutex
	PathnameToObject   map[uint64]uint32
	ObjectToPathnames  map[uint32][]uint64

	// Object checksum -> Object
	muObjects sync.Mutex
	Objects   map[uint32]IndexObject

	// Chunk checksum -> Chunk
	muChunks       sync.Mutex
	Chunks         map[uint32]IndexChunk
	ChunkToObjects map[uint32][]uint32

	// Content Type -> Object checksums
	muContentTypeToObjects sync.Mutex
	contentTypeID          uint32
	ContentTypeToObjects   map[uint32][]uint32
}

func NewIndex() *Index {
	return &Index{
		Checksums:        make(map[[32]byte]uint32),
		checksumsInverse: make(map[uint32][32]byte),

		Pathnames: make(map[uint32]uint64),

		ContentTypes:        make(map[string]uint32),
		contentTypesInverse: make(map[uint32]string),

		PathnameToObject:  make(map[uint64]uint32),
		ObjectToPathnames: make(map[uint32][]uint64),

		Objects:        make(map[uint32]IndexObject),
		Chunks:         make(map[uint32]IndexChunk),
		ChunkToObjects: make(map[uint32][]uint32),

		ContentTypeToObjects: make(map[uint32][]uint32),
	}
}

func NewIndexFromBytes(serialized []byte) (*Index, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("index.NewIndexFromBytes", time.Since(t0))
		logger.Trace("index", "NewIndexFromBytes(...): %s", time.Since(t0))
	}()

	var index Index
	if err := msgpack.Unmarshal(serialized, &index); err != nil {
		return nil, err
	}

	index.checksumsInverse = make(map[uint32][32]byte)
	for checksum, checksumID := range index.Checksums {
		index.checksumsInverse[checksumID] = checksum
	}

	index.contentTypesInverse = make(map[uint32]string)
	for contentType, contentTypeID := range index.ContentTypes {
		index.contentTypesInverse[contentTypeID] = contentType
	}
	return &index, nil
}

func (index *Index) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("index.Serialize", time.Since(t0))
		logger.Trace("index", "Serialize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(index)
	if err != nil {
		return nil, err
	}
	return serialized, nil
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

// content types
func (index *Index) addContentType(contentType string) {
	index.muContentType.Lock()
	defer index.muContentType.Unlock()

	if _, exists := index.ContentTypes[contentType]; !exists {
		index.ContentTypes[contentType] = index.contentTypeID
		index.contentTypesInverse[index.contentTypeID] = contentType
		index.contentTypeID++
	}
}

func (index *Index) getContentTypeID(contentType string) (uint32, bool) {
	index.muContentType.Lock()
	defer index.muContentType.Unlock()

	contentTypeID, exists := index.ContentTypes[contentType]

	return contentTypeID, exists
}

func (index *Index) getContentType(contentTypeID uint32) (string, bool) {
	index.muContentType.Lock()
	defer index.muContentType.Unlock()

	contentType, exists := index.contentTypesInverse[contentTypeID]
	return contentType, exists
}

func (index *Index) linkObjectToContentType(checksumID uint32, contentTypeID uint32) {
	index.muContentTypeToObjects.Lock()
	defer index.muContentTypeToObjects.Unlock()

	if _, exists := index.ContentTypeToObjects[contentTypeID]; !exists {
		index.ContentTypeToObjects[contentTypeID] = make([]uint32, 0)
	}
	index.ContentTypeToObjects[contentTypeID] = append(index.ContentTypeToObjects[contentTypeID], checksumID)
}

func (index *Index) linkChunkToObject(chunkChecksumID uint32, objectChecksumID uint32) {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	if _, exists := index.ChunkToObjects[chunkChecksumID]; !exists {
		index.ChunkToObjects[chunkChecksumID] = make([]uint32, 0)
	}
	index.ChunkToObjects[chunkChecksumID] = append(index.ChunkToObjects[chunkChecksumID], objectChecksumID)
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

func (index *Index) ListContentTypes() []string {
	index.muContentType.Lock()
	defer index.muContentType.Unlock()

	ret := make([]string, 0)
	for key := range index.ContentTypes {
		ret = append(ret, key)
	}
	return ret
}

// Public
func (index *Index) AddChunk(chunk *objects.Chunk) {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()
	logger.Trace("index", "AddChunk(%064x)", chunk.Checksum)

	index.addChecksum(chunk.Checksum)

	checksumID, exists := index.ChecksumToId(chunk.Checksum)
	if !exists {
		panic("AddChunk: corrupted index")
	}

	index.Chunks[checksumID] = IndexChunk{
		Start:  chunk.Start,
		Length: chunk.Length,
	}
}

func (index *Index) AddObject(object *objects.Object) {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()
	logger.Trace("index", "AddObject(%064x)", object.Checksum)

	index.addChecksum(object.Checksum)
	index.addContentType(object.ContentType)

	objectChecksumID, exists := index.ChecksumToId(object.Checksum)
	if !exists {
		panic("AddObject: corrupted index: could not find object checksum")
	}

	contentTypeID, exists := index.getContentTypeID(object.ContentType)
	if !exists {
		panic("AddObject: corrupted index: could not find content type")
	}

	index.linkObjectToContentType(objectChecksumID, contentTypeID)

	chunks := make([]uint32, 0)
	for _, checksum := range object.Chunks {
		chunkChecksumID, exists := index.ChecksumToId(checksum)
		if !exists {
			panic("AddObject: corrupted index: could not find chunk checksum")
		}

		index.linkChunkToObject(chunkChecksumID, objectChecksumID)
		chunks = append(chunks, chunkChecksumID)
	}

	index.Objects[objectChecksumID] = IndexObject{
		Chunks:      chunks,
		ContentType: contentTypeID,
	}
}

func (index *Index) RecordPathnameChecksum(pathnameChecksum [32]byte, pathnameID uint64) {
	index.muPathnames.Lock()
	defer index.muPathnames.Unlock()

	checksumID := index.addChecksum(pathnameChecksum)

	if _, exists := index.Pathnames[checksumID]; !exists {
		index.Pathnames[checksumID] = pathnameID
	}

}

func (index *Index) LinkPathnameToObject(pathnameID uint64, object *objects.Object) {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

	checksumID, exists := index.ChecksumToId(object.Checksum)
	if !exists {
		panic("LinkPathnameToObject: corrupted index: could not find object checksum")
	}

	index.PathnameToObject[pathnameID] = checksumID
	if _, exists := index.ObjectToPathnames[checksumID]; !exists {
		index.ObjectToPathnames[checksumID] = make([]uint64, 0)
	}
	index.ObjectToPathnames[checksumID] = append(index.ObjectToPathnames[checksumID], pathnameID)
}

func (index *Index) LookupChunk(checksum [32]byte) *objects.Chunk {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	checksumID, exists := index.ChecksumToId(checksum)
	if !exists {
		return nil
	}

	if chunk, ok := index.Chunks[checksumID]; !ok {
		return nil
	} else {
		return &objects.Chunk{
			Checksum: checksum,
			Start:    chunk.Start,
			Length:   chunk.Length,
		}
	}
}

func (index *Index) LookupObject(checksum [32]byte) *objects.Object {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	checksumID, exists := index.ChecksumToId(checksum)
	if !exists {
		return nil
	}

	object, exists := index.Objects[checksumID]
	if !exists {
		return nil
	}

	chunks := make([][32]byte, 0)
	for _, checksumID := range object.Chunks {
		checksum, exists := index.IdToChecksum(checksumID)
		if !exists {
			panic("LookupObject: corrupted index: could not find chunk checksum")
		}
		chunks = append(chunks, checksum)
	}

	contentTypeID, exists := index.getContentType(object.ContentType)
	if !exists {
		panic("LookupObject: corrupted index: could not find content type")
	}

	return &objects.Object{
		Checksum:    checksum,
		Chunks:      chunks,
		ContentType: contentTypeID,
	}
}

func (index *Index) LookupObjectForPathname(pathnameID uint64) *objects.Object {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

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

func (index *Index) LookupObjectForPathnameHash(pathnameHash [32]byte) *objects.Object {
	checksumID, exists := index.ChecksumToId(pathnameHash)
	if !exists {
		return nil
	}

	index.muPathnames.Lock()
	pathnameID, exists := index.Pathnames[checksumID]
	index.muPathnames.Unlock()
	if !exists {
		return nil
	}
	return index.LookupObjectForPathname(pathnameID)

}

func (index *Index) LookupObjectsForContentType(contentType string) [][32]byte {
	index.muContentTypeToObjects.Lock()
	defer index.muContentTypeToObjects.Unlock()

	contentTypeID, exists := index.getContentTypeID(contentType)
	if !exists {
		panic("LookupObjectsForContentType: corrupted index: could not find content type")
	}

	if objectsChecksums, ok := index.ContentTypeToObjects[contentTypeID]; !ok {
		return nil
	} else {
		ret := make([][32]byte, 0)
		for _, symbolKey := range objectsChecksums {
			checksum, exists := index.IdToChecksum(symbolKey)
			if !exists {
				panic("LookupObjectsForContentType: corrupted index: could not find chunk")
			}

			ret = append(ret, checksum)
		}
		return ret
	}
}
