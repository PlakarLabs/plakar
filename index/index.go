package index

import (
	"path/filepath"
	"sync"

	"github.com/poolpOrg/plakar/objects"

	"github.com/poolpOrg/plakar/logger"
	"github.com/vmihailenco/msgpack/v5"
)

type IndexObject struct {
	Chunks      []uint64
	ContentType uint64
}

type IndexChunk struct {
	Start  uint
	Length uint
}

type Index struct {
	muChecksums      sync.Mutex
	Checksums        map[[32]byte]uint64
	checksumsInverse map[uint64][32]byte

	muPathnames      sync.Mutex
	Pathnames        map[string]uint64
	pathnamesInverse map[uint64]string

	muContentType       sync.Mutex
	ContentTypes        map[string]uint64
	contentTypesInverse map[uint64]string

	muPathnameToObject sync.Mutex
	PathnameToObject   map[uint64]uint64
	ObjectToPathnames  map[uint64][]uint64

	// Object checksum -> Object
	muObjects sync.Mutex
	Objects   map[uint64]IndexObject

	// Chunk checksum -> Chunk
	muChunks       sync.Mutex
	Chunks         map[uint64]IndexChunk
	ChunkToObjects map[uint64][]uint64

	// Content Type -> Object checksums
	muContentTypeToObjects sync.Mutex
	ContentTypeToObjects   map[uint64][]uint64
}

func NewIndex() *Index {
	return &Index{
		Checksums:        make(map[[32]byte]uint64),
		checksumsInverse: make(map[uint64][32]byte),

		Pathnames:        make(map[string]uint64),
		pathnamesInverse: make(map[uint64]string),

		ContentTypes:        make(map[string]uint64),
		contentTypesInverse: make(map[uint64]string),

		PathnameToObject:  make(map[uint64]uint64),
		ObjectToPathnames: make(map[uint64][]uint64),

		Objects:        make(map[uint64]IndexObject),
		Chunks:         make(map[uint64]IndexChunk),
		ChunkToObjects: make(map[uint64][]uint64),

		ContentTypeToObjects: make(map[uint64][]uint64),
	}
}

func NewIndexFromBytes(serialized []byte) (*Index, error) {
	var index Index
	if err := msgpack.Unmarshal(serialized, &index); err != nil {
		return nil, err
	}

	index.checksumsInverse = make(map[uint64][32]byte)
	for checksum, checksumID := range index.Checksums {
		index.checksumsInverse[checksumID] = checksum
	}

	index.pathnamesInverse = make(map[uint64]string)
	for pathname, pathnameID := range index.Pathnames {
		index.pathnamesInverse[pathnameID] = pathname
	}

	index.contentTypesInverse = make(map[uint64]string)
	for contentType, contentTypeID := range index.ContentTypes {
		index.contentTypesInverse[contentTypeID] = contentType
	}

	return &index, nil
}

func (index *Index) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(index)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

// checksums
func (index *Index) addChecksum(checksum [32]byte) {
	index.muChecksums.Lock()
	defer index.muChecksums.Unlock()

	if _, exists := index.Checksums[checksum]; !exists {
		checksumID := uint64(len(index.Checksums))
		index.Checksums[checksum] = checksumID
		index.checksumsInverse[checksumID] = checksum
	}
}

func (index *Index) ChecksumToId(checksum [32]byte) (uint64, bool) {
	index.muChecksums.Lock()
	defer index.muChecksums.Unlock()

	checksumID, exists := index.Checksums[checksum]
	return checksumID, exists
}

func (index *Index) IdToChecksum(checksumID uint64) ([32]byte, bool) {
	index.muChecksums.Lock()
	defer index.muChecksums.Unlock()

	checksum, exists := index.checksumsInverse[checksumID]
	return checksum, exists
}

// pathnames
func (index *Index) addPathname(pathname string) {
	index.muPathnames.Lock()
	defer index.muPathnames.Unlock()

	if _, exists := index.Pathnames[pathname]; !exists {
		pathnameID := uint64(len(index.Pathnames))
		index.Pathnames[pathname] = pathnameID
		index.pathnamesInverse[pathnameID] = pathname
	}
}

func (index *Index) getPathnameID(pathname string) (uint64, bool) {
	index.muPathnames.Lock()
	defer index.muPathnames.Unlock()

	pathnameID, exists := index.Pathnames[pathname]
	return pathnameID, exists
}

func (index *Index) getPathname(pathnameID uint64) (string, bool) {
	index.muPathnames.Lock()
	defer index.muPathnames.Unlock()

	pathname, exists := index.pathnamesInverse[pathnameID]
	return pathname, exists
}

// content types
func (index *Index) addContentType(contentType string) {
	index.muContentType.Lock()
	defer index.muContentType.Unlock()

	if _, exists := index.ContentTypes[contentType]; !exists {
		contentTypeID := uint64(len(index.ContentTypes))
		index.ContentTypes[contentType] = contentTypeID
		index.contentTypesInverse[contentTypeID] = contentType
	}
}

func (index *Index) getContentTypeID(contentType string) (uint64, bool) {
	index.muContentType.Lock()
	defer index.muContentType.Unlock()

	contentTypeID, exists := index.ContentTypes[contentType]

	return contentTypeID, exists
}

func (index *Index) getContentType(contentTypeID uint64) (string, bool) {
	index.muContentType.Lock()
	defer index.muContentType.Unlock()

	contentType, exists := index.contentTypesInverse[contentTypeID]
	return contentType, exists
}

func (index *Index) linkObjectToContentType(checksumID uint64, contentTypeID uint64) {
	index.muContentTypeToObjects.Lock()
	defer index.muContentTypeToObjects.Unlock()

	if _, exists := index.ContentTypeToObjects[contentTypeID]; !exists {
		index.ContentTypeToObjects[contentTypeID] = make([]uint64, 0)
	}
	index.ContentTypeToObjects[contentTypeID] = append(index.ContentTypeToObjects[contentTypeID], checksumID)
}

func (index *Index) linkChunkToObject(chunkChecksumID uint64, objectChecksumID uint64) {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	if _, exists := index.ChunkToObjects[chunkChecksumID]; !exists {
		index.ChunkToObjects[chunkChecksumID] = make([]uint64, 0)
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

func (index *Index) ListPathnames() []string {
	index.muPathnames.Lock()
	defer index.muPathnames.Unlock()

	ret := make([]string, 0)
	for pathname := range index.Pathnames {
		ret = append(ret, pathname)
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

	chunks := make([]uint64, 0)
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

func (index *Index) LinkPathnameToObject(pathname string, object *objects.Object) {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

	pathname = filepath.Clean(pathname)
	index.addPathname(pathname)

	pathnameID, exists := index.getPathnameID(pathname)
	if !exists {
		panic("LinkPathnameToObject: corrupted index: could not find pathname")
	}

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

func (index *Index) LookupObjectForPathname(pathname string) *objects.Object {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

	// should implement an "exists" version ?
	pathnameID, exists := index.getPathnameID(filepath.Clean(pathname))
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
