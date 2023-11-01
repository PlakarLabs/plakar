package index

import (
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/PlakarLabs/plakar/logger"
)

const VERSION string = "0.0.1"

type Index struct {
	muPathnameToObject           sync.Mutex
	PathnamesChecksumsMap        map[[32]byte]uint32
	inversePathnamesChecksumsMap map[uint32][32]byte
	PathnameToObject             map[uint32]uint32

	// Object checksum -> chunks checksums
	muObjects                  sync.Mutex
	ObjectsChecksumsMap        map[[32]byte]uint32
	inverseObjectsChecksumsMap map[uint32][32]byte
	Objects                    map[uint32][]uint32

	// Chunk checksum -> length
	muChunks                  sync.Mutex
	ChunksChecksumsMap        map[[32]byte]uint32
	inverseChunksChecksumsMap map[uint32][32]byte
	Chunks                    map[uint32]uint32
}

func NewIndex() *Index {
	return &Index{
		PathnamesChecksumsMap:        make(map[[32]byte]uint32),
		inversePathnamesChecksumsMap: make(map[uint32][32]byte),
		PathnameToObject:             make(map[uint32]uint32),

		ObjectsChecksumsMap:        make(map[[32]byte]uint32),
		inverseObjectsChecksumsMap: make(map[uint32][32]byte),
		Objects:                    make(map[uint32][]uint32),

		ChunksChecksumsMap:        make(map[[32]byte]uint32),
		inverseChunksChecksumsMap: make(map[uint32][32]byte),
		Chunks:                    make(map[uint32]uint32),
	}
}

func NewIndexFromBytes(serialized []byte) (*Index, error) {

	var index Index
	if err := msgpack.Unmarshal(serialized, &index); err != nil {
		return nil, err
	}

	index.inverseChunksChecksumsMap = make(map[uint32][32]byte)
	for checksum, checksumID := range index.ChunksChecksumsMap {
		index.inverseChunksChecksumsMap[checksumID] = checksum
	}

	index.inverseObjectsChecksumsMap = make(map[uint32][32]byte)
	for checksum, checksumID := range index.ObjectsChecksumsMap {
		index.inverseObjectsChecksumsMap[checksumID] = checksum
	}

	index.inversePathnamesChecksumsMap = make(map[uint32][32]byte)
	for checksum, checksumID := range index.PathnamesChecksumsMap {
		index.inversePathnamesChecksumsMap[checksumID] = checksum
	}

	return &index, nil
}

func (index *Index) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("index.normalize", time.Since(t0))
		logger.Trace("index", "normalize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(index)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (index *Index) ListObjects() [][32]byte {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	ret := make([][32]byte, 0)
	for checksumID := range index.Objects {
		checksum, exists := index.inverseObjectsChecksumsMap[checksumID]
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
		checksum, exists := index.inverseChunksChecksumsMap[checksumID]
		if !exists {
			panic("ListChunks: corrupted index")
		}
		ret = append(ret, checksum)
	}
	return ret
}

// Public
func (index *Index) AddChunk(chunk *objects.Chunk) {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()
	logger.Trace("index", "AddChunk(%064x)", chunk.Checksum)

	if _, exists := index.ChunksChecksumsMap[chunk.Checksum]; !exists {
		checksumID := uint32(len(index.ChunksChecksumsMap))
		index.ChunksChecksumsMap[chunk.Checksum] = checksumID
		index.inverseChunksChecksumsMap[checksumID] = chunk.Checksum
		index.Chunks[checksumID] = chunk.Length
	}
}

func (index *Index) AddObject(object *objects.Object) {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()
	logger.Trace("index", "AddObject(%064x)", object.Checksum)

	if _, exists := index.ObjectsChecksumsMap[object.Checksum]; !exists {
		checksumID := uint32(len(index.ObjectsChecksumsMap))
		index.ObjectsChecksumsMap[object.Checksum] = checksumID
		index.inverseObjectsChecksumsMap[checksumID] = object.Checksum
		index.Objects[checksumID] = make([]uint32, len(object.Chunks))
		for offset, checksum := range object.Chunks {
			index.muChunks.Lock()
			chunkID := index.ChunksChecksumsMap[checksum]
			index.muChunks.Unlock()
			index.Objects[checksumID][offset] = chunkID
		}
	}
}

func (index *Index) LinkPathnameToObject(pathnameChecksum [32]byte, object *objects.Object) {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

	index.muObjects.Lock()
	objectID, exists := index.ObjectsChecksumsMap[object.Checksum]
	index.muObjects.Unlock()
	if !exists {
		panic("LinkPathnameToObject: corrupted index: could not find object checksum")
	}

	if _, exists := index.PathnamesChecksumsMap[pathnameChecksum]; !exists {
		pathnameID := uint32(len(index.PathnamesChecksumsMap))
		index.PathnamesChecksumsMap[pathnameChecksum] = pathnameID
		index.inversePathnamesChecksumsMap[pathnameID] = pathnameChecksum
		index.PathnameToObject[pathnameID] = objectID
	}
}

func (index *Index) LookupChunk(checksum [32]byte) *objects.Chunk {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	checksumID, exists := index.ChunksChecksumsMap[checksum]
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

	checksumID, exists := index.ObjectsChecksumsMap[checksum]
	if !exists {
		return nil
	}

	objectChunks, exists := index.Objects[checksumID]
	if !exists {
		return nil
	}

	chunks := make([][32]byte, 0)
	for _, checksumID := range objectChunks {
		index.muChunks.Lock()
		checksum, exists := index.inverseChunksChecksumsMap[checksumID]
		index.muChunks.Unlock()
		if !exists {
			panic("LookupObject: corrupted index: could not find chunk checksum")
		}
		chunks = append(chunks, checksum)
	}

	return &objects.Object{
		Checksum: checksum,
		Chunks:   chunks,
	}
}

func (index *Index) ObjectExists(checksum [32]byte) bool {
	return index.LookupObject(checksum) != nil
}

func (index *Index) LookupObjectForPathnameChecksum(pathnameChecksum [32]byte) *objects.Object {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

	pathnameID, exists := index.PathnamesChecksumsMap[pathnameChecksum]
	if !exists {
		return nil
	}

	checksumID, exists := index.PathnameToObject[pathnameID]
	if !exists {
		return nil
	}

	index.muObjects.Lock()
	objectChecksum, exists := index.inverseObjectsChecksumsMap[checksumID]
	index.muObjects.Unlock()
	if !exists {
		panic("LookupObjectForPathname: corrupted index: could not find object checksum")
	}

	obj := index.LookupObject(objectChecksum)
	return obj
}

func (index *Index) GetChunkLength(checksum [32]byte) (uint32, bool) {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	checksumID, exists := index.ChunksChecksumsMap[checksum]
	if !exists {
		return 0, false
	}

	length, exists := index.Chunks[checksumID]
	return length, exists
}
