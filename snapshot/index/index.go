package index

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/PlakarLabs/plakar/logger"
)

const VERSION string = "0.0.1"

type Index struct {
	muPathnameToObject    sync.Mutex
	pathnamesChecksumsMap map[[32]byte]uint32
	//inversePathnamesChecksumsMap map[uint32][32]byte
	pathnameToObjectMap   map[uint32]uint32
	PathnameToObjectList  []uint32
	PathnamesChecksumList [][32]byte

	// Object checksum -> chunks checksums
	muObjects           sync.Mutex
	objectsChecksumsMap map[[32]byte]uint32
	//inverseObjectsChecksumsMap map[uint32][32]byte
	objectsMap          map[uint32][]uint32
	ObjectsList         [][]uint32
	ObjectsChecksumList [][32]byte

	// Chunk checksum -> length
	muChunks           sync.Mutex
	chunksChecksumsMap map[[32]byte]uint32
	//inverseChunksChecksumsMap map[uint32][32]byte
	chunksMap          map[uint32]uint32
	ChunksList         []uint32
	ChunksChecksumList [][32]byte
}

func NewIndex() *Index {
	return &Index{
		pathnamesChecksumsMap: make(map[[32]byte]uint32),
		//inversePathnamesChecksumsMap: make(map[uint32][32]byte),
		pathnameToObjectMap:   make(map[uint32]uint32),
		PathnamesChecksumList: make([][32]byte, 0),

		objectsChecksumsMap: make(map[[32]byte]uint32),
		//inverseObjectsChecksumsMap: make(map[uint32][32]byte),
		objectsMap:          make(map[uint32][]uint32),
		ObjectsChecksumList: make([][32]byte, 0),

		chunksChecksumsMap: make(map[[32]byte]uint32),
		//inverseChunksChecksumsMap: make(map[uint32][32]byte),
		chunksMap:          make(map[uint32]uint32),
		ChunksChecksumList: make([][32]byte, 0),
	}
}

func NewIndexFromBytes(serialized []byte) (*Index, error) {

	var index Index
	if err := msgpack.Unmarshal(serialized, &index); err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		index.chunksChecksumsMap = make(map[[32]byte]uint32)
		//index.inverseChunksChecksumsMap = make(map[uint32][32]byte)
		for offset, checksum := range index.ChunksChecksumList {
			index.chunksChecksumsMap[checksum] = uint32(offset)
			//	index.inverseChunksChecksumsMap[uint32(offset)] = checksum
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		index.objectsChecksumsMap = make(map[[32]byte]uint32)
		//index.inverseObjectsChecksumsMap = make(map[uint32][32]byte)
		for offset, checksum := range index.ObjectsChecksumList {
			index.objectsChecksumsMap[checksum] = uint32(offset)
			//	index.inverseObjectsChecksumsMap[uint32(offset)] = checksum
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		index.pathnamesChecksumsMap = make(map[[32]byte]uint32)
		//index.inversePathnamesChecksumsMap = make(map[uint32][32]byte)
		for offset, checksum := range index.PathnamesChecksumList {
			index.pathnamesChecksumsMap[checksum] = uint32(offset)
			//	index.inversePathnamesChecksumsMap[uint32(offset)] = checksum
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		index.chunksMap = make(map[uint32]uint32)
		for checksumID, chunkLength := range index.ChunksList {
			index.chunksMap[uint32(checksumID)] = chunkLength
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		index.objectsMap = make(map[uint32][]uint32)
		for checksumID, chunkIDs := range index.ObjectsList {
			index.objectsMap[uint32(checksumID)] = chunkIDs
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		index.pathnameToObjectMap = make(map[uint32]uint32)
		for pathnameID, objectID := range index.PathnameToObjectList {
			index.pathnameToObjectMap[uint32(pathnameID)] = objectID
		}
	}()

	wg.Wait()

	return &index, nil
}

func (index *Index) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("index.normalize", time.Since(t0))
		logger.Trace("index", "normalize(): %s", time.Since(t0))
	}()

	newIndex := &Index{
		ChunksChecksumList: make([][32]byte, len(index.ChunksChecksumList)),
		chunksChecksumsMap: make(map[[32]byte]uint32),

		ObjectsChecksumList: make([][32]byte, len(index.ObjectsChecksumList)),
		objectsChecksumsMap: make(map[[32]byte]uint32),

		PathnamesChecksumList: make([][32]byte, len(index.PathnamesChecksumList)),
		pathnamesChecksumsMap: make(map[[32]byte]uint32),
	}

	//newChunksChecksumsList := make([][32]byte, len(index.chunksChecksumsMap))
	//newChunkIDMap := make(map[[32]byte]uint32)
	//newObjectsChecksumsList := make([][32]byte, len(index.objectsChecksumsMap))
	//newObjectsIDMap := make(map[[32]byte]uint32)
	//newPathnamesChecksumsList := make([][32]byte, len(index.pathnamesChecksumsMap))
	//newPathnamesIDMap := make(map[[32]byte]uint32)

	wg := sync.WaitGroup{}
	// first of all, sort the checksums
	wg.Add(1)
	go func() {
		defer wg.Done()
		copy(newIndex.ChunksChecksumList, index.ChunksChecksumList)
		sort.Slice(newIndex.ChunksChecksumList, func(i, j int) bool {
			return bytes.Compare(newIndex.ChunksChecksumList[i][:], newIndex.ChunksChecksumList[j][:]) < 0
		})
		for offset, checksum := range newIndex.ChunksChecksumList {
			newIndex.chunksChecksumsMap[checksum] = uint32(offset)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		copy(newIndex.ObjectsChecksumList, index.ObjectsChecksumList)
		sort.Slice(newIndex.ObjectsChecksumList, func(i, j int) bool {
			return bytes.Compare(newIndex.ObjectsChecksumList[i][:], newIndex.ObjectsChecksumList[j][:]) < 0
		})
		for offset, checksum := range newIndex.ObjectsChecksumList {
			newIndex.objectsChecksumsMap[checksum] = uint32(offset)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		copy(newIndex.PathnamesChecksumList, index.PathnamesChecksumList)
		sort.Slice(newIndex.PathnamesChecksumList, func(i, j int) bool {
			return bytes.Compare(newIndex.PathnamesChecksumList[i][:], newIndex.PathnamesChecksumList[j][:]) < 0
		})
		for offset, checksum := range newIndex.PathnamesChecksumList {
			newIndex.pathnamesChecksumsMap[checksum] = uint32(offset)
		}
	}()
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		newIndex.ChunksList = make([]uint32, len(index.chunksMap))
		for checksumID, chunkLength := range index.chunksMap {
			newChecksumID := newIndex.chunksChecksumsMap[index.ChunksChecksumList[checksumID]]
			newIndex.ChunksList[newChecksumID] = chunkLength
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		newIndex.ObjectsList = make([][]uint32, len(index.objectsMap))
		for checksumID, chunkIDs := range index.objectsMap {
			newChecksumID := newIndex.objectsChecksumsMap[index.ObjectsChecksumList[checksumID]]
			newIndex.ObjectsList[newChecksumID] = make([]uint32, len(chunkIDs))
			for offset, chunkID := range chunkIDs {
				newChunkID := newIndex.chunksChecksumsMap[index.ChunksChecksumList[chunkID]]
				newIndex.ObjectsList[newChecksumID][offset] = newChunkID
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		newIndex.PathnameToObjectList = make([]uint32, len(index.pathnameToObjectMap))
		for pathnameID, objectID := range index.pathnameToObjectMap {
			newChecksumID := newIndex.pathnamesChecksumsMap[index.PathnamesChecksumList[pathnameID]]
			newObjectID := newIndex.objectsChecksumsMap[index.ObjectsChecksumList[objectID]]
			newIndex.PathnameToObjectList[newChecksumID] = newObjectID
		}
	}()

	wg.Wait()

	serialized, err := msgpack.Marshal(newIndex)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (index *Index) ListObjects() [][32]byte {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	ret := make([][32]byte, 0)
	for checksumID := range index.objectsMap {
		/*
			checksum, exists := index.inverseObjectsChecksumsMap[checksumID]
			if !exists {
				panic("ListObjects: corrupted index")
			}
		*/
		checksum := index.ObjectsChecksumList[checksumID]
		ret = append(ret, checksum)
	}
	return ret
}

func (index *Index) ListChunks() [][32]byte {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	ret := make([][32]byte, 0)
	for checksumID := range index.chunksMap {
		/*
			checksum, exists := index.inverseChunksChecksumsMap[checksumID]
			if !exists {
				panic("ListChunks: corrupted index")
			}
		*/
		checksum := index.ChunksChecksumList[checksumID]
		ret = append(ret, checksum)
	}
	return ret
}

// Public
func (index *Index) AddChunk(chunk *objects.Chunk) {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()
	logger.Trace("index", "AddChunk(%064x)", chunk.Checksum)

	if _, exists := index.chunksChecksumsMap[chunk.Checksum]; !exists {
		checksumID := uint32(len(index.chunksChecksumsMap))
		index.ChunksChecksumList = append(index.ChunksChecksumList, chunk.Checksum)
		index.chunksChecksumsMap[chunk.Checksum] = checksumID
		//index.inverseChunksChecksumsMap[checksumID] = chunk.Checksum
		index.chunksMap[checksumID] = chunk.Length
	}
}

func (index *Index) AddObject(object *objects.Object) {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()
	logger.Trace("index", "AddObject(%064x)", object.Checksum)

	if _, exists := index.objectsChecksumsMap[object.Checksum]; !exists {
		checksumID := uint32(len(index.objectsChecksumsMap))
		index.ObjectsChecksumList = append(index.ObjectsChecksumList, object.Checksum)
		index.objectsChecksumsMap[object.Checksum] = checksumID
		//index.inverseObjectsChecksumsMap[checksumID] = object.Checksum
		index.objectsMap[checksumID] = make([]uint32, len(object.Chunks))
		for offset, checksum := range object.Chunks {
			index.muChunks.Lock()
			chunkID := index.chunksChecksumsMap[checksum]
			index.muChunks.Unlock()
			index.objectsMap[checksumID][offset] = chunkID
		}
	}
}

func (index *Index) LinkPathnameToObject(pathnameChecksum [32]byte, object *objects.Object) {
	index.muPathnameToObject.Lock()
	defer index.muPathnameToObject.Unlock()

	index.muObjects.Lock()
	objectID, exists := index.objectsChecksumsMap[object.Checksum]
	index.muObjects.Unlock()
	if !exists {
		panic("LinkPathnameToObject: corrupted index: could not find object checksum")
	}

	if _, exists := index.pathnamesChecksumsMap[pathnameChecksum]; !exists {
		pathnameID := uint32(len(index.pathnamesChecksumsMap))
		index.PathnamesChecksumList = append(index.PathnamesChecksumList, pathnameChecksum)
		index.pathnamesChecksumsMap[pathnameChecksum] = pathnameID
		//index.inversePathnamesChecksumsMap[pathnameID] = pathnameChecksum
		index.pathnameToObjectMap[pathnameID] = objectID
	}
}

func (index *Index) LookupChunk(checksum [32]byte) *objects.Chunk {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	checksumID, exists := index.chunksChecksumsMap[checksum]
	if !exists {
		return nil
	}

	if chunkLength, ok := index.chunksMap[checksumID]; !ok {
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

	checksumID, exists := index.objectsChecksumsMap[checksum]
	if !exists {
		return nil
	}

	objectChunks, exists := index.objectsMap[checksumID]
	if !exists {
		return nil
	}

	chunks := make([][32]byte, 0)
	for _, checksumID := range objectChunks {
		/*
			index.muChunks.Lock()
			checksum, exists := index.inverseChunksChecksumsMap[checksumID]
			index.muChunks.Unlock()
			if !exists {
				panic("LookupObject: corrupted index: could not find chunk checksum")
			}
		*/
		index.muChunks.Lock()
		checksum := index.ChunksChecksumList[checksumID]
		index.muChunks.Unlock()

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

	pathnameID, exists := index.pathnamesChecksumsMap[pathnameChecksum]
	if !exists {
		return nil
	}

	checksumID, exists := index.pathnameToObjectMap[pathnameID]
	if !exists {
		return nil
	}

	/*
		index.muObjects.Lock()
		objectChecksum, exists := index.inverseObjectsChecksumsMap[checksumID]
		index.muObjects.Unlock()
		if !exists {
			panic("LookupObjectForPathname: corrupted index: could not find object checksum")
		}
	*/

	index.muObjects.Lock()
	objectChecksum := index.ObjectsChecksumList[checksumID]
	index.muObjects.Unlock()

	obj := index.LookupObject(objectChecksum)
	return obj
}

func (index *Index) GetChunkLength(checksum [32]byte) (uint32, bool) {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	checksumID, exists := index.chunksChecksumsMap[checksum]
	if !exists {
		return 0, false
	}

	length, exists := index.chunksMap[checksumID]
	return length, exists
}
