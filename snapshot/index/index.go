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

	var index Index
	if err := msgpack.Unmarshal(serialized, &index); err != nil {
		return nil, err
	}
	index.checksumsInverse = make(map[uint32][32]byte)
	for checksum, checksumID := range index.Checksums {
		index.checksumsInverse[checksumID] = checksum
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
	chunks := make([]uint32, 0)
	for _, checksum := range object.Chunks {
		chunkChecksumID, exists := index.ChecksumToId(checksum)
		if !exists {
			panic("AddObject: corrupted index: could not find chunk checksum")
		}
		chunks = append(chunks, chunkChecksumID)
	}

	index.Objects[objectChecksumID] = chunks

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
