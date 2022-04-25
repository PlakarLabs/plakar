package snapshot

import (
	"sync"

	"github.com/poolpOrg/plakar/filesystem"
	"github.com/vmihailenco/msgpack/v5"
)

type Index struct {
	Filesystem *filesystem.Filesystem

	muStringSymbols  sync.Mutex
	StringSymbols    map[string]uint64
	StringSymbolsRev map[uint64]string

	// Pathnames -> Object checksum
	muPathnames sync.Mutex
	Pathnames   map[string][32]byte

	// Object checksum -> Object
	muObjects sync.Mutex
	Objects   map[[32]byte]*Object

	// Chunk checksum -> Chunk
	muChunks sync.Mutex
	Chunks   map[[32]byte]*Chunk

	// Chunk checksum -> Object checksums
	muChunkToObjects sync.Mutex
	ChunkToObjects   map[[32]byte][][32]byte

	// Object checksum -> Filenames
	muObjectToPathnames sync.Mutex
	ObjectToPathnames   map[[32]byte][]string

	// Content Type -> Object checksums
	muContentTypeToObjects sync.Mutex
	ContentTypeToObjects   map[string][][32]byte
}

func NewIndex() *Index {
	return &Index{
		StringSymbols:    make(map[string]uint64),
		StringSymbolsRev: make(map[uint64]string),

		Filesystem: filesystem.NewFilesystem(),

		Pathnames: make(map[string][32]byte),
		Objects:   make(map[[32]byte]*Object),
		Chunks:    make(map[[32]byte]*Chunk),

		ChunkToObjects:       make(map[[32]byte][][32]byte),
		ObjectToPathnames:    make(map[[32]byte][]string),
		ContentTypeToObjects: make(map[string][][32]byte),
	}
}

func NewIndexFromBytes(serialized []byte) (*Index, error) {
	var index Index
	if err := msgpack.Unmarshal(serialized, &index); err != nil {
		return nil, err
	}
	index.Filesystem.Reindex()
	return &index, nil
}

func (index *Index) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(index)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (index *Index) GetChunkInfo(checksum [32]byte) (*Chunk, bool) {
	index.muChunks.Lock()
	chunk, exists := index.Chunks[checksum]
	index.muChunks.Unlock()
	return chunk, exists
}
