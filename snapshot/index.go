package snapshot

import (
	"path/filepath"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

type Index struct {
	Filesystem *Filesystem

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
		Filesystem: NewFilesystem(),

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

func (index *Index) LookupObjectForChecksum(checksum [32]byte) *Object {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	object, exists := index.Objects[checksum]
	if !exists {
		return nil
	}

	return object
}

func (index *Index) LookupObjectForPathname(pathname string) *Object {
	index.muPathnames.Lock()
	defer index.muPathnames.Unlock()

	objectChecksum, exists := index.Pathnames[filepath.Clean(pathname)]
	if !exists {
		return nil
	}

	return index.LookupObjectForChecksum(objectChecksum)
}

func (index *Index) LookupInodeForPathname(pathname string) (*Fileinfo, bool) {
	return index.Filesystem.LookupInode(pathname)
}

func (index *Index) LookupInodeForFilename(pathname string) (*Fileinfo, bool) {
	return index.Filesystem.LookupInodeForFile(pathname)
}

func (index *Index) LookupInodeForDirectory(pathname string) (*Fileinfo, bool) {
	return index.Filesystem.LookupInodeForDirectory(pathname)
}

func (index *Index) LookupPathChildren(pathname string) (map[string]*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)

	parent, err := index.Filesystem.Lookup(pathname)
	if err != nil {
		return nil, false
	}

	ret := make(map[string]*Fileinfo)
	for child, node := range parent.Children {
		ret[child] = node.Inode
	}
	return ret, true
}

func (index *Index) LinkChunkToObject(chunkChecksum [32]byte, objectChecksum [32]byte) {
	index.muChunkToObjects.Lock()
	defer index.muChunkToObjects.Unlock()

	if _, exists := index.ChunkToObjects[chunkChecksum]; !exists {
		index.ChunkToObjects[chunkChecksum] = make([][32]byte, 0)
	}

	for _, value := range index.ChunkToObjects[chunkChecksum] {
		if value == objectChecksum {
			return
		}
	}
	index.ChunkToObjects[chunkChecksum] = append(index.ChunkToObjects[chunkChecksum], objectChecksum)
}

func (index *Index) AddPathnameToObject(pathname string, object *Object) {
	index.muPathnames.Lock()
	defer index.muPathnames.Unlock()

	index.Pathnames[pathname] = object.Checksum
}

func (index *Index) AddObject(object *Object) {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	index.Objects[object.Checksum] = object
}

func (index *Index) AddChunk(chunk *Chunk) {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	index.Chunks[chunk.Checksum] = chunk
}

func (index *Index) AddObjectToPathnames(object *Object, pathname string) {
	index.muObjectToPathnames.Lock()
	defer index.muObjectToPathnames.Unlock()

	index.ObjectToPathnames[object.Checksum] = append(index.ObjectToPathnames[object.Checksum], pathname)
}

func (index *Index) AddContentTypeToObjects(object *Object) {
	index.muContentTypeToObjects.Lock()
	defer index.muContentTypeToObjects.Unlock()

	index.ContentTypeToObjects[object.ContentType] = append(index.ContentTypeToObjects[object.ContentType], object.Checksum)
}

func (index *Index) LookupObject(checksum [32]byte) *Object {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	if object, ok := index.Objects[checksum]; !ok {
		return nil
	} else {
		return object
	}
}

func (index *Index) ListObjects() [][32]byte {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	ret := make([][32]byte, 0)
	for checksum := range index.Objects {
		ret = append(ret, checksum)
	}
	return ret
}

func (index *Index) LookupChunk(checksum [32]byte) *Chunk {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	if chunk, ok := index.Chunks[checksum]; !ok {
		return nil
	} else {
		return chunk
	}
}

func (index *Index) ListChunks() [][32]byte {
	index.muChunks.Lock()
	defer index.muChunks.Unlock()

	ret := make([][32]byte, 0)
	for checksum := range index.Chunks {
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
	index.muContentTypeToObjects.Lock()
	defer index.muContentTypeToObjects.Unlock()

	ret := make([]string, 0)
	for contentType := range index.ContentTypeToObjects {
		ret = append(ret, contentType)
	}
	return ret
}

func (index *Index) GetContentType(contentType string) [][32]byte {
	index.muContentTypeToObjects.Lock()
	defer index.muContentTypeToObjects.Unlock()

	if objectsChecksums, ok := index.ContentTypeToObjects[contentType]; !ok {
		return nil
	} else {
		return objectsChecksums
	}
}

func (index *Index) GetObjects() map[[32]byte]*Object {
	index.muObjects.Lock()
	defer index.muObjects.Unlock()

	return index.Objects
}
