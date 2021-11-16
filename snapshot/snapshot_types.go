package snapshot

import (
	"sync"
	"time"

	"github.com/poolpOrg/plakar/filesystem"
	"github.com/poolpOrg/plakar/storage"
)

type Chunk struct {
	Checksum string
	Start    uint
	Length   uint
}

type Object struct {
	Checksum    string
	Chunks      []*Chunk
	ContentType string
}

type CachedObject struct {
	Checksum    string
	Chunks      []*Chunk
	ContentType string
	Info        filesystem.Fileinfo
}

type TreeNode struct {
	muNode   sync.Mutex
	Inode    *filesystem.Fileinfo
	Children map[string]*TreeNode
}

type SnapshotStorage struct {
	Uuid         string
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string
	CommandLine  string

	Size uint64

	Filesystem *filesystem.Filesystem

	//	Tree *TreeNode

	Filenames map[string]string
	Objects   map[string]*Object
	Chunks    map[string]*Chunk

	// reverse lookups
	ChunkToObjects       map[string][]string
	ObjectToPathnames    map[string][]string
	ContentTypeToObjects map[string][]string
}

type Snapshot struct {
	store       *storage.Store
	transaction *storage.Transaction

	SkipDirs []string

	Uuid         string
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string
	CommandLine  string

	Size uint64

	Filesystem *filesystem.Filesystem

	//	Tree *TreeNode

	muFilenames sync.Mutex
	Filenames   map[string]string

	muObjects sync.Mutex
	Objects   map[string]*Object

	muChunks sync.Mutex
	Chunks   map[string]*Chunk

	muChunkToObjects sync.Mutex
	ChunkToObjects   map[string][]string

	muObjectToPathnames sync.Mutex
	ObjectToPathnames   map[string][]string

	muContentTypeToObjects sync.Mutex
	ContentTypeToObjects   map[string][]string
}
