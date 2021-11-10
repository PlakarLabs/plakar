package snapshot

import (
	"os"
	"sync"
	"time"

	"github.com/poolpOrg/plakar/storage"
)

type FileInfo struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime time.Time
	Dev     uint64
	Ino     uint64
	Uid     uint64
	Gid     uint64

	path string
}

type Chunk struct {
	Checksum string
	Start    uint
	Length   uint
}

type Object struct {
	Checksum    string
	Chunks      []*Chunk
	ContentType string

	fp   *os.File
	path string
}

type CachedObject struct {
	Checksum    string
	Chunks      []*Chunk
	ContentType string
	Info        FileInfo
}

type TreeNode struct {
	muNode   sync.Mutex
	Inode    *FileInfo
	Children map[string]*TreeNode
}

type SnapshotStorage struct {
	Uuid         string
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string
	CommandLine  string

	Roots []string

	Tree *TreeNode

	Directories []string
	Files       []string
	NonRegular  []string

	Pathnames map[string]string
	Objects   map[string]*Object
	Chunks    map[string]*Chunk

	// reverse lookups
	ChunkToObjects       map[string][]string
	ObjectToPathnames    map[string][]string
	ContentTypeToObjects map[string][]string

	Size uint64
}

type Snapshot struct {
	Uuid         string
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string
	CommandLine  string

	muRoots sync.Mutex
	Roots   []string

	Tree *TreeNode

	muDirectories sync.Mutex
	Directories   map[string]bool

	muFiles sync.Mutex
	Files   map[string]bool

	muNonRegular sync.Mutex
	NonRegular   map[string]bool

	muPathnames sync.Mutex
	Pathnames   map[string]string

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

	Size uint64

	store       *storage.Store
	transaction *storage.Transaction
	SkipDirs    []string

	muWrittenChunks sync.Mutex
	WrittenChunks   map[string]bool

	muInflightChunks sync.Mutex
	InflightChunks   map[string]*Chunk

	muWrittenObjects sync.Mutex
	WrittenObjects   map[string]bool

	muInflightObjects sync.Mutex
	InflightObjects   map[string]*Object
}
