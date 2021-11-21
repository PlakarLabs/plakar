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

// CachedObject needs to be killed
type CachedObject struct {
	Checksum    string
	Chunks      []*Chunk
	ContentType string
	Info        filesystem.Fileinfo
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

	// Pathnames -> Object checksum
	muPathnames sync.Mutex
	Pathnames   map[string]string

	// Object checksum -> Object
	muObjects sync.Mutex
	Objects   map[string]*Object

	// Chunk checksum -> Chunk
	muChunks sync.Mutex
	Chunks   map[string]*Chunk

	// Chunk checksum -> Object checksums
	muChunkToObjects sync.Mutex
	ChunkToObjects   map[string][]string

	// Object checksum -> Filenames
	muObjectToPathnames sync.Mutex
	ObjectToPathnames   map[string][]string

	// Content Type -> Object checksums
	muContentTypeToObjects sync.Mutex
	ContentTypeToObjects   map[string][]string
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

	// Pathname -> Object checksum
	Pathnames map[string]string

	// Object checksum -> Object
	Objects map[string]*Object

	// Chunk checksum -> Chunk
	Chunks map[string]*Chunk

	// Chunk checksum -> Object checksums
	ChunkToObjects map[string][]string

	// Object checksum -> Filenames
	ObjectToPathnames map[string][]string

	// Content Type -> Object checksums
	ContentTypeToObjects map[string][]string
}
