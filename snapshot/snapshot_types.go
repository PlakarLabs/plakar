package snapshot

import (
	"os"
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

type SnapshotStorage struct {
	Uuid         string
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string

	Directories map[string]*FileInfo
	Files       map[string]*FileInfo
	NonRegular  map[string]*FileInfo
	Pathnames   map[string]string
	Objects     map[string]*Object
	Chunks      map[string]*Chunk

	Size uint64
}

type Snapshot struct {
	Uuid         string
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string

	Directories map[string]*FileInfo
	Files       map[string]*FileInfo
	NonRegular  map[string]*FileInfo
	Pathnames   map[string]string
	Objects     map[string]*Object
	Chunks      map[string]*Chunk

	Size uint64

	Quiet bool

	store       storage.Store
	transaction storage.Transaction
	SkipDirs    []string

	WrittenChunks  map[string]bool
	InflightChunks map[string]*Chunk

	WrittenObjects  map[string]bool
	InflightObjects map[string]*Object
}

type SnapshotSummary struct {
	Uuid         string
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string

	Directories uint64
	Files       uint64
	NonRegular  uint64
	Pathnames   uint64
	Objects     uint64
	Chunks      uint64

	Size uint64
}
