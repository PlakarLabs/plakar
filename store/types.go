package store

import (
	"io/fs"
	"os"
	"time"
)

type Store interface {
	Init()
	Transaction() Transaction
	Snapshot(id string) (*Snapshot, error)
	Snapshots() map[string]fs.FileInfo

	IndexGet(id string) ([]byte, error)
	ObjectGet(checksum string) ([]byte, error)
	ChunkGet(checksum string) ([]byte, error)

	Purge(id string) error
}

type Transaction interface {
	Snapshot() *Snapshot

	ObjectMark(key string) bool
	ObjectPut(checksum string, buf string) error

	ChunksMark(keys []string) map[string]bool
	ChunksPut(map[string]string) error

	IndexPut(buf string) error
	Commit(snapshot *Snapshot) (*Snapshot, error)
}

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
	Checksum string
	Chunks   []*Chunk

	fp   *os.File
	path string
}

type Snapshot struct {
	Uuid         string
	CreationTime time.Time
	Version      string

	Directories map[string]*FileInfo
	Files       map[string]*FileInfo
	NonRegular  map[string]*FileInfo
	Sums        map[string]string
	Objects     map[string]*Object
	Chunks      map[string]*Chunk

	Size     uint64
	RealSize uint64

	store       Store
	transaction Transaction
	skipDirs    []string
}

type SnapshotSummary struct {
	Uuid         string
	CreationTime time.Time
	Version      string

	Directories uint64
	Files       uint64
	NonRegular  uint64
	Sums        uint64
	Objects     uint64
	Chunks      uint64

	Size     uint64
	RealSize uint64
}
