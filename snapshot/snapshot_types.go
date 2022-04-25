package snapshot

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/filesystem"
	"github.com/poolpOrg/plakar/storage"
)

type Chunk struct {
	Checksum [32]byte
	Start    uint
	Length   uint
}

type Object struct {
	Checksum    [32]byte
	Chunks      [][32]byte
	ContentType string
}

// CachedObject needs to be killed
type CachedObject struct {
	Checksum    [32]byte
	Chunks      []*Chunk
	ContentType string
	Info        filesystem.Fileinfo
}

type Statistics struct {
	Duration    time.Duration
	Chunks      uint64
	Objects     uint64
	Files       uint64
	Directories uint64
	NonRegular  uint64
	Pathnames   uint64

	Kind      map[string]uint64
	Type      map[string]uint64
	Extension map[string]uint64

	PercentKind      map[string]float64
	PercentType      map[string]float64
	PercentExtension map[string]float64
}

type Metadata struct {
	Uuid         uuid.UUID
	CreationTime time.Time
	Version      string
	Hostname     string
	Username     string
	CommandLine  string
	MachineID    string
	PublicKey    string
	Size         uint64
	Checksum     []byte

	ScannedDirectories []string

	IndexSize uint64

	Statistics Statistics
}

type Index struct {
	Filesystem *filesystem.Filesystem

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

type Snapshot struct {
	repository  *storage.Repository
	transaction *storage.Transaction

	SkipDirs []string

	Metadata *Metadata
	Index    *Index
}

type SnapshotStorage struct {
	Metadata *Metadata
	Index    *Index
}
