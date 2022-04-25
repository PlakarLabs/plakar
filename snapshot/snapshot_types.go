package snapshot

import (
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
