package filesystem

import (
	"os"
	"sync"
	"time"
)

type Fileinfo struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime time.Time
	Dev     uint64
	Ino     uint64
	Uid     uint64
	Gid     uint64
}

type FilesystemNode struct {
	muNode   sync.Mutex
	Inode    *Fileinfo
	Children map[string]*FilesystemNode
}

type Filesystem struct {
	Root *FilesystemNode

	muScannedDirectories sync.Mutex
	ScannedDirectories   []string

	muInodes sync.Mutex
	Inodes   map[string]*Fileinfo

	muDirectories sync.Mutex
	Directories   map[string]*Fileinfo

	muFiles sync.Mutex
	Files   map[string]*Fileinfo

	muNonRegular sync.Mutex
	NonRegular   map[string]*Fileinfo
}
