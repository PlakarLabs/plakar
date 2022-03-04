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

	muNames sync.Mutex
	Names   map[string][]string

	muStat   sync.Mutex
	Stat     []string
	statInfo map[string]*Fileinfo

	muLstat   sync.Mutex
	Lstat     []string
	lstatInfo map[string]*Fileinfo

	muDirectories   sync.Mutex
	Directories     []string
	directoriesInfo map[string]*Fileinfo

	muFiles   sync.Mutex
	Files     []string
	filesInfo map[string]*Fileinfo

	muNonRegular   sync.Mutex
	NonRegular     []string
	nonRegularInfo map[string]*Fileinfo

	muSymlinks sync.Mutex
	Symlinks   map[string]string
}
