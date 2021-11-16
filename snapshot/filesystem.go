package snapshot

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/iafan/cwalk"
	"github.com/poolpOrg/plakar/logger"
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

	path string
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

func NewFilesystem() *Filesystem {
	filesystem := &Filesystem{}
	filesystem.Root = &FilesystemNode{Children: make(map[string]*FilesystemNode)}
	filesystem.Inodes = make(map[string]*Fileinfo)
	filesystem.Directories = make(map[string]*Fileinfo)
	filesystem.Files = make(map[string]*Fileinfo)
	filesystem.NonRegular = make(map[string]*Fileinfo)
	return filesystem
}

func (filesystem *Filesystem) buildTree(pathname string, fileinfo *Fileinfo) {
	pathname = filepath.Clean(pathname)
	p := filesystem.Root
	if pathname == "/" {
		p.Inode = fileinfo
		return
	}

	atoms := strings.Split(pathname, "/")[1:]
	for _, atom := range atoms {
		p.muNode.Lock()
		tmp, exists := p.Children[atom]
		p.muNode.Unlock()

		if !exists {
			p.muNode.Lock()
			p.Children[atom] = &FilesystemNode{Children: make(map[string]*FilesystemNode)}
			tmp = p.Children[atom]
			p.muNode.Unlock()
		}
		p = tmp
	}

	p.muNode.Lock()
	p.Inode = fileinfo
	p.muNode.Unlock()

	filesystem.muInodes.Lock()
	filesystem.Inodes[pathname] = fileinfo
	filesystem.muInodes.Unlock()

	if p.Inode.Mode.IsDir() {
		filesystem.muDirectories.Lock()
		filesystem.Directories[pathname] = fileinfo
		filesystem.muDirectories.Unlock()
	} else if p.Inode.Mode.IsRegular() {
		filesystem.muFiles.Lock()
		filesystem.Files[pathname] = fileinfo
		filesystem.muFiles.Unlock()
	} else {
		filesystem.muNonRegular.Lock()
		filesystem.NonRegular[pathname] = fileinfo
		filesystem.muNonRegular.Unlock()
	}
}

func (filesystem *Filesystem) Scan(directory string, skip []string) error {
	directory = filepath.Clean(directory)
	for _, scanned := range filesystem.ScannedDirectories {
		if scanned == directory {
			return nil
		}
	}
	filesystem.muScannedDirectories.Lock()
	filesystem.ScannedDirectories = append(filesystem.ScannedDirectories, directory)
	filesystem.muScannedDirectories.Unlock()

	atoms := strings.Split(directory, "/")
	for i := len(atoms) - 1; i != 0; i-- {
		path := filepath.Clean(fmt.Sprintf("/%s", strings.Join(atoms[0:i], "/")))
		f, err := os.Stat(path)
		if err != nil {
			return err
		}
		fi := FileinfoFromStat(f)
		filesystem.buildTree(path, &fi)
	}

	err := cwalk.Walk(directory, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			logger.Warn("%s", err)
			return nil
		}

		for _, skipPath := range skip {
			if strings.HasPrefix(fmt.Sprintf("%s/%s", directory, path), skipPath) {
				return nil
			}
		}

		fileinfo := FileinfoFromStat(f)
		filesystem.buildTree(fmt.Sprintf("%s/%s", directory, path), &fileinfo)
		return nil
	})
	if err != nil {
		logger.Warn("%s", err)
	}
	return err
}

func (filesystem *Filesystem) Lookup(pathname string) (*FilesystemNode, error) {
	p := filesystem.Root
	if pathname == "/" {
		return p, nil
	}

	atoms := strings.Split(pathname, "/")[1:]
	for _, atom := range atoms {
		p.muNode.Lock()
		tmp, exists := p.Children[atom]
		p.muNode.Unlock()

		if !exists {
			return nil, fs.ErrNotExist
		}
		p = tmp
	}
	return p, nil
}
