package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
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

func FileinfoFromStat(stat os.FileInfo) Fileinfo {
	return Fileinfo{
		Name:    stat.Name(),
		Size:    stat.Size(),
		Mode:    stat.Mode(),
		ModTime: stat.ModTime(),
		Dev:     uint64(stat.Sys().(*syscall.Stat_t).Dev),
		Ino:     uint64(stat.Sys().(*syscall.Stat_t).Ino),
		Uid:     uint64(stat.Sys().(*syscall.Stat_t).Uid),
		Gid:     uint64(stat.Sys().(*syscall.Stat_t).Gid),
	}
}

func (fileinfo *Fileinfo) HumanSize() string {
	return humanize.Bytes(uint64(fileinfo.Size))
}

func NewFilesystem() *Filesystem {
	filesystem := &Filesystem{}
	filesystem.Root = &FilesystemNode{Children: make(map[string]*FilesystemNode)}
	filesystem.Names = make(map[string][]string)
	filesystem.Stat = make([]string, 0)
	filesystem.Lstat = make([]string, 0)
	filesystem.statInfo = make(map[string]*Fileinfo)
	filesystem.lstatInfo = make(map[string]*Fileinfo)
	filesystem.Directories = make([]string, 0)
	filesystem.directoriesInfo = make(map[string]*Fileinfo)
	filesystem.Files = make([]string, 0)
	filesystem.filesInfo = make(map[string]*Fileinfo)
	filesystem.NonRegular = make([]string, 0)
	filesystem.nonRegularInfo = make(map[string]*Fileinfo)
	filesystem.Symlinks = make(map[string]string)
	return filesystem
}

func (filesystem *Filesystem) buildTree(pathname string, fileinfo *Fileinfo) {
	pathname = filepath.Clean(pathname)

	p := filesystem.Root
	if pathname != "/" {
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
	}
	p.muNode.Lock()
	p.Inode = fileinfo
	p.muNode.Unlock()

	filesystem.muNames.Lock()
	filesystem.Names[p.Inode.Name] = append(filesystem.Names[p.Inode.Name], pathname)
	filesystem.muNames.Unlock()

	filesystem.muStat.Lock()
	filesystem.Stat = append(filesystem.Stat, pathname)
	filesystem.statInfo[pathname] = fileinfo
	filesystem.muStat.Unlock()

	if p.Inode.Mode.IsDir() {
		filesystem.muDirectories.Lock()
		filesystem.Directories = append(filesystem.Directories, pathname)
		filesystem.directoriesInfo[pathname] = fileinfo
		filesystem.muDirectories.Unlock()
	} else if p.Inode.Mode.IsRegular() {
		filesystem.muFiles.Lock()
		filesystem.Files = append(filesystem.Files, pathname)
		filesystem.filesInfo[pathname] = fileinfo
		filesystem.muFiles.Unlock()
	} else {
		filesystem.muNonRegular.Lock()
		filesystem.NonRegular = append(filesystem.NonRegular, pathname)
		filesystem.nonRegularInfo[pathname] = fileinfo
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

		pathname := fmt.Sprintf("%s/%s", directory, path)

		fileinfo := FileinfoFromStat(f)
		filesystem.buildTree(pathname, &fileinfo)

		if !fileinfo.Mode.IsDir() && !fileinfo.Mode.IsRegular() {
			lstat, err := os.Lstat(pathname)
			if err != nil {
				logger.Warn("%s", err)
				return nil
			}

			lfileinfo := FileinfoFromStat(lstat)
			if lfileinfo.Mode&os.ModeSymlink != 0 {
				originFile, err := os.Readlink(lfileinfo.Name)
				if err != nil {
					logger.Warn("%s", err)
					return nil
				}

				filesystem.muLstat.Lock()
				filesystem.Lstat = append(filesystem.Lstat, pathname)
				filesystem.lstatInfo[pathname] = &lfileinfo
				filesystem.muLstat.Unlock()

				filesystem.muSymlinks.Lock()
				filesystem.Symlinks[pathname] = originFile
				filesystem.muSymlinks.Unlock()
			}
		}

		return nil
	})
	if err != nil {
		logger.Warn("%s", err)
	}
	return err
}

func (filesystem *Filesystem) Lookup(pathname string) (*FilesystemNode, error) {
	pathname = filepath.Clean(pathname)

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
			return nil, os.ErrNotExist
		}
		p = tmp
	}
	return p, nil
}

func (filesystem *Filesystem) LookupInode(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	filesystem.muStat.Lock()
	fileinfo, exists := filesystem.statInfo[pathname]
	filesystem.muStat.Unlock()
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupInodeForFile(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	filesystem.muFiles.Lock()
	fileinfo, exists := filesystem.filesInfo[pathname]
	filesystem.muFiles.Unlock()
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupInodeForDirectory(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	filesystem.muDirectories.Lock()
	fileinfo, exists := filesystem.directoriesInfo[pathname]
	filesystem.muDirectories.Unlock()
	return fileinfo, exists
}

func (filesystem *Filesystem) ListFiles() []string {
	list := make([]string, 0)
	filesystem.muFiles.Lock()
	list = append(list, filesystem.Files...)
	filesystem.muFiles.Unlock()
	return list
}

func (filesystem *Filesystem) ListDirectories() []string {
	list := make([]string, 0)
	filesystem.muDirectories.Lock()
	list = append(list, filesystem.Directories...)
	filesystem.muDirectories.Unlock()
	return list
}

func (filesystem *Filesystem) ListNonRegular() []string {
	list := make([]string, 0)
	filesystem.muNonRegular.Lock()
	list = append(list, filesystem.NonRegular...)
	filesystem.muNonRegular.Unlock()
	return list
}

func (filesystem *Filesystem) ListStat() []string {
	list := make([]string, 0)
	filesystem.muStat.Lock()
	list = append(list, filesystem.Stat...)
	filesystem.muStat.Unlock()
	return list
}

func (filesystem *Filesystem) ListLstat() []string {
	list := make([]string, 0)
	filesystem.muLstat.Lock()
	list = append(list, filesystem.Lstat...)
	filesystem.muLstat.Unlock()
	return list
}

func (filesystem *Filesystem) Reindex() {
	filesystem.filesInfo = make(map[string]*Fileinfo)
	filesystem.directoriesInfo = make(map[string]*Fileinfo)
	filesystem.nonRegularInfo = make(map[string]*Fileinfo)
	filesystem.statInfo = make(map[string]*Fileinfo)
	filesystem.lstatInfo = make(map[string]*Fileinfo)

	for _, stat := range filesystem.ListStat() {
		node, _ := filesystem.Lookup(stat)
		filesystem.statInfo[stat] = node.Inode
		if node.Inode.Mode.IsDir() {
			filesystem.directoriesInfo[stat] = node.Inode
		} else if node.Inode.Mode.IsRegular() {
			filesystem.filesInfo[stat] = node.Inode
		} else {
			filesystem.nonRegularInfo[stat] = node.Inode
		}
	}

	for _, lstat := range filesystem.ListLstat() {
		node, _ := filesystem.Lookup(lstat)
		filesystem.lstatInfo[lstat] = node.Inode
	}
}
