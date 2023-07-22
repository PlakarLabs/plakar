package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/iafan/cwalk"
	"github.com/poolpOrg/plakar/logger"
	"github.com/vmihailenco/msgpack/v5"
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
	Inode    string
	Children map[string]*FilesystemNode
}

type Filesystem struct {
	Root *FilesystemNode

	muInodes sync.Mutex
	Inodes   map[string]Fileinfo

	muPathnames      sync.Mutex
	Pathnames        map[string]uint64
	pathnamesInverse map[uint64]string

	muScannedDirectories sync.Mutex
	scannedDirectories   []string

	muStat   sync.Mutex
	statInfo map[string]*Fileinfo

	muSymlinks sync.Mutex
	Symlinks   map[string]string

	totalSize uint64
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
	filesystem.Inodes = make(map[string]Fileinfo)
	filesystem.Pathnames = make(map[string]uint64)
	filesystem.pathnamesInverse = make(map[uint64]string)
	filesystem.Root = &FilesystemNode{Children: make(map[string]*FilesystemNode)}
	filesystem.statInfo = make(map[string]*Fileinfo)
	filesystem.Symlinks = make(map[string]string)
	filesystem.totalSize = 0
	return filesystem
}

func (filesystem *Filesystem) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(filesystem)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func NewFilesystemFromBytes(serialized []byte) (*Filesystem, error) {
	var filesystem Filesystem
	if err := msgpack.Unmarshal(serialized, &filesystem); err != nil {
		return nil, err
	}
	filesystem.reindex()
	return &filesystem, nil
}

func (filesystem *Filesystem) buildTree(pathname string, fileinfo *Fileinfo) {
	inodeKey := filesystem.addInode(*fileinfo)

	pathname = filepath.Clean(pathname)
	filesystem.addPathname(pathname)

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
	p.Inode = inodeKey
	p.muNode.Unlock()

	filesystem.muStat.Lock()
	filesystem.statInfo[pathname] = fileinfo
	filesystem.muStat.Unlock()
}

func (filesystem *Filesystem) Scan(c chan<- int64, directory string, skip []string) error {
	directory = filepath.Clean(directory)
	for _, scanned := range filesystem.scannedDirectories {
		if scanned == directory {
			return nil
		}
	}
	filesystem.muScannedDirectories.Lock()
	filesystem.scannedDirectories = append(filesystem.scannedDirectories, directory)
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

				filesystem.muStat.Lock()
				filesystem.statInfo[pathname] = &lfileinfo
				filesystem.muStat.Unlock()

				filesystem.muSymlinks.Lock()
				filesystem.Symlinks[pathname] = originFile
				filesystem.muSymlinks.Unlock()
			}
		}
		c <- 1
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
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	pathname = filepath.Clean(pathname)
	fileinfo, exists := filesystem.statInfo[pathname]
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupInodeForFile(pathname string) (*Fileinfo, bool) {
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	pathname = filepath.Clean(pathname)
	fileinfo, exists := filesystem.statInfo[pathname]
	if !exists || !fileinfo.Mode.IsRegular() {
		return nil, false
	}
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupInodeForDirectory(pathname string) (*Fileinfo, bool) {
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	pathname = filepath.Clean(pathname)
	fileinfo, exists := filesystem.statInfo[pathname]
	if !exists || !fileinfo.Mode.IsDir() {
		return nil, false
	}
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupChildren(pathname string) ([]string, error) {
	pathname = filepath.Clean(pathname)
	parent, err := filesystem.Lookup(pathname)
	if err != nil {
		return nil, os.ErrNotExist
	}

	filesystem.muInodes.Lock()
	parentInode := filesystem.Inodes[parent.Inode]
	filesystem.muInodes.Unlock()

	if !parentInode.Mode.IsDir() {
		return nil, os.ErrInvalid
	}

	ret := make([]string, 0)
	for child := range parent.Children {
		ret = append(ret, child)
	}

	sort.Strings(ret)

	return ret, nil

}

func (filesystem *Filesystem) ListFiles() []string {
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	list := make([]string, 0)
	for pathname, stat := range filesystem.statInfo {
		if stat.Mode.IsRegular() {
			list = append(list, pathname)
		}
	}
	return list
}

func (filesystem *Filesystem) ListDirectories() []string {
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	list := make([]string, 0)
	for pathname, stat := range filesystem.statInfo {
		if stat.Mode.IsDir() {
			list = append(list, pathname)
		}
	}
	return list
}

func (filesystem *Filesystem) ListNonRegular() []string {
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	list := make([]string, 0)
	for pathname, stat := range filesystem.statInfo {
		if !stat.Mode.IsDir() && !stat.Mode.IsRegular() {
			list = append(list, pathname)
		}
	}
	return list
}

func (filesystem *Filesystem) ListStat() []string {
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	list := make([]string, 0)
	for pathname, _ := range filesystem.statInfo {
		list = append(list, pathname)
	}
	return list
}

func (filesystem *Filesystem) _reindex(pathname string) {
	node, err := filesystem.Lookup(pathname)
	if err != nil {
		return
	}

	pathnameInode := filesystem.Inodes[node.Inode]
	filesystem.statInfo[pathname] = &pathnameInode
	filesystem.totalSize += uint64(pathnameInode.Size)

	for name, node := range node.Children {
		nodeInode := filesystem.Inodes[node.Inode]
		child := filepath.Clean(fmt.Sprintf("%s/%s", pathname, name))
		if nodeInode.Mode.IsDir() {
			filesystem._reindex(child)
		} else {
			filesystem.statInfo[child] = &nodeInode
		}
	}
}

func (filesystem *Filesystem) reindex() {
	filesystem.muPathnames.Lock()
	filesystem.pathnamesInverse = make(map[uint64]string)
	for pathname, pathnameId := range filesystem.Pathnames {
		filesystem.pathnamesInverse[pathnameId] = pathname
	}
	filesystem.muPathnames.Unlock()

	filesystem.statInfo = make(map[string]*Fileinfo)
	filesystem._reindex("/")
}

func (filesystem *Filesystem) addInode(fileinfo Fileinfo) string {
	filesystem.muInodes.Lock()
	defer filesystem.muInodes.Unlock()

	key := fmt.Sprintf("%d,%d", fileinfo.Dev, fileinfo.Ino)
	if _, exists := filesystem.Inodes[key]; !exists {
		filesystem.Inodes[key] = fileinfo
		filesystem.totalSize += uint64(fileinfo.Size)
	}
	return key
}

func (filesystem *Filesystem) addPathname(pathname string) uint64 {
	filesystem.muPathnames.Lock()
	defer filesystem.muPathnames.Unlock()

	if pathnameId, exists := filesystem.Pathnames[pathname]; !exists {
		pathnameId := uint64(len(filesystem.Pathnames))
		filesystem.Pathnames[pathname] = pathnameId
		filesystem.pathnamesInverse[pathnameId] = pathname
		return pathnameId
	} else {
		return pathnameId
	}
}

func (filesystem *Filesystem) GetPathnameID(pathname string) uint64 {
	filesystem.muPathnames.Lock()
	defer filesystem.muPathnames.Unlock()

	return filesystem.Pathnames[pathname]
}

func (filesystem *Filesystem) GetPathname(pathnameId uint64) string {
	filesystem.muPathnames.Lock()
	defer filesystem.muPathnames.Unlock()

	return filesystem.pathnamesInverse[pathnameId]
}

func (filesystem *Filesystem) Size() uint64 {
	filesystem.muInodes.Lock()
	defer filesystem.muInodes.Unlock()

	return filesystem.totalSize
}
