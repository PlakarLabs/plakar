package filesystem

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/iafan/cwalk"
	"github.com/poolpOrg/plakar/logger"
)

func NewFilesystem() *Filesystem {
	filesystem := &Filesystem{}
	filesystem.Root = &FilesystemNode{Children: make(map[string]*FilesystemNode)}
	filesystem.Names = make(map[string][]string)
	filesystem.Stat = make(map[string]*Fileinfo)
	filesystem.Lstat = make(map[string]*Fileinfo)
	filesystem.Directories = make(map[string]*Fileinfo)
	filesystem.Files = make(map[string]*Fileinfo)
	filesystem.NonRegular = make(map[string]*Fileinfo)
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
	filesystem.Stat[pathname] = fileinfo
	filesystem.muStat.Unlock()

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
				filesystem.Lstat[pathname] = &lfileinfo
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
			return nil, fs.ErrNotExist
		}
		p = tmp
	}
	return p, nil
}

func (filesystem *Filesystem) LookupInode(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	filesystem.muStat.Lock()
	fileinfo, exists := filesystem.Stat[pathname]
	filesystem.muStat.Unlock()
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupInodeForFile(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	filesystem.muFiles.Lock()
	fileinfo, exists := filesystem.Files[pathname]
	filesystem.muFiles.Unlock()
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupInodeForDirectory(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	filesystem.muDirectories.Lock()
	fileinfo, exists := filesystem.Directories[pathname]
	filesystem.muDirectories.Unlock()
	return fileinfo, exists
}
