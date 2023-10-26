/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package vfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/vfs/importer"
	"github.com/iafan/cwalk"
	"github.com/vmihailenco/msgpack/v5"
)

type FilesystemNode struct {
	muNode   sync.Mutex
	Inode    string
	Children map[string]*FilesystemNode
}

type Filesystem struct {
	importer *importer.Importer

	Root *FilesystemNode

	muInodes sync.Mutex
	Inodes   map[string]FileInfo

	muScannedDirectories sync.Mutex
	scannedDirectories   []string

	muStat   sync.Mutex
	statInfo map[string]*FileInfo

	muSymlinks sync.Mutex
	Symlinks   map[string]string

	nFiles       uint64
	nDirectories uint64
	totalSize    uint64
}

type flattenedFilesystemNode struct {
	Inode    string
	Children []*flattenedFilesystemNode
}

type flattenedInode struct {
	Inode    string
	Fileinfo FileInfo
}

type flattenedSymlink struct {
	Link   string
	Target string
}

type flattenedFilesystem struct {
	Root     flattenedFilesystemNode
	Inodes   []flattenedInode
	Symlinks []flattenedSymlink
}

func NewFilesystem() *Filesystem {
	filesystem := &Filesystem{}
	filesystem.Inodes = make(map[string]FileInfo)
	filesystem.Root = &FilesystemNode{Children: make(map[string]*FilesystemNode)}
	filesystem.statInfo = make(map[string]*FileInfo)
	filesystem.Symlinks = make(map[string]string)
	filesystem.nFiles = 0
	filesystem.nDirectories = 0
	filesystem.totalSize = 0
	return filesystem
}

func (filesystem *Filesystem) _flatten_debug(flatnode *flattenedFilesystemNode, depth int) {
	fmt.Println(strings.Repeat(" ", depth), filesystem.Inodes[flatnode.Inode])
	for i, _ := range flatnode.Children {
		if flatnode.Children[i] != nil {
			filesystem._flatten_debug(flatnode.Children[i], depth+1)
		}
	}
}

func (filesystem *Filesystem) _flatten_fs(fsnode *FilesystemNode, flatnode *flattenedFilesystemNode) {
	flatnode.Inode = fsnode.Inode
	flatnode.Children = make([]*flattenedFilesystemNode, 0)
	for name, node := range fsnode.Children {
		i := sort.Search(len(flatnode.Children), func(i int) bool {
			return filesystem.Inodes[flatnode.Children[i].Inode].Name() > name
		})
		flatnode.Children = append(flatnode.Children, &flattenedFilesystemNode{})
		copy(flatnode.Children[i+1:], flatnode.Children[i:])
		flatnode.Children[i] = &flattenedFilesystemNode{node.Inode, make([]*flattenedFilesystemNode, 0)}

		filesystem._flatten_fs(node, flatnode.Children[i])

	}
}

func (filesystem *Filesystem) flatten() *flattenedFilesystem {
	flattenedFilesystem := &flattenedFilesystem{
		Inodes:   make([]flattenedInode, 0),
		Symlinks: make([]flattenedSymlink, 0),
	}

	for inode, fileinfo := range filesystem.Inodes {
		i := sort.Search(len(flattenedFilesystem.Inodes), func(i int) bool {
			return flattenedFilesystem.Inodes[i].Inode > inode
		})
		flattenedFilesystem.Inodes = append(flattenedFilesystem.Inodes, flattenedInode{})
		copy(flattenedFilesystem.Inodes[i+1:], flattenedFilesystem.Inodes[i:])
		flattenedFilesystem.Inodes[i] = flattenedInode{inode, fileinfo}
	}

	for link, target := range filesystem.Symlinks {
		i := sort.Search(len(flattenedFilesystem.Symlinks), func(i int) bool {
			return flattenedFilesystem.Symlinks[i].Link > link
		})
		flattenedFilesystem.Symlinks = append(flattenedFilesystem.Symlinks, flattenedSymlink{})
		copy(flattenedFilesystem.Symlinks[i+1:], flattenedFilesystem.Symlinks[i:])
		flattenedFilesystem.Symlinks[i] = flattenedSymlink{link, target}
	}

	filesystem._flatten_fs(filesystem.Root, &flattenedFilesystem.Root)

	return flattenedFilesystem
}

func (filesystem *Filesystem) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.Serialize", time.Since(t0))
		logger.Trace("vfs", "Serialize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(filesystem.flatten())
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

/*
func (filesystem *Filesystem) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.Serialize", time.Since(t0))
		logger.Trace("vfs", "Serialize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(filesystem)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}
*/

func (filesystem *Filesystem) _unflatten_fs(fsnode *FilesystemNode, flatnode *flattenedFilesystemNode) {
	fsnode.Inode = flatnode.Inode
	fsnode.Children = make(map[string]*FilesystemNode)
	for _, child := range flatnode.Children {
		childNode := &FilesystemNode{}
		filesystem._unflatten_fs(childNode, child)
		fsnode.Children[filesystem.Inodes[childNode.Inode].Name()] = childNode
	}
}

func NewFilesystemFromBytes(serialized []byte) (*Filesystem, error) {

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.Deserialize", time.Since(t0))
		logger.Trace("vfs", "Deserialize(): %s", time.Since(t0))
	}()

	// Unmarshal the flattened filesystem
	var flatFs flattenedFilesystem
	err := msgpack.Unmarshal(serialized, &flatFs)
	if err != nil {
		return nil, err
	}

	var filesystem Filesystem
	// Populate the Inodes map
	filesystem.Inodes = make(map[string]FileInfo)
	for _, inode := range flatFs.Inodes {
		filesystem.Inodes[inode.Inode] = inode.Fileinfo
	}

	// Populate the Symlinks map
	filesystem.Symlinks = make(map[string]string)
	for _, symlink := range flatFs.Symlinks {
		filesystem.Symlinks[symlink.Link] = symlink.Target
	}

	// Reconstruct the filesystem tree
	filesystem.Root = &FilesystemNode{}
	filesystem._unflatten_fs(filesystem.Root, &flatFs.Root)

	filesystem.reindex()

	return &filesystem, nil
	/*
	   t0 := time.Now()

	   	defer func() {
	   		profiler.RecordEvent("vfs.NewFilesystemFromBytes", time.Since(t0))
	   		logger.Trace("vfs", "NewFilesystemFromBytes(): %s", time.Since(t0))
	   	}()

	   var filesystem Filesystem

	   	if err := msgpack.Unmarshal(serialized, &filesystem); err != nil {
	   		return nil, err
	   	}

	   filesystem.reindex()
	   return &filesystem, nil
	*/
}

/*
func NewFilesystemFromBytes(serialized []byte) (*Filesystem, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.NewFilesystemFromBytes", time.Since(t0))
		logger.Trace("vfs", "NewFilesystemFromBytes(): %s", time.Since(t0))
	}()

	var filesystem Filesystem
	if err := msgpack.Unmarshal(serialized, &filesystem); err != nil {
		return nil, err
	}
	filesystem.reindex()
	return &filesystem, nil
}
*/

func NewFilesystemFromScan(repository string, directory string) (*Filesystem, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.NewFilesystemFromScan", time.Since(t0))
		logger.Trace("vfs", "NewFilesystemFromScan(): %s", time.Since(t0))
	}()

	imp, err := importer.NewImporter(directory)
	if err != nil {
		return nil, err
	}
	imp.Begin(directory)

	schan, echan, err := imp.Scan()
	if err != nil {
		return nil, err
	}

	fs := NewFilesystem()
	fs.importer = imp

	go func() {
		for msg := range echan {
			logger.Warn("%s", msg)
		}
	}()

	for msg := range schan {
		pathname := filepath.Clean(msg.Pathname)
		if pathname == repository || strings.HasPrefix(filepath.ToSlash(pathname), filepath.ToSlash(repository)+"/") {
			continue
		}

		if stat, ok := msg.Stat.(FileInfo); !ok {
			return nil, fmt.Errorf("received invalid stat type")
		} else {
			if pathname != "/" {
				atoms := strings.Split(pathname, "/")
				for i := 0; i < len(atoms)-1; i++ {
					path := filepath.Clean(fmt.Sprintf("%s%s", "/", strings.Join(atoms[0:i], "/")))
					path = filepath.ToSlash(path)
					if _, found := fs.LookupInodeForDirectory(path); !found {
						return nil, err
					}
				}
			}
			pathname = filepath.ToSlash(pathname)
			fs.buildTree(pathname, &stat)

			/*
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
			*/
		}
	}

	return fs, nil
}

func (filesystem *Filesystem) buildTree(pathname string, fileinfo *FileInfo) {
	inodeKey := filesystem.addInode(*fileinfo)

	pathname = filepath.Clean(pathname)
	pathname = filepath.ToSlash(pathname)

	//	filesystem.addPathname(pathname)

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

	if fileinfo.Mode().IsRegular() {
		atomic.AddUint64(&filesystem.nFiles, uint64(1))
		return
	}

	if fileinfo.Mode().IsDir() {
		atomic.AddUint64(&filesystem.nDirectories, uint64(1))
		return
	}

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
		path := filepath.Clean(fmt.Sprintf("%s%s", "/", strings.Join(atoms[0:i], "/")))
		f, err := os.Stat(path)
		if err != nil {
			return err
		}
		fi := FileInfoFromStat(f)
		filesystem.buildTree(path, &fi)
	}

	err := cwalk.Walk(directory, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			logger.Warn("%s", err)
			return nil
		}

		for _, skipPath := range skip {
			if strings.HasPrefix(filepath.Join(directory, path), skipPath) {
				return nil
			}
		}

		pathname := fmt.Sprintf("%s/%s", directory, path)

		fileinfo := FileInfoFromStat(f)
		filesystem.buildTree(pathname, &fileinfo)

		if !fileinfo.Mode().IsDir() && !fileinfo.Mode().IsRegular() {
			lstat, err := os.Lstat(pathname)
			if err != nil {
				logger.Warn("%s", err)
				return nil
			}

			lfileinfo := FileInfoFromStat(lstat)
			if lfileinfo.Mode()&os.ModeSymlink != 0 {
				originFile, err := os.Readlink(lfileinfo.Name())
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
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.Lookup", time.Since(t0))
		logger.Trace("vfs", "Lookup(%s): %s", pathname, time.Since(t0))
	}()
	pathname = filepath.Clean(pathname)
	pathname = filepath.ToSlash(pathname)
	if pathname == "." {
		pathname = "/"
	}

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

func (filesystem *Filesystem) LookupInode(pathname string) (*FileInfo, bool) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.LookupInode", time.Since(t0))
		logger.Trace("vfs", "LookupInode(%s): %s", pathname, time.Since(t0))
	}()
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	pathname = filepath.Clean(pathname)
	pathname = filepath.ToSlash(pathname)
	if pathname == "." {
		pathname = "/"
	}

	fileinfo, exists := filesystem.statInfo[pathname]
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupInodeForFile(pathname string) (*FileInfo, bool) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.LookupInodeForFile", time.Since(t0))
		logger.Trace("vfs", "LookupInodeForFile(%s): %s", pathname, time.Since(t0))
	}()
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	pathname = filepath.Clean(pathname)
	pathname = filepath.ToSlash(pathname)
	if pathname == "." {
		pathname = "/"
	}

	fileinfo, exists := filesystem.statInfo[pathname]
	if !exists || !fileinfo.Mode().IsRegular() {
		return nil, false
	}
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupInodeForDirectory(pathname string) (*FileInfo, bool) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.LookupInodeForDirectory", time.Since(t0))
		logger.Trace("vfs", "LookupInodeForDirectory(%s): %s", pathname, time.Since(t0))
	}()
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	pathname = filepath.Clean(pathname)
	pathname = filepath.ToSlash(pathname)
	if pathname == "." {
		pathname = "/"
	}

	fileinfo, exists := filesystem.statInfo[pathname]
	if !exists || !fileinfo.Mode().IsDir() {
		return nil, false
	}
	return fileinfo, exists
}

func (filesystem *Filesystem) LookupChildren(pathname string) ([]string, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.LookupChildren", time.Since(t0))
		logger.Trace("vfs", "LookupChildren(%s): %s", pathname, time.Since(t0))
	}()
	pathname = filepath.Clean(pathname)
	pathname = filepath.ToSlash(pathname)
	if pathname == "." {
		pathname = "/"
	}

	parent, err := filesystem.Lookup(pathname)
	if err != nil {
		return nil, os.ErrNotExist
	}

	filesystem.muInodes.Lock()
	parentInode := filesystem.Inodes[parent.Inode]
	filesystem.muInodes.Unlock()

	if !parentInode.Mode().IsDir() {
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
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.ListFiles", time.Since(t0))
		logger.Trace("vfs", "ListFiles(): %s", time.Since(t0))
	}()
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	list := make([]string, 0)
	for pathname, stat := range filesystem.statInfo {
		if stat.Mode().IsRegular() {
			list = append(list, pathname)
		}
	}
	return list
}

func (filesystem *Filesystem) ListDirectories() []string {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.ListDirectories", time.Since(t0))
		logger.Trace("vfs", "ListDirectories(): %s", time.Since(t0))
	}()
	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	list := make([]string, 0)
	for pathname, stat := range filesystem.statInfo {
		if stat.Mode().IsDir() {
			list = append(list, pathname)
		}
	}
	return list
}

func (filesystem *Filesystem) ListNonRegular() []string {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.ListNonRegular", time.Since(t0))
		logger.Trace("vfs", "ListNonRegular(): %s", time.Since(t0))
	}()

	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	list := make([]string, 0)
	for pathname, stat := range filesystem.statInfo {
		if !stat.Mode().IsDir() && !stat.Mode().IsRegular() {
			list = append(list, pathname)
		}
	}
	return list
}

func (filesystem *Filesystem) ListStat() []string {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.ListStat", time.Since(t0))
		logger.Trace("vfs", "ListStat(): %s", time.Since(t0))
	}()

	filesystem.muStat.Lock()
	defer filesystem.muStat.Unlock()

	list := make([]string, 0)
	for pathname := range filesystem.statInfo {
		list = append(list, pathname)
	}
	return list
}

func (filesystem *Filesystem) _reindex(pathname string) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs._reindex", time.Since(t0))
		logger.Trace("vfs", "_reindex(): %s", time.Since(t0))
	}()

	node, err := filesystem.Lookup(pathname)
	if err != nil {
		return
	}

	pathnameInode := filesystem.Inodes[node.Inode]
	filesystem.statInfo[pathname] = &pathnameInode
	filesystem.totalSize += uint64(pathnameInode.Size())

	for name, node := range node.Children {
		nodeInode := filesystem.Inodes[node.Inode]
		child := filepath.Clean(fmt.Sprintf("%s/%s", pathname, name))
		if nodeInode.Mode().IsDir() {
			filesystem._reindex(child)
		} else {
			filesystem.statInfo[child] = &nodeInode
		}
	}
}

func (filesystem *Filesystem) reindex() {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.reindex", time.Since(t0))
		logger.Trace("vfs", "reindex(): %s", time.Since(t0))
	}()
	filesystem.statInfo = make(map[string]*FileInfo)
	filesystem._reindex("/")
}

func (filesystem *Filesystem) addInode(fileinfo FileInfo) string {
	filesystem.muInodes.Lock()
	defer filesystem.muInodes.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.addInode", time.Since(t0))
		logger.Trace("vfs", "addInode(): %s", time.Since(t0))
	}()

	key := fmt.Sprintf("%d,%d", fileinfo.Dev(), fileinfo.Ino())
	if _, exists := filesystem.Inodes[key]; !exists {
		filesystem.Inodes[key] = fileinfo
		filesystem.totalSize += uint64(fileinfo.Size())
	}
	return key
}

func (filesystem *Filesystem) Size() uint64 {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.Size", time.Since(t0))
		logger.Trace("vfs", "Size(): %s", time.Since(t0))
	}()
	return filesystem.totalSize
}

func (filesystem *Filesystem) NFiles() uint64 {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.NDirectories", time.Since(t0))
		logger.Trace("vfs", "NFiles(): %s", time.Since(t0))
	}()
	return filesystem.nFiles
}

func (filesystem *Filesystem) NDirectories() uint64 {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.NDirectories", time.Since(t0))
		logger.Trace("vfs", "NDirectories(): %s", time.Since(t0))
	}()
	return filesystem.nDirectories
}

func (filesystem *Filesystem) ImporterBegin(location string) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.ImporterBegin", time.Since(t0))
		logger.Trace("vfs", "ImporterBegin(): %s", time.Since(t0))
	}()
	return filesystem.importer.Begin(location)
}

func (filesystem *Filesystem) ImporterEnd() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.ImporterEnd", time.Since(t0))
		logger.Trace("vfs", "ImporterEnd(): %s", time.Since(t0))
	}()
	return filesystem.importer.End()
}

func (filesystem *Filesystem) ImporterOpen(filename string) (io.ReadCloser, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.ImporterOpen", time.Since(t0))
		logger.Trace("vfs", "ImporterOpen(): %s", time.Since(t0))
	}()
	return filesystem.importer.Open(filename)
}
