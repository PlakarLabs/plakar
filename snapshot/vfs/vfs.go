package vfs

import (
	"io"
	"io/fs"
	"log"
	"path"
	"strings"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION = 002

type FSEntry interface {
	Stat() *objects.FileInfo
	Name() string
	Size() int64
	Path() string
}

type Classification struct {
	Analyzer string   `msgpack:"analyzer" json:"analyzer"`
	Classes  []string `msgpack:"classes" json:"classes"`
}

type ExtendedAttribute struct {
	Name  string `msgpack:"name" json:"name"`
	Value []byte `msgpack:"value" json:"value"`
}

type CustomMetadata struct {
	Key   string `msgpack:"key" json:"key"`
	Value []byte `msgpack:"value" json:"value"`
}

type AlternateDataStream struct {
	Name    string `msgpack:"name" json:"name"`
	Content []byte `msgpack:"content" json:"content"`
}

type Filesystem struct {
	tree *btree.BTree[string, objects.Checksum, Entry]
	repo *repository.Repository
}

func PathCmp(a, b string) int {
	da := strings.Count(a, "/")
	db := strings.Count(b, "/")

	if da > db {
		return 1
	}
	if da < db {
		return -1
	}
	return strings.Compare(a, b)
}

func NewFilesystem(repo *repository.Repository, root objects.Checksum) (*Filesystem, error) {
	rd, err := repo.GetBlob(packfile.TYPE_FILE, root)
	if err != nil {
		return nil, err
	}

	bytes, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	var tree btree.BTree[string, objects.Checksum, Entry]
	if err := msgpack.Unmarshal(bytes, &tree); err != nil {
		return nil, err
	}

	storage := repository.NewRepositoryStore[string, Entry](repo, packfile.TYPE_FILE)
	fs := &Filesystem{
		tree: btree.FromStorage(tree.Root, storage, PathCmp, tree.Order),
		repo: repo,
	}

	iter, _ := fs.tree.ScanAll()
	for iter.Next() {
		path, _ := iter.Current()
		log.Println("tree:", path)
	}

	return fs, nil
}

func (fsc *Filesystem) lookup(entrypath string) (*Entry, error) {
	if !strings.HasPrefix(entrypath, "/") {
		entrypath = "/" + entrypath
	}
	entrypath = path.Clean(entrypath)

	if entrypath == "" {
		entrypath = "/"
	}

	entry, found, err := fsc.tree.Find(entrypath)
	if err != nil {
		log.Println("error looking up", entrypath, ":", err)
		return nil, err
	}
	if !found {
		log.Println("path not found", entrypath)
		return nil, fs.ErrNotExist
	}
	//log.Println("found", path, ":", entry)
	return &entry, nil
}

func (fsc *Filesystem) Open(path string) (fs.File, error) {
	entry, err := fsc.lookup(path)
	if err != nil {
		return nil, err
	}

	return entry.Open(fsc, path), nil
}

func (fsc *Filesystem) ReadDir(path string) (entries []fs.DirEntry, err error) {
	fp, err := fsc.Open(path)
	if err != nil {
		return
	}
	dir, ok := fp.(fs.ReadDirFile)
	if !ok {
		return entries, fs.ErrInvalid
	}

	return dir.ReadDir(-1)
}

func (fsc *Filesystem) Files() <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)

		iter, err := fsc.tree.ScanAll()
		if err != nil {
			return
		}

		for iter.Next() {
			path, entry := iter.Current()
			if entry.FileInfo.Lmode.IsRegular() {
				ch <- path
			}
		}
	}()
	return ch
}

func (fsc *Filesystem) Pathnames() <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)

		iter, err := fsc.tree.ScanAll()
		if err != nil {
			return
		}

		for iter.Next() {
			path, _ := iter.Current()
			ch <- path
		}
	}()
	return ch
}

func (fsc *Filesystem) GetEntry(path string) (*Entry, error) {
	return fsc.lookup(path)
}

func (fsc *Filesystem) Stat(path string) (FSEntry, error) {
	return fsc.lookup(path)
}

func (fsc *Filesystem) Children(path string) (<-chan string, error) {
	fp, err := fsc.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	dir, ok := fp.(fs.ReadDirFile)
	if !ok {
		return nil, fs.ErrInvalid
	}

	ch := make(chan string)
	go func() {
		defer close(ch)
		for {
			entries, err := dir.ReadDir(16)
			if err != nil {
				return
			}
			for i := range entries {
				ch <- entries[i].Name()
			}
		}
	}()
	return ch, nil
}

func (fsc *Filesystem) ChildrenIter(path string) (chan<- string, error) {
	fp, err := fsc.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	dir, ok := fp.(fs.ReadDirFile)
	if !ok {
		return nil, fs.ErrInvalid
	}

	ch := make(chan string)
	go func() {
		defer close(ch)
		for {
			entries, err := dir.ReadDir(16)
			if err != nil {
				return
			}
			for i := range entries {
				ch <- entries[i].Name()
			}
		}
	}()
	return ch, nil
}
