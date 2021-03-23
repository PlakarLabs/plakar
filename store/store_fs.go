package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/iafan/cwalk"
)

type FSStore struct {
	Namespace  string
	Repository string
	root       string

	skipDirs []string

	Store
}

type FSTransaction struct {
	Uuid     string
	store    *FSStore
	prepared bool

	skipDirs []string

	Transaction
}

func (self *FSStore) Init() {
	self.skipDirs = append(self.skipDirs, path.Clean(self.Repository))
	self.root = fmt.Sprintf("%s/%s", self.Repository, self.Namespace)

	os.MkdirAll(fmt.Sprintf("%s", self.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/chunks", self.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", self.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/transactions", self.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/snapshots", self.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/purge", self.root), 0700)
}

func (self *FSStore) Transaction() Transaction {
	tx := &FSTransaction{}
	tx.Uuid = uuid.New().String()
	tx.store = self
	tx.prepared = false
	tx.skipDirs = self.skipDirs

	return tx
}

func (self *FSStore) Snapshot(id string) *Snapshot {
	index, err := self.IndexGet(id)
	if err != nil {
	}

	index, _ = Inflate(index)

	var snapshot Snapshot

	if err = json.Unmarshal(index, &snapshot); err != nil {

	}
	snapshot.store = self

	return &snapshot
}

func (self *FSStore) PathPurge() string {
	return fmt.Sprintf("%s/purge", self.root)
}

func (self *FSStore) PathChunks() string {
	return fmt.Sprintf("%s/chunks", self.root)
}

func (self *FSStore) PathObjects() string {
	return fmt.Sprintf("%s/objects", self.root)
}

func (self *FSStore) PathTransactions() string {
	return fmt.Sprintf("%s/transactions", self.root)
}

func (self *FSStore) PathSnapshots() string {
	return fmt.Sprintf("%s/snapshots", self.root)
}

func (self *FSStore) PathChunkBucket(checksum string) string {
	return fmt.Sprintf("%s/chunks/%s", self.root, checksum[0:2])
}

func (self *FSStore) PathObjectBucket(checksum string) string {
	return fmt.Sprintf("%s/objects/%s", self.root, checksum[0:2])
}

func (self *FSStore) PathSnapshotBucket(checksum string) string {
	return fmt.Sprintf("%s/snapshots/%s", self.root, checksum[0:2])
}

func (self *FSStore) PathChunk(checksum string) string {
	return fmt.Sprintf("%s/%s", self.PathChunkBucket(checksum), checksum)
}

func (self *FSStore) PathObject(checksum string) string {
	return fmt.Sprintf("%s/%s", self.PathObjectBucket(checksum), checksum)
}

func (self *FSStore) PathSnapshot(checksum string) string {
	return fmt.Sprintf("%s/%s", self.PathSnapshotBucket(checksum), checksum)
}

func (self *FSStore) ObjectExists(checksum string) bool {
	return pathnameExists(self.PathObject(checksum))
}

func (self *FSStore) ChunkExists(checksum string) bool {
	return pathnameExists(self.PathChunk(checksum))
}

func (self *FSStore) Snapshots() map[string]os.FileInfo {
	ret := make(map[string]os.FileInfo)

	filepath.Walk(self.PathSnapshots(), func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		_, err = uuid.Parse(f.Name())
		if err != nil {
			return nil
		}

		ret[f.Name()] = f
		return nil
	})

	return ret
}

func (self *FSStore) IndexGet(Uuid string) ([]byte, error) {
	return ioutil.ReadFile(fmt.Sprintf("%s/INDEX", self.PathSnapshot(Uuid)))
}

func (self *FSStore) ObjectGet(checksum string) ([]byte, error) {
	return ioutil.ReadFile(fmt.Sprintf("%s", self.PathObject(checksum)))
}

func (self *FSStore) ChunkGet(checksum string) ([]byte, error) {
	return ioutil.ReadFile(fmt.Sprintf("%s", self.PathChunk(checksum)))
}

func (self *FSStore) Purge(id string) error {
	dest := fmt.Sprintf("%s/%s", self.PathPurge(), id)
	err := os.Rename(self.PathSnapshot(id), dest)
	if err != nil {
		return err
	}

	err = os.RemoveAll(dest)
	if err != nil {
		return err
	}

	self.Tidy()

	return nil
}

func (self *FSStore) Tidy() {
	cwalk.Walk(self.PathObjects(), func(path string, f os.FileInfo, err error) error {
		object := fmt.Sprintf("%s/%s", self.PathObjects(), path)
		if filepath.Clean(object) == filepath.Clean(self.PathObjects()) {
			return nil
		}
		if !f.IsDir() {
			if f.Sys().(*syscall.Stat_t).Nlink == 1 {
				os.Remove(object)
			}
		}
		return nil
	})

	cwalk.Walk(self.PathChunks(), func(path string, f os.FileInfo, err error) error {
		chunk := fmt.Sprintf("%s/%s", self.PathChunks(), path)
		if filepath.Clean(chunk) == filepath.Clean(self.PathChunks()) {
			return nil
		}

		if !f.IsDir() {
			if f.Sys().(*syscall.Stat_t).Nlink == 1 {
				os.Remove(chunk)
			}
		}
		return nil
	})
}

func pathnameExists(pathname string) bool {
	_, err := os.Stat(pathname)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (self *FSTransaction) prepare() {
	os.MkdirAll(self.store.root, 0700)
	os.MkdirAll(fmt.Sprintf("%s/%s", self.store.PathTransactions(),
		self.Uuid[0:2]), 0700)
	os.MkdirAll(fmt.Sprintf("%s", self.Path()), 0700)
	os.MkdirAll(fmt.Sprintf("%s/chunks", self.Path()), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", self.Path()), 0700)
}

func (self *FSTransaction) Snapshot() *Snapshot {
	return &Snapshot{
		Uuid:         uuid.New().String(),
		CreationTime: time.Now(),
		Version:      "0.1.0",
		Directories:  make(map[string]*FileInfo),
		Files:        make(map[string]*FileInfo),
		NonRegular:   make(map[string]*FileInfo),
		Sums:         make(map[string]string),
		Objects:      make(map[string]*Object),
		Chunks:       make(map[string]*Chunk),

		transaction: self,
		skipDirs:    self.skipDirs,
	}
}

func (self *FSTransaction) Path() string {
	return fmt.Sprintf("%s/%s/%s", self.store.PathTransactions(),
		self.Uuid[0:2], self.Uuid)
}

func (self *FSTransaction) PathObjects() string {
	return fmt.Sprintf("%s/objects", self.Path())
}

func (self *FSTransaction) PathObjectBucket(checksum string) string {
	return fmt.Sprintf("%s/%s", self.PathObjects(), checksum[0:2])
}

func (self *FSTransaction) PathObject(checksum string) string {
	return fmt.Sprintf("%s/%s", self.PathObjectBucket(checksum), checksum)
}

func (self *FSTransaction) PathChunks() string {
	return fmt.Sprintf("%s/chunks", self.Path())
}

func (self *FSTransaction) PathChunkBucket(checksum string) string {
	return fmt.Sprintf("%s/%s", self.PathChunks(), checksum[0:2])
}

func (self *FSTransaction) PathChunk(checksum string) string {
	return fmt.Sprintf("%s/%s", self.PathChunkBucket(checksum), checksum)
}

func (self *FSTransaction) ObjectsCheck(keys []string) map[string]bool {
	ret := make(map[string]bool)

	for _, key := range keys {
		ret[key] = self.store.ObjectExists(key)
	}

	return ret
}

func (self *FSTransaction) ChunksMark(keys []string) map[string]bool {
	if !self.prepared {
		self.prepare()
	}

	ret := make(map[string]bool)
	for _, key := range keys {
		os.Mkdir(self.PathChunkBucket(key), 0700)
		err := os.Link(self.store.PathChunk(key), self.PathChunk(key))
		if err != nil {
			if os.IsNotExist(err) {
				ret[key] = false
			} else {
				ret[key] = true
			}
		} else {
			ret[key] = true
		}
	}

	return ret
}

func (self *FSTransaction) ChunksCheck(keys []string) map[string]bool {
	ret := make(map[string]bool)

	for _, key := range keys {
		ret[key] = self.store.ChunkExists(key)
	}

	return ret
}

func (self *FSTransaction) ObjectMark(key string) bool {
	if !self.prepared {
		self.prepare()
	}

	ret := false
	os.Mkdir(self.PathObjectBucket(key), 0700)
	err := os.Link(self.store.PathObject(key), self.PathObject(key))
	if err != nil {
		if os.IsNotExist(err) {
			ret = false
		} else {
			ret = true
		}
	} else {
		ret = true
	}
	return ret
}

func (self *FSTransaction) ObjectRecord(checksum string, buf string) (bool, error) {
	if !self.prepared {
		self.prepare()
	}
	err := error(nil)
	recorded := false
	if self.ChunkExists(checksum) {
		err = self.ObjectLink(checksum)
	} else {
		err = self.ObjectPut(checksum, buf)
		if err == nil {
			recorded = true
		}
	}
	return recorded, err
}

func (self *FSTransaction) ObjectPut(checksum string, buf string) error {
	if !self.prepared {
		self.prepare()
	}
	os.Mkdir(self.PathObjectBucket(checksum), 0700)
	f, err := os.Create(self.PathObject(checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (self *FSTransaction) ObjectLink(checksum string) error {
	if !self.prepared {
		self.prepare()
	}
	os.Mkdir(self.PathObjectBucket(checksum), 0700)
	os.Link(self.store.PathObject(checksum), self.PathObject(checksum))
	return nil
}

func (self *FSTransaction) ChunkRecord(checksum string, buf string) (bool, error) {
	if !self.prepared {
		self.prepare()
	}
	err := error(nil)
	recorded := false
	if self.ChunkExists(checksum) {
		err = self.ChunkLink(checksum)
	} else {
		err = self.ChunkPut(checksum, buf)
		if err == nil {
			recorded = true
		}
	}
	return recorded, err
}

func (self *FSTransaction) ChunksPut(chunks map[string]string) error {
	if !self.prepared {
		self.prepare()
	}

	for checksum, value := range chunks {
		os.Mkdir(self.PathChunkBucket(checksum), 0700)
		f, err := os.Create(self.PathChunk(checksum))
		if err != nil {
			return err
		}
		defer f.Close()

		f.WriteString(value)

	}
	return nil
}

func (self *FSTransaction) ChunkPut(checksum string, buf string) error {
	if !self.prepared {
		self.prepare()
	}
	os.Mkdir(self.PathChunkBucket(checksum), 0700)
	f, err := os.Create(self.PathChunk(checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (self *FSTransaction) ChunkExists(checksum string) bool {
	return self.store.ChunkExists(checksum)
}

func (self *FSTransaction) ChunkLink(checksum string) error {
	if !self.prepared {
		self.prepare()
	}
	os.Mkdir(self.PathChunkBucket(checksum), 0700)
	os.Link(self.store.PathChunk(checksum), self.PathChunk(checksum))
	return nil
}

func (self *FSTransaction) IndexPut(buf string) error {
	if !self.prepared {
		self.prepare()
	}
	f, err := os.Create(fmt.Sprintf("%s/INDEX", self.Path()))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (self *FSTransaction) Commit(snapshot *Snapshot) (*Snapshot, error) {
	if !self.prepared {
		self.prepare()
	}

	// first pass, link chunks to store
	for chunk := range snapshot.Chunks {
		if !self.store.ChunkExists(chunk) {
			os.Mkdir(self.store.PathChunkBucket(chunk), 0700)
			os.Rename(self.PathChunk(chunk), self.store.PathChunk(chunk))
		} else {
			os.Remove(self.PathChunk(chunk))
		}
		os.Link(self.store.PathChunk(chunk), self.PathChunk(chunk))
	}

	// second pass, link objects to store
	for object := range snapshot.Objects {
		if !self.store.ObjectExists(object) {
			os.Mkdir(self.store.PathObjectBucket(object), 0700)
			os.Rename(self.PathObject(object), self.store.PathObject(object))
		} else {
			os.Remove(self.PathObject(object))
		}
		os.Link(self.store.PathObject(object), self.PathObject(object))
	}

	// final pass, move snapshot to store
	os.Mkdir(self.store.PathSnapshotBucket(snapshot.Uuid), 0700)
	os.Rename(self.Path(), self.store.PathSnapshot(snapshot.Uuid))

	return snapshot, nil
}
