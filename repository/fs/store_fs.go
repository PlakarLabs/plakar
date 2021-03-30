/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
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

package fs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/poolpOrg/plakar/repository"
	"github.com/poolpOrg/plakar/repository/compression"
	"github.com/poolpOrg/plakar/repository/encryption"

	"github.com/google/uuid"
	"github.com/iafan/cwalk"
)

type FSStore struct {
	Namespace  string
	Repository string
	root       string

	SkipDirs []string

	repository.Store
}

type FSTransaction struct {
	Uuid     string
	store    *FSStore
	prepared bool

	SkipDirs []string

	repository.Transaction
}

func (store *FSStore) Init() {
	store.SkipDirs = append(store.SkipDirs, path.Clean(store.Repository))
	store.root = fmt.Sprintf("%s/%s", store.Repository, store.Namespace)

	os.MkdirAll(store.root, 0700)
	os.MkdirAll(fmt.Sprintf("%s/chunks", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/transactions", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/snapshots", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/purge", store.root), 0700)
}

func (store *FSStore) Transaction() repository.Transaction {
	tx := &FSTransaction{}
	tx.Uuid = uuid.New().String()
	tx.store = store
	tx.prepared = false
	tx.SkipDirs = store.SkipDirs
	return tx
}

func (store *FSStore) Snapshot(id string) (*repository.Snapshot, error) {
	index, err := store.IndexGet(id)
	if err != nil {
		return nil, err
	}

	encryptionKey := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	index, _ = encryption.Decrypt(encryptionKey, index)
	index, _ = compression.Inflate(index)

	var snapshotStorage repository.SnapshotStorage

	if err = json.Unmarshal(index, &snapshotStorage); err != nil {
		return nil, err
	}

	snapshot := repository.Snapshot{}
	snapshot.Uuid = snapshotStorage.Uuid
	snapshot.CreationTime = snapshotStorage.CreationTime
	snapshot.Version = snapshotStorage.Version
	snapshot.Directories = snapshotStorage.Directories
	snapshot.Files = snapshotStorage.Files
	snapshot.NonRegular = snapshotStorage.NonRegular
	snapshot.Sums = snapshotStorage.Sums
	snapshot.Objects = snapshotStorage.Objects
	snapshot.Chunks = snapshotStorage.Chunks
	snapshot.Size = snapshotStorage.Size
	snapshot.RealSize = snapshotStorage.RealSize
	snapshot.BackingStore = store

	return &snapshot, nil
}

func (store *FSStore) ObjectExists(checksum string) bool {
	return pathnameExists(store.PathObject(checksum))
}

func (store *FSStore) ChunkExists(checksum string) bool {
	return pathnameExists(store.PathChunk(checksum))
}

func (store *FSStore) Snapshots() ([]string, error) {
	ret := make([]string, 0)

	err := filepath.Walk(store.PathSnapshots(), func(path string, f os.FileInfo, err error) error {
		_, err = uuid.Parse(f.Name())
		if err != nil {
			return nil
		}

		ret = append(ret, f.Name())
		return nil
	})

	return ret, err
}

func (store *FSStore) IndexGet(Uuid string) ([]byte, error) {
	return ioutil.ReadFile(fmt.Sprintf("%s/INDEX", store.PathSnapshot(Uuid)))
}

func (store *FSStore) ObjectGet(checksum string) ([]byte, error) {
	return ioutil.ReadFile(store.PathObject(checksum))
}

func (store *FSStore) ChunkGet(checksum string) ([]byte, error) {
	return ioutil.ReadFile(store.PathChunk(checksum))
}

func (store *FSStore) Purge(id string) error {
	dest := fmt.Sprintf("%s/%s", store.PathPurge(), id)
	err := os.Rename(store.PathSnapshot(id), dest)
	if err != nil {
		return err
	}

	err = os.RemoveAll(dest)
	if err != nil {
		return err
	}

	store.Tidy()

	return nil
}

func (store *FSStore) Tidy() {
	cwalk.Walk(store.PathObjects(), func(path string, f os.FileInfo, err error) error {
		object := fmt.Sprintf("%s/%s", store.PathObjects(), path)
		if filepath.Clean(object) == filepath.Clean(store.PathObjects()) {
			return nil
		}
		if !f.IsDir() {
			if f.Sys().(*syscall.Stat_t).Nlink == 1 {
				os.Remove(object)
			}
		}
		return nil
	})

	cwalk.Walk(store.PathChunks(), func(path string, f os.FileInfo, err error) error {
		chunk := fmt.Sprintf("%s/%s", store.PathChunks(), path)
		if filepath.Clean(chunk) == filepath.Clean(store.PathChunks()) {
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
	return !os.IsNotExist(err)
}

func (transaction *FSTransaction) prepare() {
	os.MkdirAll(transaction.store.root, 0700)
	os.MkdirAll(fmt.Sprintf("%s/%s", transaction.store.PathTransactions(),
		transaction.Uuid[0:2]), 0700)
	os.MkdirAll(transaction.Path(), 0700)
	os.MkdirAll(fmt.Sprintf("%s/chunks", transaction.Path()), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", transaction.Path()), 0700)
}

func (transaction *FSTransaction) Snapshot() *repository.Snapshot {
	return &repository.Snapshot{
		Uuid:         transaction.Uuid,
		CreationTime: time.Now(),
		Version:      "0.1.0",
		Directories:  make(map[string]*repository.FileInfo),
		Files:        make(map[string]*repository.FileInfo),
		NonRegular:   make(map[string]*repository.FileInfo),
		Sums:         make(map[string]string),
		Objects:      make(map[string]*repository.Object),
		Chunks:       make(map[string]*repository.Chunk),

		BackingTransaction: transaction,
		SkipDirs:           transaction.SkipDirs,
	}
}

func (transaction *FSTransaction) ObjectsCheck(keys []string) map[string]bool {
	ret := make(map[string]bool)

	for _, key := range keys {
		ret[key] = transaction.store.ObjectExists(key)
	}

	return ret
}

func (transaction *FSTransaction) ChunksMark(keys []string) map[string]bool {
	if !transaction.prepared {
		transaction.prepare()
	}

	ret := make(map[string]bool)
	for _, key := range keys {
		os.Mkdir(transaction.PathChunkBucket(key), 0700)
		err := os.Link(transaction.store.PathChunk(key), transaction.PathChunk(key))
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

func (transaction *FSTransaction) ChunksCheck(keys []string) map[string]bool {
	ret := make(map[string]bool)

	for _, key := range keys {
		ret[key] = transaction.store.ChunkExists(key)
	}

	return ret
}

func (transaction *FSTransaction) ObjectMark(key string) bool {
	if !transaction.prepared {
		transaction.prepare()
	}

	ret := false
	os.Mkdir(transaction.PathObjectBucket(key), 0700)
	err := os.Link(transaction.store.PathObject(key), transaction.PathObject(key))
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

func (transaction *FSTransaction) ObjectRecord(checksum string, buf string) (bool, error) {
	if !transaction.prepared {
		transaction.prepare()
	}
	err := error(nil)
	recorded := false
	if transaction.ChunkExists(checksum) {
		err = transaction.ObjectLink(checksum)
	} else {
		err = transaction.ObjectPut(checksum, buf)
		if err == nil {
			recorded = true
		}
	}
	return recorded, err
}

func (transaction *FSTransaction) ObjectPut(checksum string, buf string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathObjectBucket(checksum), 0700)
	f, err := os.Create(transaction.PathObject(checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (transaction *FSTransaction) ObjectLink(checksum string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathObjectBucket(checksum), 0700)
	os.Link(transaction.store.PathObject(checksum), transaction.PathObject(checksum))
	return nil
}

func (transaction *FSTransaction) ChunkRecord(checksum string, buf string) (bool, error) {
	if !transaction.prepared {
		transaction.prepare()
	}
	err := error(nil)
	recorded := false
	if transaction.ChunkExists(checksum) {
		err = transaction.ChunkLink(checksum)
	} else {
		err = transaction.ChunkPut(checksum, buf)
		if err == nil {
			recorded = true
		}
	}
	return recorded, err
}

func (transaction *FSTransaction) ChunkPut(checksum string, buf string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathChunkBucket(checksum), 0700)
	f, err := os.Create(transaction.PathChunk(checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (transaction *FSTransaction) ChunkExists(checksum string) bool {
	return transaction.store.ChunkExists(checksum)
}

func (transaction *FSTransaction) ChunkLink(checksum string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathChunkBucket(checksum), 0700)
	os.Link(transaction.store.PathChunk(checksum), transaction.PathChunk(checksum))
	return nil
}

func (transaction *FSTransaction) IndexPut(buf string) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	f, err := os.Create(fmt.Sprintf("%s/INDEX", transaction.Path()))
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString(buf)
	return nil
}

func (transaction *FSTransaction) Commit(snapshot *repository.Snapshot) (*repository.Snapshot, error) {
	if !transaction.prepared {
		transaction.prepare()
	}

	// first pass, link chunks to store
	for chunk := range snapshot.Chunks {
		if !transaction.store.ChunkExists(chunk) {
			os.Mkdir(transaction.store.PathChunkBucket(chunk), 0700)
			os.Rename(transaction.PathChunk(chunk), transaction.store.PathChunk(chunk))
		} else {
			os.Remove(transaction.PathChunk(chunk))
		}
		os.Link(transaction.store.PathChunk(chunk), transaction.PathChunk(chunk))
	}

	// second pass, link objects to store
	for object := range snapshot.Objects {
		if !transaction.store.ObjectExists(object) {
			os.Mkdir(transaction.store.PathObjectBucket(object), 0700)
			os.Rename(transaction.PathObject(object), transaction.store.PathObject(object))
		} else {
			os.Remove(transaction.PathObject(object))
		}
		os.Link(transaction.store.PathObject(object), transaction.PathObject(object))
	}

	// final pass, move snapshot to store
	os.Mkdir(transaction.store.PathSnapshotBucket(snapshot.Uuid), 0700)
	os.Rename(transaction.Path(), transaction.store.PathSnapshot(snapshot.Uuid))

	return snapshot, nil
}
