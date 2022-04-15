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
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"

	"github.com/google/uuid"
	"github.com/iafan/cwalk"
)

func init() {
	storage.Register("filesystem", NewFSStore)
}

func NewFSStore() storage.RepositoryBackend {
	return &FSStore{}
}

func (repository *FSStore) objectExists(checksum string) bool {
	return pathnameExists(repository.PathObject(checksum))
}

func (repository *FSStore) chunkExists(checksum string) bool {
	return pathnameExists(repository.PathChunk(checksum))
}

func (repository *FSStore) Create(location string, config storage.RepositoryConfig) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("Create(%s): %s", location, time.Since(t0))
	}()

	repository.root = location

	err := os.Mkdir(repository.root, 0700)
	if err != nil {
		return err
	}
	os.MkdirAll(fmt.Sprintf("%s/chunks", repository.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", repository.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/transactions", repository.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/snapshots", repository.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/purge", repository.root), 0700)

	for i := 0; i < 256; i++ {
		os.MkdirAll(fmt.Sprintf("%s/chunks/%02x", repository.root, i), 0700)
		os.MkdirAll(fmt.Sprintf("%s/objects/%02x", repository.root, i), 0700)
		os.MkdirAll(fmt.Sprintf("%s/transactions/%02x", repository.root, i), 0700)
		os.MkdirAll(fmt.Sprintf("%s/snapshots/%02x", repository.root, i), 0700)
	}

	f, err := os.Create(fmt.Sprintf("%s/CONFIG", repository.root))
	if err != nil {
		return err
	}
	defer f.Close()

	jconfig, err := json.Marshal(config)
	if err != nil {
		return err
	}

	_, err = f.Write(compression.Deflate(jconfig))
	if err != nil {
		return err
	}

	repository.config = config

	return nil
}

func (repository *FSStore) Open(location string) error {
	repository.root = location

	compressed, err := ioutil.ReadFile(fmt.Sprintf("%s/CONFIG", repository.root))
	if err != nil {
		return err
	}

	jconfig, err := compression.Inflate(compressed)
	if err != nil {
		return err
	}

	config := storage.RepositoryConfig{}
	err = json.Unmarshal(jconfig, &config)
	if err != nil {
		return err
	}

	repository.config = config

	return nil
}

func (repository *FSStore) Configuration() storage.RepositoryConfig {
	return repository.config
}

func (repository *FSStore) Transaction() (storage.TransactionBackend, error) {
	// XXX - keep a map of current transactions

	tx := &FSTransaction{}
	tx.Uuid = uuid.New().String()
	tx.store = *repository
	tx.prepared = false

	tx.chunks = make(map[string]bool)
	tx.objects = make(map[string]bool)
	tx.chunkBucket = make(map[string]bool)
	tx.objectBucket = make(map[string]bool)

	tx.prepare()

	return tx, nil
}

func (repository *FSStore) GetIndexes() ([]string, error) {
	ret := make([]string, 0)

	buckets, err := ioutil.ReadDir(repository.PathIndexes())
	if err != nil {
		return ret, nil
	}

	for _, bucket := range buckets {
		indexes, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", repository.PathIndexes(), bucket.Name()))
		if err != nil {
			return ret, err
		}
		for _, index := range indexes {
			_, err = uuid.Parse(index.Name())
			if err != nil {
				return ret, nil
			}
			ret = append(ret, index.Name())
		}
	}
	return ret, nil
}

func (repository *FSStore) GetMetadata(Uuid string) ([]byte, error) {
	parsedUuid, err := uuid.Parse(Uuid)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadFile(fmt.Sprintf("%s/METADATA", repository.PathIndex(parsedUuid.String())))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *FSStore) GetIndex(Uuid string) ([]byte, error) {
	parsedUuid, err := uuid.Parse(Uuid)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadFile(fmt.Sprintf("%s/INDEX", repository.PathIndex(parsedUuid.String())))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *FSStore) PutMetadata(id string, data []byte) error {
	os.Mkdir(repository.PathIndexBucket(id), 0700)
	os.Mkdir(repository.PathIndex(id), 0700)
	os.Mkdir(repository.PathIndexObjects(id), 0700)
	os.Mkdir(repository.PathIndexChunks(id), 0700)

	f, err := os.Create(fmt.Sprintf("%s/METADATA", repository.PathIndex(id)))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (repository *FSStore) PutIndex(id string, data []byte) error {
	os.Mkdir(repository.PathIndexBucket(id), 0700)
	os.Mkdir(repository.PathIndex(id), 0700)
	os.Mkdir(repository.PathIndexObjects(id), 0700)
	os.Mkdir(repository.PathIndexChunks(id), 0700)

	f, err := os.Create(fmt.Sprintf("%s/INDEX", repository.PathIndex(id)))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (repository *FSStore) ReferenceIndexChunk(id string, checksum string) error {
	os.Mkdir(repository.PathIndexChunkBucket(id, checksum), 0700)
	os.Link(repository.PathChunk(checksum), repository.PathIndexChunk(id, checksum))
	return nil
}

func (repository *FSStore) ReferenceIndexObject(id string, checksum string) error {
	os.Mkdir(repository.PathIndexObjectBucket(id, checksum), 0700)
	os.Link(repository.PathObject(checksum), repository.PathIndexObject(id, checksum))
	return nil
}

func (repository *FSStore) GetObjects() ([]string, error) {
	ret := make([]string, 0)

	buckets, err := ioutil.ReadDir(repository.PathObjects())
	if err != nil {
		return nil, err
	}

	for _, bucket := range buckets {
		objects, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", repository.PathObjects(), bucket.Name()))
		if err != nil {
			return ret, err
		}

		for _, object := range objects {
			//_, err = uuid.Parse(object.Name())
			//if err != nil {
			//		return ret, nil
			//	}
			ret = append(ret, object.Name())
		}
	}
	return ret, nil
}

func (repository *FSStore) GetIndexObject(id string, checksum string) ([]byte, error) {
	data, err := ioutil.ReadFile(repository.PathIndexObject(id, checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *FSStore) GetIndexChunk(id string, checksum string) ([]byte, error) {
	data, err := ioutil.ReadFile(repository.PathIndexChunk(id, checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *FSStore) GetObject(checksum string) ([]byte, error) {
	data, err := ioutil.ReadFile(repository.PathObject(checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *FSStore) GetObjectRefCount(checksum string) (uint64, error) {
	st, err := os.Stat(repository.PathObject(checksum))
	if err != nil {
		return 0, err
	}
	return uint64(st.Sys().(*syscall.Stat_t).Nlink - 1), nil
}

func (repository *FSStore) GetChunkRefCount(checksum string) (uint64, error) {
	st, err := os.Stat(repository.PathChunk(checksum))
	if err != nil {
		return 0, err
	}
	return uint64(st.Sys().(*syscall.Stat_t).Nlink - 1), nil
}

func (repository *FSStore) GetObjectSize(checksum string) (uint64, error) {
	st, err := os.Stat(repository.PathObject(checksum))
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()), nil
}

func (repository *FSStore) GetChunkSize(checksum string) (uint64, error) {
	st, err := os.Stat(repository.PathChunk(checksum))
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()), nil
}

func (repository *FSStore) PutObject(checksum string, data []byte) error {
	f, err := ioutil.TempFile(repository.PathObjectBucket(checksum), fmt.Sprintf("%s.*", checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	err = os.Rename(f.Name(), repository.PathObject(checksum))
	if err != nil {
		return err
	}

	return nil
}

func (repository *FSStore) PutObjectSafe(checksum string, data []byte, link string) error {
	f, err := ioutil.TempFile(repository.PathObjectBucket(checksum), fmt.Sprintf("%s.*", checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	err = os.Link(f.Name(), link)
	if err != nil {
		return err
	}

	err = os.Rename(f.Name(), repository.PathObject(checksum))
	if err != nil {
		return err
	}

	return nil
}

func (repository *FSStore) GetChunks() ([]string, error) {
	ret := make([]string, 0)

	buckets, err := ioutil.ReadDir(repository.PathChunks())
	if err != nil {
		return nil, err
	}

	for _, bucket := range buckets {
		chunks, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", repository.PathChunks(), bucket.Name()))
		if err != nil {
			return ret, err
		}

		for _, chunk := range chunks {
			//_, err = uuid.Parse(object.Name())
			//if err != nil {
			//		return ret, nil
			//	}
			ret = append(ret, chunk.Name())
		}
	}
	return ret, nil
}

func (repository *FSStore) GetChunk(checksum string) ([]byte, error) {
	data, err := ioutil.ReadFile(repository.PathChunk(checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *FSStore) PutChunk(checksum string, data []byte) error {
	f, err := ioutil.TempFile(repository.PathChunkBucket(checksum), fmt.Sprintf("%s.*", checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	err = os.Rename(f.Name(), repository.PathChunk(checksum))
	if err != nil {
		return err
	}

	return nil
}

func (repository *FSStore) PutChunkSafe(checksum string, data []byte, link string) error {
	f, err := ioutil.TempFile(repository.PathChunkBucket(checksum), fmt.Sprintf("%s.*", checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	err = os.Link(f.Name(), link)
	if err != nil {
		return err
	}

	err = os.Rename(f.Name(), repository.PathChunk(checksum))
	if err != nil {
		return err
	}

	return nil
}

func (repository *FSStore) CheckIndexObject(id string, checksum string) (bool, error) {
	fileinfo, err := os.Stat(repository.PathIndexObject(id, checksum))
	if os.IsNotExist(err) {
		return false, nil
	}
	return fileinfo.Mode().IsRegular(), nil
}

func (repository *FSStore) CheckIndexChunk(id string, checksum string) (bool, error) {
	fileinfo, err := os.Stat(repository.PathIndexChunk(id, checksum))
	if os.IsNotExist(err) {
		return false, nil
	}
	return fileinfo.Mode().IsRegular(), nil

}

func (repository *FSStore) CheckObject(checksum string) (bool, error) {
	fileinfo, err := os.Stat(repository.PathObject(checksum))
	if os.IsNotExist(err) {
		return false, nil
	}
	return fileinfo.Mode().IsRegular(), nil
}

func (repository *FSStore) CheckChunk(checksum string) (bool, error) {
	fileinfo, err := os.Stat(repository.PathChunk(checksum))
	if os.IsNotExist(err) {
		return false, nil
	}
	return fileinfo.Mode().IsRegular(), nil

}

func (repository *FSStore) Purge(id string) error {
	dest := fmt.Sprintf("%s/%s", repository.PathPurge(), id)
	err := os.Rename(repository.PathIndex(id), dest)
	if err != nil {
		return err
	}

	err = os.RemoveAll(dest)
	if err != nil {
		return err
	}

	return nil
}

func (repository *FSStore) Close() error {
	// XXX - rollback all pending transactions so they don't linger
	repository.Tidy()
	return nil
}

func (repository *FSStore) Tidy() {
	cwalk.Walk(repository.PathObjects(), func(path string, f os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		object := fmt.Sprintf("%s/%s", repository.PathObjects(), path)
		if filepath.Clean(object) == filepath.Clean(repository.PathObjects()) {
			return nil
		}
		if !f.IsDir() {
			if f.Sys().(*syscall.Stat_t).Nlink == 1 {
				os.Remove(object)
			}
		}
		return nil
	})

	cwalk.Walk(repository.PathChunks(), func(path string, f os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		chunk := fmt.Sprintf("%s/%s", repository.PathChunks(), path)
		if filepath.Clean(chunk) == filepath.Clean(repository.PathChunks()) {
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
