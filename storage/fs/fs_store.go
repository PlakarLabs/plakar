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
	storage.Register("filesystem", &FSStore{})
}

func (store *FSStore) objectExists(checksum string) bool {
	return pathnameExists(store.PathObject(checksum))
}

func (store *FSStore) chunkExists(checksum string) bool {
	return pathnameExists(store.PathChunk(checksum))
}

func (store *FSStore) Create(repository string, config storage.StoreConfig) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("Create(%s): %s", repository, time.Since(t0))
	}()

	store.root = repository

	err := os.Mkdir(store.root, 0700)
	if err != nil {
		return err
	}
	os.MkdirAll(fmt.Sprintf("%s/chunks", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/transactions", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/snapshots", store.root), 0700)
	os.MkdirAll(fmt.Sprintf("%s/purge", store.root), 0700)

	f, err := os.Create(fmt.Sprintf("%s/CONFIG", store.root))
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

	return nil
}

func (store *FSStore) Open(repository string) error {
	store.root = repository

	compressed, err := ioutil.ReadFile(fmt.Sprintf("%s/CONFIG", store.root))
	if err != nil {
		return err
	}

	jconfig, err := compression.Inflate(compressed)
	if err != nil {
		return err
	}

	config := storage.StoreConfig{}
	err = json.Unmarshal(jconfig, &config)
	if err != nil {
		return err
	}

	store.config = config

	return nil
}

func (store *FSStore) Configuration() storage.StoreConfig {
	return store.config
}

func (store *FSStore) Transaction() (storage.TransactionBackend, error) {
	// XXX - keep a map of current transactions

	tx := &FSTransaction{}
	tx.Uuid = uuid.New().String()
	tx.store = *store
	tx.prepared = false

	tx.chunks = make(map[string]bool)
	tx.objects = make(map[string]bool)

	return tx, nil
}

func (store *FSStore) GetIndexes() ([]string, error) {
	ret := make([]string, 0)

	buckets, err := ioutil.ReadDir(store.PathIndexes())
	if err != nil {
		return ret, nil
	}

	for _, bucket := range buckets {
		indexes, err := ioutil.ReadDir(fmt.Sprintf("%s/%s", store.PathIndexes(), bucket.Name()))
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

func (store *FSStore) GetIndex(Uuid string) ([]byte, error) {
	parsedUuid, err := uuid.Parse(Uuid)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadFile(fmt.Sprintf("%s/INDEX", store.PathIndex(parsedUuid.String())))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (store *FSStore) GetObject(checksum string) ([]byte, error) {
	data, err := ioutil.ReadFile(store.PathObject(checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (store *FSStore) GetChunk(checksum string) ([]byte, error) {
	data, err := ioutil.ReadFile(store.PathChunk(checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (store *FSStore) CheckObject(checksum string) (bool, error) {
	fileinfo, err := os.Stat(store.PathObject(checksum))
	if os.IsNotExist(err) {
		return false, nil
	}
	return fileinfo.Mode().IsRegular(), nil
}

func (store *FSStore) CheckChunk(checksum string) (bool, error) {
	fileinfo, err := os.Stat(store.PathChunk(checksum))
	if os.IsNotExist(err) {
		return false, nil
	}
	return fileinfo.Mode().IsRegular(), nil

}

func (store *FSStore) Purge(id string) error {
	dest := fmt.Sprintf("%s/%s", store.PathPurge(), id)
	err := os.Rename(store.PathIndex(id), dest)
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

func (store *FSStore) Close() error {
	// XXX - rollback all pending transactions so they don't linger
	store.Tidy()
	return nil
}

func (store *FSStore) Tidy() {
	cwalk.Walk(store.PathObjects(), func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
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
		if err != nil {
			return nil
		}
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
