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
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PlakarLabs/plakar/cache"
	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/google/uuid"
)

type FSRepository struct {
	config storage.RepositoryConfig

	Cache *cache.Cache

	Repository string
	root       string

	//storage.RepositoryBackend
}

type FSTransaction struct {
	Uuid       uuid.UUID
	repository FSRepository
	//prepared   bool

	//storage.TransactionBackend
}

func init() {
	storage.Register("fs", NewFSRepository)
}

func NewFSRepository() storage.RepositoryBackend {
	return &FSRepository{}
}

func (repository *FSRepository) Create(location string, config storage.RepositoryConfig) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("Create(%s): %s", location, time.Since(t0))
	}()

	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	repository.root = location

	err := os.Mkdir(repository.root, 0700)
	if err != nil {
		return err
	}

	os.MkdirAll(filepath.Join(repository.root, "chunks"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "objects"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "blobs"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "transactions"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "snapshots"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "purge"), 0700)

	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			os.MkdirAll(filepath.Join(repository.root, "chunks", fmt.Sprintf("%02x", i)), 0700)
			os.MkdirAll(filepath.Join(repository.root, "objects", fmt.Sprintf("%02x", i)), 0700)
			os.MkdirAll(filepath.Join(repository.root, "blobs", fmt.Sprintf("%02x", i)), 0700)
			os.MkdirAll(filepath.Join(repository.root, "transactions", fmt.Sprintf("%02x", i)), 0700)
			os.MkdirAll(filepath.Join(repository.root, "snapshots", fmt.Sprintf("%02x", i)), 0700)
		}
	}

	configPath := filepath.Join(repository.root, "CONFIG")
	f, err := os.Create(configPath)
	if err != nil {
		return err
	}
	defer f.Close()

	jconfig, err := msgpack.Marshal(config)
	if err != nil {
		return err
	}

	compressedConfig, err := compression.Deflate("gzip", jconfig)
	if err != nil {
		return err
	}

	_, err = f.Write(compressedConfig)
	if err != nil {
		return err
	}

	repository.config = config

	return nil
}

func (repository *FSRepository) Open(location string) error {
	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	repository.root = location

	configPath := filepath.Join(repository.root, "CONFIG")
	compressed, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}

	jconfig, err := compression.Inflate("gzip", compressed)
	if err != nil {
		return err
	}

	config := storage.RepositoryConfig{}
	err = msgpack.Unmarshal(jconfig, &config)
	if err != nil {
		return err
	}

	repository.config = config

	return nil
}

func (repository *FSRepository) Configuration() storage.RepositoryConfig {
	return repository.config
}

func (repository *FSRepository) Transaction(indexID uuid.UUID) (storage.TransactionBackend, error) {
	tx := &FSTransaction{}
	tx.Uuid = indexID
	tx.repository = *repository
	return tx, nil
}

func (repository *FSRepository) GetSnapshots() ([]uuid.UUID, error) {
	ret := make([]uuid.UUID, 0)

	buckets, err := os.ReadDir(repository.PathIndexes())
	if err != nil {
		return ret, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repository.PathIndexes(), bucket.Name())
		indexes, err := os.ReadDir(pathBuckets)
		if err != nil {
			return ret, err
		}
		for _, index := range indexes {
			if index.IsDir() {
				continue
			}
			indexID, err := uuid.Parse(index.Name())
			if err != nil {
				return ret, err
			}
			ret = append(ret, indexID)
		}
	}
	return ret, nil
}

func (repository *FSRepository) GetMetadata(indexID uuid.UUID) ([]byte, error) {
	data, err := os.ReadFile(repository.PathIndex(indexID))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *FSRepository) GetBlob(checksum [32]byte) ([]byte, error) {
	data, err := os.ReadFile(repository.PathBlob(checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *FSRepository) PutMetadata(indexID uuid.UUID, data []byte) error {
	f, err := os.Create(repository.PathIndex(indexID))
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

func (repository *FSRepository) PutBlob(checksum [32]byte, data []byte) error {
	f, err := os.Create(repository.PathBlob(checksum))
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

func (repository *FSRepository) GetObjects() ([][32]byte, error) {
	ret := make([][32]byte, 0)

	buckets, err := os.ReadDir(repository.PathObjects())
	if err != nil {
		return nil, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repository.PathObjects(), bucket.Name())
		objects, err := os.ReadDir(pathBuckets)
		if err != nil {
			return ret, err
		}

		for _, object := range objects {
			if object.IsDir() {
				continue
			}
			t, err := hex.DecodeString(object.Name())
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			var t32 [32]byte
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *FSRepository) PutObject(checksum [32]byte) error {
	file, err := os.Create(repository.PathObject(checksum))
	if err != nil {
		return err
	}
	file.Close()
	return nil
}

func (repository *FSRepository) DeleteObject(checksum [32]byte) error {
	err := os.Remove(repository.PathObject(checksum))
	if err != nil {
		return err
	}
	return nil
}

func (repository *FSRepository) GetChunks() ([][32]byte, error) {
	ret := make([][32]byte, 0)

	buckets, err := os.ReadDir(repository.PathChunks())
	if err != nil {
		return nil, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue // Skip non-directory entries
		}
		pathBuckets := filepath.Join(repository.PathChunks(), bucket.Name())
		chunks, err := os.ReadDir(pathBuckets)
		if err != nil {
			return ret, err
		}

		for _, chunk := range chunks {
			if chunk.IsDir() {
				continue // Skip directory entries, assuming chunks are files
			}
			t, err := hex.DecodeString(chunk.Name())
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			var t32 [32]byte
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *FSRepository) GetChunk(checksum [32]byte) ([]byte, error) {
	data, err := os.ReadFile(repository.PathChunk(checksum))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *FSRepository) PutChunk(checksum [32]byte, data []byte) error {
	f, err := os.CreateTemp(repository.PathChunkBucket(checksum), fmt.Sprintf("%064x.*", checksum))
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}

	name := f.Name()

	err = f.Close()
	if err != nil {
		return err
	}

	err = os.Rename(name, repository.PathChunk(checksum))
	if err != nil {
		return err
	}

	return nil
}

func (repository *FSRepository) DeleteChunk(checksum [32]byte) error {
	err := os.Remove(repository.PathChunk(checksum))
	if err != nil {
		return err
	}
	return nil
}

func (repository *FSRepository) CheckObject(checksum [32]byte) (bool, error) {
	fileinfo, err := os.Stat(repository.PathObject(checksum))
	if os.IsNotExist(err) {
		return false, nil
	}
	return fileinfo.Mode().IsRegular(), nil
}

func (repository *FSRepository) CheckChunk(checksum [32]byte) (bool, error) {
	fileinfo, err := os.Stat(repository.PathChunk(checksum))
	if os.IsNotExist(err) {
		return false, nil
	}
	return fileinfo.Mode().IsRegular(), nil

}

func (repository *FSRepository) Purge(indexID uuid.UUID) error {
	dest := filepath.Join(repository.PathPurge(), indexID.String())
	err := os.Rename(repository.PathIndex(indexID), dest)
	if err != nil {
		return err
	}

	err = os.RemoveAll(dest)
	if err != nil {
		return err
	}
	return nil
}

func (repository *FSRepository) Close() error {
	return nil
}

func (transaction *FSTransaction) GetUuid() uuid.UUID {
	return transaction.Uuid
}

func (transaction *FSTransaction) PutMetadata(data []byte) error {
	f, err := os.Create(transaction.Path())
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

func (transaction *FSTransaction) Commit() error {
	return os.Rename(transaction.Path(), transaction.repository.PathIndex(transaction.Uuid))
}
