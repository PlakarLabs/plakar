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

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/google/uuid"
)

type Repository struct {
	config storage.RepositoryConfig

	Repository string
	root       string
}

func init() {
	storage.Register("fs", NewRepository)
}

func NewRepository() storage.RepositoryBackend {
	return &Repository{}
}

func (repository *Repository) Create(location string, config storage.RepositoryConfig) error {
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

	os.MkdirAll(filepath.Join(repository.root, "indexes"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "locks"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "blobs"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "packfiles"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "snapshots"), 0700)

	os.MkdirAll(filepath.Join(repository.root, "tmp"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "purge"), 0700)

	for i := 0; i < 256; i++ {
		os.MkdirAll(filepath.Join(repository.root, "indexes", fmt.Sprintf("%02x", i)), 0700)
		os.MkdirAll(filepath.Join(repository.root, "blobs", fmt.Sprintf("%02x", i)), 0700)
		os.MkdirAll(filepath.Join(repository.root, "packfiles", fmt.Sprintf("%02x", i)), 0700)
		os.MkdirAll(filepath.Join(repository.root, "snapshots", fmt.Sprintf("%02x", i)), 0700)
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

func (repository *Repository) Open(location string) error {
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

func (repository *Repository) Configuration() storage.RepositoryConfig {
	return repository.config
}

func (repository *Repository) GetSnapshots() ([]uuid.UUID, error) {
	ret := make([]uuid.UUID, 0)

	buckets, err := os.ReadDir(repository.PathSnapshots())
	if err != nil {
		return ret, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repository.PathSnapshots(), bucket.Name())
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

func (repository *Repository) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	data, err := os.ReadFile(repository.PathSnapshot(indexID))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *Repository) GetBlobs() ([][32]byte, error) {
	ret := make([][32]byte, 0)

	buckets, err := os.ReadDir(repository.PathBlobs())
	if err != nil {
		return ret, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repository.PathBlobs(), bucket.Name())
		blobs, err := os.ReadDir(pathBuckets)
		if err != nil {
			return ret, err
		}
		for _, blob := range blobs {
			if blob.IsDir() {
				continue
			}
			t, err := hex.DecodeString(blob.Name())
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

func (repository *Repository) GetPackfiles() ([][32]byte, error) {
	ret := make([][32]byte, 0)

	buckets, err := os.ReadDir(repository.PathPackfiles())
	if err != nil {
		return ret, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repository.PathPackfiles(), bucket.Name())
		packfiles, err := os.ReadDir(pathBuckets)
		if err != nil {
			return ret, err
		}
		for _, packfile := range packfiles {
			if packfile.IsDir() {
				continue
			}
			t, err := hex.DecodeString(packfile.Name())
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

func (repository *Repository) GetBlob(checksum [32]byte) ([]byte, error) {
	data, err := os.ReadFile(repository.PathBlob(checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *Repository) DeleteBlob(checksum [32]byte) error {
	err := os.Remove(repository.PathBlob(checksum))
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetPackfile(checksum [32]byte) ([]byte, error) {
	data, err := os.ReadFile(repository.PathPackfile(checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *Repository) DeletePackfile(checksum [32]byte) error {
	err := os.Remove(repository.PathPackfile(checksum))
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) PutSnapshot(indexID uuid.UUID, data []byte) error {
	f, err := os.Create(repository.PathSnapshot(indexID))
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

func (repository *Repository) PutBlob(checksum [32]byte, data []byte) error {
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

func (repository *Repository) PutPackfile(checksum [32]byte, data []byte) error {
	f, err := os.Create(repository.PathPackfile(checksum))
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

func (repository *Repository) DeleteSnapshot(indexID uuid.UUID) error {
	dest := filepath.Join(repository.PathPurge(), indexID.String())
	err := os.Rename(repository.PathSnapshot(indexID), dest)
	if err != nil {
		return err
	}

	err = os.RemoveAll(dest)
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) Close() error {
	return nil
}

/* Indexes */
func (repository *Repository) GetIndexes() ([][32]byte, error) {
	ret := make([][32]byte, 0)

	buckets, err := os.ReadDir(repository.PathIndexes())
	if err != nil {
		return ret, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repository.PathIndexes(), bucket.Name())
		blobs, err := os.ReadDir(pathBuckets)
		if err != nil {
			return ret, err
		}
		for _, blob := range blobs {
			if blob.IsDir() {
				continue
			}
			t, err := hex.DecodeString(blob.Name())
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

func (repository *Repository) PutIndex(checksum [32]byte, data []byte) error {
	f, err := os.Create(repository.PathIndex(checksum))
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

func (repository *Repository) GetIndex(checksum [32]byte) ([]byte, error) {
	data, err := os.ReadFile(repository.PathIndex(checksum))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *Repository) DeleteIndex(checksum [32]byte) error {
	err := os.Remove(repository.PathIndex(checksum))
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) Commit(indexID uuid.UUID, data []byte) error {
	f, err := os.CreateTemp(repository.PathTmp(), fmt.Sprintf("%s.*", indexID))
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

	err = os.Rename(name, repository.PathSnapshot(indexID))
	if err != nil {
		return err
	}

	return nil
}

func (repository *Repository) GetLocks() ([]uuid.UUID, error) {
	ret := make([]uuid.UUID, 0)

	locksdir, err := os.ReadDir(repository.PathLocks())
	if err != nil {
		return ret, err
	}

	for _, lock := range locksdir {
		if lock.IsDir() {
			continue
		}
		indexID, err := uuid.Parse(lock.Name())
		if err != nil {
			return ret, err
		}
		ret = append(ret, indexID)

	}
	return ret, nil
}

func (repository *Repository) GetLock(indexID uuid.UUID) ([]byte, error) {
	data, err := os.ReadFile(repository.PathLock(indexID))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repository *Repository) PutLock(indexID uuid.UUID, data []byte) error {
	f, err := os.Create(repository.PathLock(indexID))
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

func (repository *Repository) DeleteLock(indexID uuid.UUID) error {
	dest := filepath.Join(repository.PathPurge(), indexID.String())
	err := os.Rename(repository.PathLock(indexID), dest)
	if err != nil {
		return err
	}

	err = os.RemoveAll(dest)
	if err != nil {
		return err
	}
	return nil
}
