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
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type Repository struct {
	config storage.Configuration

	Repository string
	root       string
}

func init() {
	storage.Register("fs", NewRepository)
}

func NewRepository() storage.Backend {
	return &Repository{}
}

func (repository *Repository) Create(location string, config storage.Configuration) error {
	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	repository.root = location

	err := os.Mkdir(repository.root, 0700)
	if err != nil {
		return err
	}

	os.MkdirAll(filepath.Join(repository.root, "states"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "packfiles"), 0700)
	os.MkdirAll(filepath.Join(repository.root, "tmp"), 0700)

	for i := 0; i < 256; i++ {
		os.MkdirAll(filepath.Join(repository.root, "states", fmt.Sprintf("%02x", i)), 0700)
		os.MkdirAll(filepath.Join(repository.root, "packfiles", fmt.Sprintf("%02x", i)), 0700)
	}

	configPath := filepath.Join(repository.root, "CONFIG")
	tmpfile := filepath.Join(repository.PathTmp(), "CONFIG")

	f, err := os.Create(tmpfile)
	if err != nil {
		return err
	}
	defer f.Close()

	jconfig, err := msgpack.Marshal(config)
	if err != nil {
		return err
	}

	compressedConfig, err := compression.DeflateStream("GZIP", bytes.NewReader(jconfig))
	if err != nil {
		return err
	}

	_, err = io.Copy(f, compressedConfig)
	if err != nil {
		return err
	}

	repository.config = config

	return os.Rename(tmpfile, configPath)
}

func (repository *Repository) Open(location string) error {
	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	repository.root = location

	configPath := filepath.Join(repository.root, "CONFIG")
	rd, err := os.Open(configPath)
	if err != nil {
		return err
	}

	jconfig, err := compression.InflateStream("GZIP", rd)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(jconfig)
	if err != nil {
		return err
	}

	config := storage.Configuration{}
	err = msgpack.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	repository.config = config

	return nil
}

func (repository *Repository) Configuration() storage.Configuration {
	return repository.config
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

func (repository *Repository) GetPackfile(checksum [32]byte) (io.Reader, uint64, error) {
	pathname := repository.PathPackfile(checksum)
	if !strings.HasPrefix(pathname, repository.PathPackfiles()) {
		return nil, 0, fmt.Errorf("invalid path generated from checksum")
	}

	fp, err := os.Open(pathname)
	if err != nil {
		return nil, 0, err
	}
	info, err := fp.Stat()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, uint64(info.Size()), nil
}

func (repository *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) (io.Reader, uint32, error) {
	pathname := repository.PathPackfile(checksum)
	if !strings.HasPrefix(pathname, repository.PathPackfiles()) {
		return nil, 0, fmt.Errorf("invalid path generated from checksum")
	}

	fp, err := os.Open(pathname)
	if err != nil {
		return nil, 0, err
	}
	defer fp.Close()

	if _, err := fp.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, 0, err
	}

	st, err := fp.Stat()
	if err != nil {
		return nil, 0, err
	}

	if st.Size() == 0 {
		return bytes.NewBuffer([]byte{}), 0, nil
	}

	if length > (uint32(st.Size()) - offset) {
		return nil, 0, fmt.Errorf("invalid length")
	}

	data := make([]byte, length)
	if _, err := fp.Read(data); err != nil {
		return nil, 0, err
	}
	return bytes.NewBuffer(data), uint32(len(data)), nil
}

func (repository *Repository) DeletePackfile(checksum [32]byte) error {
	pathname := repository.PathPackfile(checksum)
	if !strings.HasPrefix(pathname, repository.PathPackfiles()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	err := os.Remove(pathname)
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) PutPackfile(checksum [32]byte, rd io.Reader, size uint64) error {
	tmpfile := filepath.Join(repository.PathTmp(), hex.EncodeToString(checksum[:]))
	if !strings.HasPrefix(tmpfile, repository.PathTmp()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	pathname := repository.PathPackfile(checksum)
	if !strings.HasPrefix(pathname, repository.PathPackfiles()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	f, err := os.Create(tmpfile)
	if err != nil {
		return err
	}
	defer f.Close()

	if n, err := io.Copy(f, rd); err != nil {
		return err
	} else if uint64(n) != size {
		return fmt.Errorf("short write")
	}

	return os.Rename(tmpfile, pathname)
}

func (repository *Repository) Close() error {
	return nil
}

/* Indexes */
func (repository *Repository) GetStates() ([][32]byte, error) {
	ret := make([][32]byte, 0)

	buckets, err := os.ReadDir(repository.PathStates())
	if err != nil {
		return ret, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repository.PathStates(), bucket.Name())
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

func (repository *Repository) PutState(checksum [32]byte, rd io.Reader, size uint64) error {
	tmpfile := filepath.Join(repository.PathTmp(), hex.EncodeToString(checksum[:]))
	if !strings.HasPrefix(tmpfile, repository.PathTmp()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	pathname := repository.PathState(checksum)
	if !strings.HasPrefix(pathname, repository.PathStates()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	f, err := os.Create(tmpfile)
	if err != nil {
		return err
	}
	defer f.Close()

	w, err := io.Copy(f, rd)
	if err != nil {
		return err
	} else if uint64(w) != size {
		return fmt.Errorf("short write")
	}
	return os.Rename(tmpfile, pathname)
}

func (repository *Repository) GetState(checksum [32]byte) (io.Reader, uint64, error) {
	pathname := repository.PathState(checksum)
	if !strings.HasPrefix(pathname, repository.PathStates()) {
		return nil, 0, fmt.Errorf("invalid path generated from checksum")
	}

	fp, err := os.Open(pathname)
	if err != nil {
		return nil, 0, err
	}
	info, err := fp.Stat()
	if err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, uint64(info.Size()), nil
}

func (repository *Repository) DeleteState(checksum [32]byte) error {
	pathname := repository.PathState(checksum)
	if !strings.HasPrefix(pathname, repository.PathStates()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	err := os.Remove(pathname)
	if err != nil {
		return err
	}
	return nil
}
