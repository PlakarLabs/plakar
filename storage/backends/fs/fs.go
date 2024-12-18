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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type Repository struct {
	config storage.Configuration

	Repository string
	root       string
	location   string
}

type LimitedReaderWithClose struct {
	*io.LimitedReader
	file *os.File
}

func (l *LimitedReaderWithClose) Read(p []byte) (int, error) {
	n, err := l.LimitedReader.Read(p)
	if err == io.EOF {
		// Close the file when EOF is reached
		closeErr := l.file.Close()
		if closeErr != nil {
			return n, fmt.Errorf("error closing file: %w", closeErr)
		}
	}
	return n, err
}

func init() {
	storage.Register("fs", NewRepository)
}

func NewRepository(location string) storage.Store {
	return &Repository{
		location: location,
	}
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repo *Repository) Create(location string, config storage.Configuration) error {
	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	repo.root = location

	err := os.Mkdir(repo.root, 0700)
	if err != nil {
		return err
	}

	os.MkdirAll(filepath.Join(repo.root, "states"), 0700)
	os.MkdirAll(filepath.Join(repo.root, "packfiles"), 0700)
	os.MkdirAll(filepath.Join(repo.root, "tmp"), 0700)

	for i := 0; i < 256; i++ {
		os.MkdirAll(filepath.Join(repo.root, "states", fmt.Sprintf("%02x", i)), 0700)
		os.MkdirAll(filepath.Join(repo.root, "packfiles", fmt.Sprintf("%02x", i)), 0700)
	}

	configPath := filepath.Join(repo.root, "CONFIG")
	tmpfile := filepath.Join(repo.PathTmp(), "CONFIG")

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

	repo.config = config

	return os.Rename(tmpfile, configPath)
}

func (repo *Repository) Open(location string) error {
	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	repo.root = location

	configPath := filepath.Join(repo.root, "CONFIG")
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

	repo.config = config

	return nil
}

func (repo *Repository) Configuration() storage.Configuration {
	return repo.config
}

func (repo *Repository) GetPackfiles() ([]objects.Checksum, error) {
	ret := make([]objects.Checksum, 0)

	buckets, err := os.ReadDir(repo.PathPackfiles())
	if err != nil {
		return ret, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repo.PathPackfiles(), bucket.Name())
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
			var t32 objects.Checksum
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repo *Repository) GetPackfile(checksum objects.Checksum) (io.Reader, error) {
	pathname := repo.PathPackfile(checksum)
	if !strings.HasPrefix(pathname, repo.PathPackfiles()) {
		return nil, fmt.Errorf("invalid path generated from checksum")
	}

	fp, err := os.Open(pathname)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = repository.ErrPackfileNotFound
		}
		return nil, err
	}

	return fp, nil
}

func (repo *Repository) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, error) {
	pathname := repo.PathPackfile(checksum)
	if !strings.HasPrefix(pathname, repo.PathPackfiles()) {
		return nil, fmt.Errorf("invalid path generated from checksum")
	}

	fp, err := os.Open(pathname)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = repository.ErrBlobNotFound
		}
		return nil, err
	}

	if _, err := fp.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	st, err := fp.Stat()
	if err != nil {
		return nil, err
	}

	if st.Size() == 0 {
		return bytes.NewBuffer([]byte{}), nil
	}

	if length > (uint32(st.Size()) - offset) {
		return nil, fmt.Errorf("invalid length")
	}

	return &LimitedReaderWithClose{
		LimitedReader: &io.LimitedReader{
			R: fp,
			N: int64(length),
		},
		file: fp,
	}, nil
}

func (repo *Repository) DeletePackfile(checksum objects.Checksum) error {
	pathname := repo.PathPackfile(checksum)
	if !strings.HasPrefix(pathname, repo.PathPackfiles()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	err := os.Remove(pathname)
	if err != nil {
		return err
	}
	return nil
}

func (repo *Repository) PutPackfile(checksum objects.Checksum, rd io.Reader) error {
	tmpfile := filepath.Join(repo.PathTmp(), hex.EncodeToString(checksum[:]))
	if !strings.HasPrefix(tmpfile, repo.PathTmp()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	pathname := repo.PathPackfile(checksum)
	if !strings.HasPrefix(pathname, repo.PathPackfiles()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	f, err := os.Create(tmpfile)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rd); err != nil {
		return err
	}

	return os.Rename(tmpfile, pathname)
}

func (repo *Repository) Close() error {
	return nil
}

/* Indexes */
func (repo *Repository) GetStates() ([]objects.Checksum, error) {
	ret := make([]objects.Checksum, 0)

	buckets, err := os.ReadDir(repo.PathStates())
	if err != nil {
		return ret, err
	}

	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		pathBuckets := filepath.Join(repo.PathStates(), bucket.Name())
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
			var t32 objects.Checksum
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repo *Repository) PutState(checksum objects.Checksum, rd io.Reader) error {
	tmpfile := filepath.Join(repo.PathTmp(), hex.EncodeToString(checksum[:]))
	if !strings.HasPrefix(tmpfile, repo.PathTmp()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	pathname := repo.PathState(checksum)
	if !strings.HasPrefix(pathname, repo.PathStates()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	f, err := os.Create(tmpfile)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, rd)
	if err != nil {
		return err
	}
	return os.Rename(tmpfile, pathname)
}

func (repo *Repository) GetState(checksum objects.Checksum) (io.Reader, error) {
	pathname := repo.PathState(checksum)
	if !strings.HasPrefix(pathname, repo.PathStates()) {
		return nil, fmt.Errorf("invalid path generated from checksum")
	}

	return os.Open(pathname)
}

func (repo *Repository) DeleteState(checksum objects.Checksum) error {
	pathname := repo.PathState(checksum)
	if !strings.HasPrefix(pathname, repo.PathStates()) {
		return fmt.Errorf("invalid path generated from checksum")
	}

	err := os.Remove(pathname)
	if err != nil {
		return err
	}
	return nil
}
