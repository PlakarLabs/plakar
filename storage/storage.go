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

package storage

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/chunking"
	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/locking"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/google/uuid"
)

const VERSION string = "0.6.0"

type Configuration struct {
	Version      string
	Timestamp    time.Time
	RepositoryID uuid.UUID

	Packfile    packfile.Configuration
	Chunking    chunking.Configuration
	Hashing     hashing.Configuration
	Compression *compression.Configuration
	Encryption  *encryption.Configuration
}

func NewConfiguration() *Configuration {
	return &Configuration{
		Version:      VERSION,
		Timestamp:    time.Now(),
		RepositoryID: uuid.Must(uuid.NewRandom()),

		Packfile: *packfile.DefaultConfiguration(),
		Chunking: *chunking.DefaultConfiguration(),
		Hashing:  *hashing.DefaultConfiguration(),

		Compression: compression.DefaultConfiguration(),
		Encryption:  encryption.DefaultConfiguration(),
	}
}

type Backend interface {
	Create(repository string, configuration Configuration) error
	Open(repository string) error
	Configuration() Configuration

	GetStates() ([][32]byte, error)
	PutState(checksum [32]byte, rd io.Reader, size uint64) error
	GetState(checksum [32]byte) (io.Reader, uint64, error)
	DeleteState(checksum [32]byte) error

	GetPackfiles() ([][32]byte, error)
	PutPackfile(checksum [32]byte, rd io.Reader, size uint64) error
	GetPackfile(checksum [32]byte) (io.Reader, uint64, error)
	GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) (io.Reader, uint32, error)
	DeletePackfile(checksum [32]byte) error

	Close() error
}

var muBackends sync.Mutex
var backends map[string]func() Backend = make(map[string]func() Backend)

type Store struct {
	backend  Backend
	context  *context.Context
	location string

	wBytes uint64
	rBytes uint64

	writeSharedLock *locking.SharedLock
	readSharedLock  *locking.SharedLock

	bufferedPackfiles chan struct{}
}

func NewStore(ctx *context.Context, name string, location string) (*Store, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if backend, exists := backends[name]; !exists {
		return nil, fmt.Errorf("backend '%s' does not exist", name)
	} else {
		store := &Store{}
		store.context = ctx
		store.backend = backend()
		store.location = location
		store.writeSharedLock = locking.NewSharedLock("store.write", ctx.GetMaxConcurrency())
		store.readSharedLock = locking.NewSharedLock("store.read", ctx.GetMaxConcurrency())
		store.bufferedPackfiles = make(chan struct{}, ctx.GetMaxConcurrency()/4)
		return store, nil
	}
}

func Register(name string, backend func() Backend) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if _, ok := backends[name]; ok {
		log.Fatalf("backend '%s' registered twice", name)
	}
	backends[name] = backend
}

func Backends() []string {
	muBackends.Lock()
	defer muBackends.Unlock()

	ret := make([]string, 0)
	for backendName := range backends {
		ret = append(ret, backendName)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

func New(ctx *context.Context, location string) (*Store, error) {
	backendName := "fs"
	if !strings.HasPrefix(location, "/") {
		if strings.HasPrefix(location, "tcp://") || strings.HasPrefix(location, "ssh://") || strings.HasPrefix(location, "stdio://") {
			backendName = "plakard"
		} else if strings.HasPrefix(location, "http://") || strings.HasPrefix(location, "https://") {
			backendName = "http"
		} else if strings.HasPrefix(location, "sqlite://") {
			backendName = "database"
		} else if strings.HasPrefix(location, "s3://") {
			backendName = "s3"
		} else if strings.HasPrefix(location, "null://") {
			backendName = "null"
		} else if strings.HasPrefix(location, "fs://") {
			backendName = "fs"
		} else if strings.Contains(location, "://") {
			return nil, fmt.Errorf("unsupported plakar protocol")
		}
	}

	if backendName == "fs" && !strings.HasPrefix(location, "/") {
		if !strings.HasPrefix(location, "fs://") {
			tmp, err := filepath.Abs(location)
			if err != nil {
				return nil, err
			}
			location = tmp
		}
	}
	return NewStore(ctx, backendName, location)
}

func Open(ctx *context.Context, location string) (*Store, error) {
	store, err := New(ctx, location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "Open(%s): %s", location, time.Since(t0))
	}()

	if err = store.backend.Open(location); err != nil {
		return nil, err
	} else {
		return store, nil
	}
}

func Create(ctx *context.Context, location string, configuration Configuration) (*Store, error) {
	store, err := New(ctx, location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "Create(%s): %s", location, time.Since(t0))
	}()

	if err = store.backend.Create(location, configuration); err != nil {
		return nil, err
	} else {
		return store, nil
	}
}

func (store *Store) Context() *context.Context {
	return store.context
}

func (store *Store) Location() string {
	return store.location
}

func (store *Store) GetRBytes() uint64 {
	return atomic.LoadUint64(&store.rBytes)
}

func (store *Store) GetWBytes() uint64 {
	return atomic.LoadUint64(&store.wBytes)
}

func (store *Store) Configuration() Configuration {
	return store.backend.Configuration()
}

/* Packfiles */
func (store *Store) GetPackfiles() ([]objects.Checksum, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "GetPackfiles(): %s", time.Since(t0))
	}()

	checksums, err := store.backend.GetPackfiles()
	ret := make([]objects.Checksum, 0, len(checksums))
	for _, checksum := range checksums {
		ret = append(ret, objects.Checksum(checksum))
	}
	return ret, err
}

func (store *Store) GetPackfile(checksum objects.Checksum) (io.Reader, uint64, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "GetPackfile(%016x): %s", checksum, time.Since(t0))
	}()

	rd, datalen, err := store.backend.GetPackfile(checksum)
	if err != nil {
		return nil, 0, err
	}
	atomic.AddUint64(&store.rBytes, uint64(datalen))
	return rd, datalen, nil
}

func (store *Store) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, uint32, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "GetPackfileBlob(%016x, %d, %d): %s", checksum, offset, length, time.Since(t0))
	}()

	rd, datalen, err := store.backend.GetPackfileBlob(checksum, offset, length)
	if err != nil {
		return nil, 0, err
	}
	atomic.AddUint64(&store.rBytes, uint64(datalen))
	return rd, datalen, nil
}

func (store *Store) PutPackfile(checksum objects.Checksum, rd io.Reader, size uint64) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "PutPackfile(%016x): %s", checksum, time.Since(t0))
	}()

	store.bufferedPackfiles <- struct{}{}
	defer func() { <-store.bufferedPackfiles }()

	atomic.AddUint64(&store.wBytes, uint64(size))
	return store.backend.PutPackfile(checksum, rd, size)
}

func (store *Store) DeletePackfile(checksum objects.Checksum) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "DeletePackfile(%064x): %s", checksum, time.Since(t0))
	}()
	return store.backend.DeletePackfile(checksum)
}

/* Indexes */
func (store *Store) GetStates() ([]objects.Checksum, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "GetStates(): %s", time.Since(t0))
	}()

	checksums, err := store.backend.GetStates()
	ret := make([]objects.Checksum, 0, len(checksums))
	for _, checksum := range checksums {
		ret = append(ret, objects.Checksum(checksum))
	}
	return ret, err
}

func (store *Store) PutState(checksum objects.Checksum, rd io.Reader, size uint64) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "PutState(%016x): %s", checksum, time.Since(t0))
	}()

	err := store.backend.PutState(checksum, rd, size)
	if err != nil {
		return err
	}
	return err
}

func (store *Store) GetState(checksum objects.Checksum) (io.Reader, uint64, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "GetState(%016x): %s", checksum, time.Since(t0))
	}()

	rd, size, err := store.backend.GetState(checksum)
	if err != nil {
		return nil, 0, err
	}
	return rd, size, nil
}

func (store *Store) DeleteState(checksum objects.Checksum) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "DeleteState(%064x): %s", checksum, time.Since(t0))
	}()
	return store.backend.DeleteState(checksum)
}

func (store *Store) Close() error {
	t0 := time.Now()
	defer func() {
		store.Logger().Trace("store", "Close(): %s", time.Since(t0))
	}()
	return store.backend.Close()
}

func (store *Store) Logger() *logging.Logger {
	return store.Context().GetLogger()
}
