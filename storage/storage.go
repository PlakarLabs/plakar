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
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarLabs/plakar/chunking"
	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/hashing"
	"github.com/PlakarLabs/plakar/locking"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/packfile"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/google/uuid"
)

const VERSION string = "0.6.0"

type Configuration struct {
	Version      string
	CreationTime time.Time

	StoreID uuid.UUID

	Encryption    string
	EncryptionKey string

	Compression string

	Hashing string

	Chunking       string
	ChunkingMin    int
	ChunkingNormal int
	ChunkingMax    int

	PackfileSize uint32
}

func NewConfiguration() *Configuration {
	return &Configuration{
		Version:        VERSION,
		StoreID:        uuid.Must(uuid.NewRandom()),
		CreationTime:   time.Now(),
		Compression:    compression.DefaultAlgorithm(),
		Hashing:        hashing.DefaultAlgorithm(),
		Chunking:       chunking.DefaultAlgorithm(),
		ChunkingMin:    chunking.DefaultConfiguration().MinSize,
		ChunkingNormal: chunking.DefaultConfiguration().NormalSize,
		ChunkingMax:    chunking.DefaultConfiguration().MaxSize,
		PackfileSize:   packfile.DefaultConfiguration().MaxSize,
	}
}

type Backend interface {
	Create(repository string, configuration Configuration) error
	Open(repository string) error
	Configuration() Configuration

	GetSnapshots() ([][32]byte, error)
	PutSnapshot(snapshotID [32]byte, data []byte) error
	GetSnapshot(snapshotID [32]byte) ([]byte, error)
	DeleteSnapshot(snapshotID [32]byte) error

	GetStates() ([][32]byte, error)
	PutState(checksum [32]byte, data []byte) error
	GetState(checksum [32]byte) ([]byte, error)
	DeleteState(checksum [32]byte) error

	GetPackfiles() ([][32]byte, error)
	PutPackfile(checksum [32]byte, data []byte) error
	GetPackfile(checksum [32]byte) ([]byte, error)
	GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) ([]byte, error)
	DeletePackfile(checksum [32]byte) error

	Commit(snapshotID [32]byte, data []byte) error

	Close() error
}

var muBackends sync.Mutex
var backends map[string]func() Backend = make(map[string]func() Backend)

type Store struct {
	backend Backend

	Location    string
	Username    string
	Hostname    string
	CommandLine string
	MachineID   string

	wBytes uint64
	rBytes uint64

	writeSharedLock *locking.SharedLock
	readSharedLock  *locking.SharedLock

	bufferedPackfiles chan struct{}
}

func NewStore(name string, location string) (*Store, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if backend, exists := backends[name]; !exists {
		return nil, fmt.Errorf("backend '%s' does not exist", name)
	} else {
		store := &Store{}
		store.Location = location
		store.backend = backend()
		store.writeSharedLock = locking.NewSharedLock("store.write", runtime.NumCPU()*8+1)
		store.readSharedLock = locking.NewSharedLock("store.read", runtime.NumCPU()*8+1)
		store.bufferedPackfiles = make(chan struct{}, runtime.NumCPU()*2+1)
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

func New(location string) (*Store, error) {
	backendName := "fs"
	if !strings.HasPrefix(location, "/") {
		if strings.HasPrefix(location, "plakar://") || strings.HasPrefix(location, "ssh://") || strings.HasPrefix(location, "stdio://") {
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
	return NewStore(backendName, location)
}

func Open(location string) (*Store, error) {
	store, err := New(location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.Open", time.Since(t0))
		logger.Trace("store", "Open(%s): %s", location, time.Since(t0))
	}()

	if err = store.backend.Open(location); err != nil {
		return nil, err
	} else {
		return store, nil
	}
}

func Create(location string, configuration Configuration) (*Store, error) {
	store, err := New(location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.Create", time.Since(t0))
		logger.Trace("store", "Create(%s): %s", location, time.Since(t0))
	}()

	if err = store.backend.Create(location, configuration); err != nil {
		return nil, err
	} else {
		return store, nil
	}
}

func (store *Store) GetRBytes() uint64 {
	return atomic.LoadUint64(&store.rBytes)
}

func (store *Store) GetWBytes() uint64 {
	return atomic.LoadUint64(&store.wBytes)
}

func (store *Store) GetUsername() string {
	return store.Username
}

func (store *Store) GetHostname() string {
	return store.Hostname
}

func (store *Store) GetCommandLine() string {
	return store.CommandLine
}

func (store *Store) GetMachineID() string {
	return store.MachineID
}

func (store *Store) SetUsername(username string) error {
	store.Username = username
	return nil
}

func (store *Store) SetHostname(hostname string) error {
	store.Hostname = hostname
	return nil
}

func (store *Store) SetCommandLine(commandLine string) error {
	store.CommandLine = commandLine
	return nil
}

func (store *Store) SetMachineID(machineID string) error {
	store.MachineID = machineID
	return nil
}

func (store *Store) Configuration() Configuration {
	return store.backend.Configuration()
}

/* snapshots  */
func (store *Store) GetSnapshots() ([][32]byte, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetSnapshots", time.Since(t0))
		logger.Trace("store", "GetSnapshots(): %s", time.Since(t0))
	}()
	return store.backend.GetSnapshots()
}

func (store *Store) PutSnapshot(snapshotID [32]byte, data []byte) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.PutSnapshot", time.Since(t0))
		logger.Trace("store", "PutSnapshot(%s): %s", snapshotID, time.Since(t0))
	}()

	atomic.AddUint64(&store.wBytes, uint64(len(data)))
	return store.backend.PutSnapshot(snapshotID, data)
}

func (store *Store) GetSnapshot(snapshotID [32]byte) ([]byte, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetSnapshot", time.Since(t0))
		logger.Trace("store", "GetSnapshot(%s): %s", fmt.Sprintf("%x", snapshotID), time.Since(t0))
	}()

	data, err := store.backend.GetSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&store.rBytes, uint64(len(data)))

	return data, nil
}

func (store *Store) DeleteSnapshot(snapshotID [32]byte) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.DeleteSnapshot", time.Since(t0))
		logger.Trace("store", "DeleteSnapshot(%s): %s", snapshotID, time.Since(t0))
	}()
	return store.backend.DeleteSnapshot(snapshotID)
}

/* Packfiles */
func (store *Store) GetPackfiles() ([][32]byte, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetPackfiles", time.Since(t0))
		logger.Trace("store", "GetPackfiles(): %s", time.Since(t0))
	}()
	return store.backend.GetPackfiles()
}

func (store *Store) GetPackfile(checksum [32]byte) ([]byte, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetPackfile", time.Since(t0))
		logger.Trace("store", "GetPackfile(%016x): %s", checksum, time.Since(t0))
	}()

	data, err := store.backend.GetPackfile(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&store.rBytes, uint64(len(data)))
	return data, nil
}

func (store *Store) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) ([]byte, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetPackfileBlob", time.Since(t0))
		logger.Trace("store", "GetPackfileBlob(%016x, %d, %d): %s", checksum, offset, length, time.Since(t0))
	}()

	data, err := store.backend.GetPackfileBlob(checksum, offset, length)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&store.rBytes, uint64(len(data)))
	return data, nil
}

func (store *Store) PutPackfile(checksum [32]byte, data []byte) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.PutPackfile", time.Since(t0))
		logger.Trace("store", "PutPackfile(%016x): %s", checksum, time.Since(t0))
	}()

	store.bufferedPackfiles <- struct{}{}
	defer func() { <-store.bufferedPackfiles }()

	atomic.AddUint64(&store.wBytes, uint64(len(data)))
	return store.backend.PutPackfile(checksum, data)
}

func (store *Store) DeletePackfile(checksum [32]byte) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.DeletePackfile", time.Since(t0))
		logger.Trace("store", "DeletePackfile(%064x): %s", checksum, time.Since(t0))
	}()
	return store.backend.DeletePackfile(checksum)
}

/* Indexes */
func (store *Store) GetStates() ([][32]byte, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetIndexes", time.Since(t0))
		logger.Trace("store", "GetIndexes(): %s", time.Since(t0))
	}()
	return store.backend.GetStates()
}

func (store *Store) PutState(checksum [32]byte, data []byte) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.PutIndex", time.Since(t0))
		logger.Trace("store", "PutIndex(%016x): %s", checksum, time.Since(t0))
	}()
	atomic.AddUint64(&store.wBytes, uint64(len(data)))
	return store.backend.PutState(checksum, data)
}

func (store *Store) GetState(checksum [32]byte) ([]byte, error) {
	store.readSharedLock.Lock()
	defer store.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetState", time.Since(t0))
		logger.Trace("store", "GetState(%016x): %s", checksum, time.Since(t0))
	}()

	data, err := store.backend.GetState(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&store.rBytes, uint64(len(data)))
	return data, nil
}

func (store *Store) DeleteState(checksum [32]byte) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.DeleteIndex", time.Since(t0))
		logger.Trace("store", "DeleteIndex(%064x): %s", checksum, time.Since(t0))
	}()
	return store.backend.DeleteState(checksum)
}

func (store *Store) Close() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.Close", time.Since(t0))
		logger.Trace("store", "Close(): %s", time.Since(t0))
	}()
	return store.backend.Close()
}

func (store *Store) Commit(snapshotID [32]byte, data []byte) error {
	store.writeSharedLock.Lock()
	defer store.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.Commit", time.Since(t0))
		logger.Trace("store", "Commit(%s): %s", fmt.Sprintf("%x", snapshotID), time.Since(t0))
	}()
	atomic.AddUint64(&store.wBytes, uint64(len(data)))

	return store.backend.Commit(snapshotID, data)
}
