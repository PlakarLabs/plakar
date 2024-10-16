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

	"github.com/PlakarLabs/plakar/cache"
	"github.com/PlakarLabs/plakar/locking"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage/state"
	"github.com/google/uuid"
)

const VERSION string = "0.5.0"

type Configuration struct {
	CreationTime time.Time
	RepositoryID uuid.UUID

	Version string

	Encryption    string
	EncryptionKey string

	Compression string

	Hashing string

	Chunking       string
	ChunkingMin    int
	ChunkingNormal int
	ChunkingMax    int

	PackfileSize int
}

type Backend interface {
	Create(repository string, configuration Configuration) error
	Open(repository string) error
	Configuration() Configuration

	GetSnapshots() ([]uuid.UUID, error)
	PutSnapshot(indexID uuid.UUID, data []byte) error
	GetSnapshot(indexID uuid.UUID) ([]byte, error)
	DeleteSnapshot(indexID uuid.UUID) error

	GetBlobs() ([][32]byte, error)
	PutBlob(checksum [32]byte, data []byte) error
	CheckBlob(checksum [32]byte) (bool, error)
	GetBlob(checksum [32]byte) ([]byte, error)
	DeleteBlob(checksum [32]byte) error

	GetStates() ([][32]byte, error)
	PutState(checksum [32]byte, data []byte) error
	GetState(checksum [32]byte) ([]byte, error)
	DeleteState(checksum [32]byte) error

	GetPackfiles() ([][32]byte, error)
	PutPackfile(checksum [32]byte, data []byte) error
	GetPackfile(checksum [32]byte) ([]byte, error)
	GetPackfileSubpart(checksum [32]byte, offset uint32, length uint32) ([]byte, error)
	DeletePackfile(checksum [32]byte) error

	Commit(indexID uuid.UUID, data []byte) error

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

	Cache *cache.Cache
	Key   []byte

	index *state.State

	wBytes uint64
	rBytes uint64

	writeSharedLock *locking.SharedLock
	readSharedLock  *locking.SharedLock

	bufferedPackfiles chan struct{}
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
	muBackends.Lock()
	defer muBackends.Unlock()

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

	if backend, exists := backends[backendName]; !exists {
		return nil, fmt.Errorf("backend '%s' does not exist", backendName)
	} else {
		repository := &Store{}
		repository.Location = location
		repository.backend = backend()
		repository.writeSharedLock = locking.NewSharedLock("store.write", runtime.NumCPU()*8+1)
		repository.readSharedLock = locking.NewSharedLock("store.read", runtime.NumCPU()*8+1)
		repository.bufferedPackfiles = make(chan struct{}, runtime.NumCPU()*2+1)
		return repository, nil
	}
}

func (repository *Store) SetRepositoryIndex(index *state.State) {
	repository.index = index
}

func (repository *Store) GetRepositoryIndex() *state.State {
	return repository.index
}

func Open(location string) (*Store, error) {
	repository, err := New(location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.Open", time.Since(t0))
		logger.Trace("store", "Open(%s): %s", location, time.Since(t0))
	}()

	if err = repository.backend.Open(location); err != nil {
		return nil, err
	} else {
		return repository, nil
	}
}

func Create(location string, configuration Configuration) (*Store, error) {
	repository, err := New(location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.Create", time.Since(t0))
		logger.Trace("store", "Create(%s): %s", location, time.Since(t0))
	}()

	if err = repository.backend.Create(location, configuration); err != nil {
		return nil, err
	} else {
		return repository, nil
	}
}

func (repository *Store) GetRBytes() uint64 {
	return atomic.LoadUint64(&repository.rBytes)
}

func (repository *Store) GetWBytes() uint64 {
	return atomic.LoadUint64(&repository.wBytes)
}

func (repository *Store) GetCache() *cache.Cache {
	return repository.Cache
}

func (repository *Store) GetSecret() []byte {
	if len(repository.Key) == 0 {
		return nil
	}
	return repository.Key
}

func (repository *Store) GetUsername() string {
	return repository.Username
}

func (repository *Store) GetHostname() string {
	return repository.Hostname
}

func (repository *Store) GetCommandLine() string {
	return repository.CommandLine
}

func (repository *Store) GetMachineID() string {
	return repository.MachineID
}

func (repository *Store) SetCache(localCache *cache.Cache) error {
	repository.Cache = localCache
	return nil
}

func (repository *Store) SetSecret(secret []byte) error {
	repository.Key = secret
	return nil
}

func (repository *Store) SetUsername(username string) error {
	repository.Username = username
	return nil
}

func (repository *Store) SetHostname(hostname string) error {
	repository.Hostname = hostname
	return nil
}

func (repository *Store) SetCommandLine(commandLine string) error {
	repository.CommandLine = commandLine
	return nil
}

func (repository *Store) SetMachineID(machineID string) error {
	repository.MachineID = machineID
	return nil
}

func (repository *Store) Configuration() Configuration {
	return repository.backend.Configuration()
}

/* snapshots  */
func (repository *Store) GetSnapshots() ([]uuid.UUID, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetSnapshots", time.Since(t0))
		logger.Trace("store", "GetSnapshots(): %s", time.Since(t0))
	}()
	return repository.backend.GetSnapshots()
}

func (repository *Store) PutSnapshot(indexID uuid.UUID, data []byte) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.PutSnapshot", time.Since(t0))
		logger.Trace("store", "PutSnapshot(%s): %s", indexID, time.Since(t0))
	}()

	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutSnapshot(indexID, data)
}

func (repository *Store) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetSnapshot", time.Since(t0))
		logger.Trace("store", "GetSnapshot(%s): %s", indexID, time.Since(t0))
	}()

	data, err := repository.backend.GetSnapshot(indexID)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))

	return data, nil
}

func (repository *Store) DeleteSnapshot(indexID uuid.UUID) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.DeleteSnapshot", time.Since(t0))
		logger.Trace("store", "DeleteSnapshot(%s): %s", indexID, time.Since(t0))
	}()
	return repository.backend.DeleteSnapshot(indexID)
}

/* Packfiles */
func (repository *Store) GetPackfiles() ([][32]byte, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetPackfiles", time.Since(t0))
		logger.Trace("store", "GetPackfiles(): %s", time.Since(t0))
	}()
	return repository.backend.GetPackfiles()
}

func (repository *Store) GetPackfile(checksum [32]byte) ([]byte, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetPackfile", time.Since(t0))
		logger.Trace("store", "GetPackfile(%016x): %s", checksum, time.Since(t0))
	}()

	data, err := repository.backend.GetPackfile(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}

func (repository *Store) GetPackfileSubpart(checksum [32]byte, offset uint32, length uint32) ([]byte, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetPackfileSubpart", time.Since(t0))
		logger.Trace("store", "GetPackfileSubpart(%016x, %d, %d): %s", checksum, offset, length, time.Since(t0))
	}()

	data, err := repository.backend.GetPackfileSubpart(checksum, offset, length)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}

func (repository *Store) PutPackfile(checksum [32]byte, data []byte) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.PutPackfile", time.Since(t0))
		logger.Trace("store", "PutPackfile(%016x): %s", checksum, time.Since(t0))
	}()

	repository.bufferedPackfiles <- struct{}{}
	defer func() { <-repository.bufferedPackfiles }()

	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutPackfile(checksum, data)
}

func (repository *Store) DeletePackfile(checksum [32]byte) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.DeletePackfile", time.Since(t0))
		logger.Trace("store", "DeletePackfile(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.DeletePackfile(checksum)
}

/* Indexes */
func (repository *Store) GetStates() ([][32]byte, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetIndexes", time.Since(t0))
		logger.Trace("store", "GetIndexes(): %s", time.Since(t0))
	}()
	return repository.backend.GetStates()
}

func (repository *Store) PutState(checksum [32]byte, data []byte) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.PutIndex", time.Since(t0))
		logger.Trace("store", "PutIndex(%016x): %s", checksum, time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutState(checksum, data)
}

func (repository *Store) GetState(checksum [32]byte) ([]byte, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetIndex", time.Since(t0))
		logger.Trace("store", "GetIndex(%016x): %s", checksum, time.Since(t0))
	}()

	data, err := repository.backend.GetState(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}

func (repository *Store) DeleteState(checksum [32]byte) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.DeleteIndex", time.Since(t0))
		logger.Trace("store", "DeleteIndex(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.DeleteState(checksum)
}

/* Blobs */
func (repository *Store) GetBlobs() ([][32]byte, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetBlobs", time.Since(t0))
		logger.Trace("store", "GetBlobs(): %s", time.Since(t0))
	}()
	return repository.backend.GetBlobs()
}

func (repository *Store) PutBlob(checksum [32]byte, data []byte) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.PutBlob", time.Since(t0))
		logger.Trace("store", "PutBlob(%016x): %s", checksum, time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutBlob(checksum, data)
}

func (repository *Store) CheckBlob(checksum [32]byte) (bool, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.CheckBlob", time.Since(t0))
		logger.Trace("store", "CheckBlob(%016x): %s", checksum, time.Since(t0))
	}()

	return repository.backend.CheckBlob(checksum)
}

func (repository *Store) GetBlob(checksum [32]byte) ([]byte, error) {
	repository.readSharedLock.Lock()
	defer repository.readSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.GetBlob", time.Since(t0))
		logger.Trace("store", "GetBlob(%016x): %s", checksum, time.Since(t0))
	}()

	data, err := repository.backend.GetBlob(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}

func (repository *Store) DeleteBlob(checksum [32]byte) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.DeleteBlob", time.Since(t0))
		logger.Trace("store", "DeleteBlob(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.DeleteBlob(checksum)
}

func (repository *Store) Close() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.Close", time.Since(t0))
		logger.Trace("store", "Close(): %s", time.Since(t0))
	}()
	return repository.backend.Close()
}

func (repository *Store) Commit(indexID uuid.UUID, data []byte) error {
	repository.writeSharedLock.Lock()
	defer repository.writeSharedLock.Unlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("store.Commit", time.Since(t0))
		logger.Trace("store", "Commit(%s): %s", indexID.String(), time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))

	return repository.backend.Commit(indexID, data)
}
