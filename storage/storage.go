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
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage/index"
	"github.com/google/uuid"
)

const VERSION string = "0.4.0"

type RepositoryConfig struct {
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

type RepositoryBackend interface {
	Create(repository string, configuration RepositoryConfig) error
	Open(repository string) error
	Configuration() RepositoryConfig

	GetSnapshots() ([]uuid.UUID, error)
	PutSnapshot(indexID uuid.UUID, data []byte) error
	GetSnapshot(indexID uuid.UUID) ([]byte, error)
	DeleteSnapshot(indexID uuid.UUID) error

	GetBlobs() ([][32]byte, error)
	PutBlob(checksum [32]byte, data []byte) error
	GetBlob(checksum [32]byte) ([]byte, error)
	DeleteBlob(checksum [32]byte) error

	GetIndexes() ([][32]byte, error)
	PutIndex(checksum [32]byte, data []byte) error
	GetIndex(checksum [32]byte) ([]byte, error)
	DeleteIndex(checksum [32]byte) error

	GetPackfiles() ([][32]byte, error)
	PutPackfile(checksum [32]byte, data []byte) error
	GetPackfile(checksum [32]byte) ([]byte, error)
	DeletePackfile(checksum [32]byte) error

	Commit(indexID uuid.UUID, data []byte) error

	Close() error
}

var muBackends sync.Mutex
var backends map[string]func() RepositoryBackend = make(map[string]func() RepositoryBackend)

type Repository struct {
	backend RepositoryBackend

	Location    string
	Username    string
	Hostname    string
	CommandLine string
	MachineID   string

	Cache *cache.Cache
	Key   []byte

	index *index.Index

	wBytes uint64
	rBytes uint64

	wChan          chan bool
	rChan          chan bool
	sChan          chan bool
	maxParallelism chan bool
}

func Register(name string, backend func() RepositoryBackend) {
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

func New(location string) (*Repository, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	var backendName string
	if !strings.HasPrefix(location, "/") {
		if strings.HasPrefix(location, "plakar://") || strings.HasPrefix(location, "ssh://") || strings.HasPrefix(location, "stdio://") {
			backendName = "client"
		} else if strings.HasPrefix(location, "sqlite://") {
			backendName = "database"
		} else if strings.HasPrefix(location, "s3://") {
			backendName = "s3"
		} else if strings.HasPrefix(location, "null://") {
			backendName = "null"
		} else if strings.HasPrefix(location, "fs://") {
			backendName = "fs"
		} else {
			if strings.Contains(location, "://") {
				return nil, fmt.Errorf("unsupported plakar protocol")
			} else {
				backendName = "fs"
			}
		}
	} else {
		backendName = "fs"
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
		repository := &Repository{}
		repository.Location = location
		repository.backend = backend()
		//		repository.maxParallelism = make(chan bool, runtime.NumCPU()*8+1)
		repository.wChan = make(chan bool, runtime.NumCPU()*8+1)
		repository.rChan = make(chan bool, runtime.NumCPU()*8+1)
		repository.sChan = make(chan bool, runtime.NumCPU()*8+1)

		return repository, nil
	}
}

func (repository *Repository) SetMaxParallelism(parallelism int) {
	if parallelism == 0 || parallelism == cap(repository.maxParallelism) {
		return
	}
	repository.maxParallelism = make(chan bool, parallelism)
}

func (repository *Repository) SetReadParallelism(parallelism int) {
	if parallelism == 0 || parallelism == cap(repository.rChan) {
		return
	}
	repository.rChan = make(chan bool, parallelism)
}

func (repository *Repository) SetWriteParallelism(parallelism int) {
	if parallelism == 0 || parallelism == cap(repository.wChan) {
		return
	}
	repository.wChan = make(chan bool, parallelism)
}

func (repository *Repository) SetStatParallelism(parallelism int) {
	if parallelism == 0 || parallelism == cap(repository.sChan) {
		return
	}
	repository.sChan = make(chan bool, parallelism)
}

func (repository *Repository) SetRepositoryIndex(index *index.Index) {
	repository.index = index
}

func (repository *Repository) GetRepositoryIndex() *index.Index {
	return repository.index
}

func (repository *Repository) wLock() {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.wLock", time.Since(t0))
		logger.Trace("storage.locking", "wLock -> %d : %s", len(repository.wChan), time.Since(t0))
	}()
	//	repository.maxParallelism <- true
	repository.wChan <- true
}
func (repository *Repository) wUnlock() {
	<-repository.wChan
	logger.Trace("storage.locking", "wUnlock -> %d", len(repository.wChan))
	// <-repository.maxParallelism
}

func (repository *Repository) rLock() {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.rLock", time.Since(t0))
		logger.Trace("storage.locking", "rLock -> %d : %s", len(repository.rChan), time.Since(t0))
	}()
	//	repository.maxParallelism <- true
	repository.rChan <- true
}
func (repository *Repository) rUnlock() {
	<-repository.rChan
	logger.Trace("storage.locking", "rUnlock -> %d", len(repository.rChan))
	// <-repository.maxParallelism
}

func (repository *Repository) sLock() {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.sLock", time.Since(t0))
		logger.Trace("storage.locking", "sLock -> %d : %s", len(repository.sChan), time.Since(t0))
	}()
	//	repository.maxParallelism <- true
	repository.sChan <- true
}
func (repository *Repository) sUnlock() {
	<-repository.sChan
	logger.Trace("storage.locking", "sUnlock -> %d", len(repository.sChan))
	// <-repository.maxParallelism
}

func Open(location string) (*Repository, error) {
	repository, err := New(location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.Open", time.Since(t0))
		logger.Trace("storage", "Open(%s): %s", location, time.Since(t0))
	}()

	err = repository.backend.Open(location)
	if err != nil {
		return nil, err
	}
	return repository, nil
}

func Create(location string, configuration RepositoryConfig) (*Repository, error) {
	repository, err := New(location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.Create", time.Since(t0))
		logger.Trace("storage", "Create(%s): %s", location, time.Since(t0))
	}()

	err = repository.backend.Create(location, configuration)
	if err != nil {
		return nil, err
	}
	return repository, nil
}

func (repository *Repository) GetRBytes() uint64 {
	return atomic.LoadUint64(&repository.rBytes)
}

func (repository *Repository) GetWBytes() uint64 {
	return atomic.LoadUint64(&repository.wBytes)
}

func (repository *Repository) GetCache() *cache.Cache {
	return repository.Cache
}

func (repository *Repository) GetSecret() []byte {
	if len(repository.Key) == 0 {
		return nil
	}
	return repository.Key
}

func (repository *Repository) GetUsername() string {
	return repository.Username
}

func (repository *Repository) GetHostname() string {
	return repository.Hostname
}

func (repository *Repository) GetCommandLine() string {
	return repository.CommandLine
}

func (repository *Repository) GetMachineID() string {
	return repository.MachineID
}

func (repository *Repository) SetCache(localCache *cache.Cache) error {
	repository.Cache = localCache
	return nil
}

func (repository *Repository) SetSecret(secret []byte) error {
	repository.Key = secret
	return nil
}

func (repository *Repository) SetUsername(username string) error {
	repository.Username = username
	return nil
}

func (repository *Repository) SetHostname(hostname string) error {
	repository.Hostname = hostname
	return nil
}

func (repository *Repository) SetCommandLine(commandLine string) error {
	repository.CommandLine = commandLine
	return nil
}

func (repository *Repository) SetMachineID(machineID string) error {
	repository.MachineID = machineID
	return nil
}

func (repository *Repository) Configuration() RepositoryConfig {
	return repository.backend.Configuration()
}

/* snapshots  */
func (repository *Repository) GetSnapshots() ([]uuid.UUID, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetIndexes", time.Since(t0))
		logger.Trace("storage", "GetIndexes(): %s", time.Since(t0))
	}()
	return repository.backend.GetSnapshots()
}

func (repository *Repository) PutSnapshot(indexID uuid.UUID, data []byte) error {
	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.PutSnapshot", time.Since(t0))
		logger.Trace("storage", "PutSnapshot(%s): %s", indexID, time.Since(t0))
	}()

	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutSnapshot(indexID, data)
}

func (repository *Repository) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetSnapshot", time.Since(t0))
		logger.Trace("storage", "GetSnapshot(%s): %s", indexID, time.Since(t0))
	}()

	data, err := repository.backend.GetSnapshot(indexID)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))

	return data, nil
}

func (repository *Repository) DeleteSnapshot(indexID uuid.UUID) error {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.DeleteSnapshot", time.Since(t0))
		logger.Trace("storage", "DeleteSnapshot(%s): %s", indexID, time.Since(t0))
	}()
	return repository.backend.DeleteSnapshot(indexID)
}

/* Packfiles */
func (repository *Repository) GetPackfiles() ([][32]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetPackfiles", time.Since(t0))
		logger.Trace("storage", "GetPackfiles(): %s", time.Since(t0))
	}()
	return repository.backend.GetPackfiles()
}

func (repository *Repository) GetPackfile(checksum [32]byte) ([]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetPackfile", time.Since(t0))
		logger.Trace("storage", "GetPackfile(%016x): %s", checksum, time.Since(t0))
	}()

	data, err := repository.backend.GetPackfile(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}

func (repository *Repository) PutPackfile(checksum [32]byte, data []byte) error {
	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.PutPackfile", time.Since(t0))
		logger.Trace("storage", "PutPackfile(%016x): %s", checksum, time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutPackfile(checksum, data)
}

func (repository *Repository) DeletePackfile(checksum [32]byte) error {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.DeletePackfile", time.Since(t0))
		logger.Trace("storage", "DeletePackfile(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.DeletePackfile(checksum)
}

/* Indexes */
func (repository *Repository) GetIndexes() ([][32]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetIndexes", time.Since(t0))
		logger.Trace("storage", "GetIndexes(): %s", time.Since(t0))
	}()
	return repository.backend.GetIndexes()
}

func (repository *Repository) PutIndex(checksum [32]byte, data []byte) error {
	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.PutIndex", time.Since(t0))
		logger.Trace("storage", "PutIndex(%016x): %s", checksum, time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutIndex(checksum, data)
}

func (repository *Repository) GetIndex(checksum [32]byte) ([]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetIndex", time.Since(t0))
		logger.Trace("storage", "GetIndex(%016x): %s", checksum, time.Since(t0))
	}()

	data, err := repository.backend.GetIndex(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}

func (repository *Repository) DeleteIndex(checksum [32]byte) error {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.DeleteIndex", time.Since(t0))
		logger.Trace("storage", "DeleteIndex(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.DeleteIndex(checksum)
}

/* Blobs */
func (repository *Repository) GetBlobs() ([][32]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetBlobs", time.Since(t0))
		logger.Trace("storage", "GetBlobs(): %s", time.Since(t0))
	}()
	return repository.backend.GetBlobs()
}

func (repository *Repository) PutBlob(checksum [32]byte, data []byte) error {
	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.PutBlob", time.Since(t0))
		logger.Trace("storage", "PutBlob(%016x): %s", checksum, time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutBlob(checksum, data)
}

func (repository *Repository) GetBlob(checksum [32]byte) ([]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetBlob", time.Since(t0))
		logger.Trace("storage", "GetBlob(%016x): %s", checksum, time.Since(t0))
	}()

	data, err := repository.backend.GetBlob(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}
func (repository *Repository) DeleteBlob(checksum [32]byte) error {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.DeleteBlob", time.Since(t0))
		logger.Trace("storage", "DeleteBlob(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.DeleteBlob(checksum)
}

func (repository *Repository) Close() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.Close", time.Since(t0))
		logger.Trace("storage", "Close(): %s", time.Since(t0))
	}()
	return repository.backend.Close()
}

func (repository *Repository) Commit(indexID uuid.UUID, data []byte) error {
	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.Commit", time.Since(t0))
		logger.Trace("storage", "Commit(%s): %s", indexID.String(), time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))

	return repository.backend.Commit(indexID, data)
}
