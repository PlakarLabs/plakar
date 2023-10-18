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
	"github.com/google/uuid"
)

const VERSION string = "0.2.1"

type RepositoryConfig struct {
	CreationTime time.Time
	RepositoryID uuid.UUID
	Version      string
	Encryption   string
	Compression  string
	Hashing      string
}

type RepositoryBackend interface {
	Create(repository string, configuration RepositoryConfig) error
	Open(repository string) error
	Configuration() RepositoryConfig

	Transaction(indexID uuid.UUID) (TransactionBackend, error)

	GetIndexes() ([]uuid.UUID, error)
	GetMetadata(indexID uuid.UUID) ([]byte, error)
	PutMetadata(indexID uuid.UUID, data []byte) error
	GetBlob(checksum [32]byte) ([]byte, error)
	PutBlob(checksum [32]byte, data []byte) error

	GetObjects() ([][32]byte, error)
	GetObject(checksum [32]byte) ([]byte, error)
	CheckObject(checksum [32]byte) (bool, error)
	PutObject(checksum [32]byte) error
	DeleteObject(checksum [32]byte) error

	GetChunks() ([][32]byte, error)
	GetChunk(checksum [32]byte) ([]byte, error)
	CheckChunk(checksum [32]byte) (bool, error)
	PutChunk(checksum [32]byte, data []byte) error
	DeleteChunk(checksum [32]byte) error

	Purge(indexID uuid.UUID) error

	Close() error
}

type TransactionBackend interface {
	GetUuid() uuid.UUID

	PutObject(checksum [32]byte) error
	PutChunk(checksum [32]byte, data []byte) error

	PutMetadata(data []byte) error
	PutBlob(checksum [32]byte, data []byte) error

	Commit() error
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

	wBytes uint64
	rBytes uint64

	wChan          chan bool
	rChan          chan bool
	sChan          chan bool
	maxParallelism chan bool
}

type Transaction struct {
	repository *Repository
	backend    TransactionBackend
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

func (repository *Repository) wLock() {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.wLock", time.Since(t0))
		logger.Trace("storage", "wLock -> %d : %s", len(repository.wChan), time.Since(t0))
	}()
	//	repository.maxParallelism <- true
	repository.wChan <- true
}
func (repository *Repository) wUnlock() {
	<-repository.wChan
	logger.Trace("storage", "wUnlock -> %d", len(repository.wChan))
	// <-repository.maxParallelism
}

func (repository *Repository) rLock() {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.rLock", time.Since(t0))
		logger.Trace("storage", "rLock -> %d : %s", len(repository.rChan), time.Since(t0))
	}()
	//	repository.maxParallelism <- true
	repository.rChan <- true
}
func (repository *Repository) rUnlock() {
	<-repository.rChan
	logger.Trace("storage", "rUnlock -> %d", len(repository.rChan))
	// <-repository.maxParallelism
}

func (repository *Repository) sLock() {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.sLock", time.Since(t0))
		logger.Trace("storage", "sLock -> %d : %s", len(repository.sChan), time.Since(t0))
	}()
	//	repository.maxParallelism <- true
	repository.sChan <- true
}
func (repository *Repository) sUnlock() {
	<-repository.sChan
	logger.Trace("storage", "sUnlock -> %d", len(repository.sChan))
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

func (repository *Repository) Transaction(indexID uuid.UUID) (*Transaction, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.Transaction", time.Since(t0))
		logger.Trace("storage", "Transaction(): %s", time.Since(t0))
	}()
	tx, err := repository.backend.Transaction(indexID)
	if err != nil {
		return nil, err
	}

	wrapperTx := &Transaction{}
	wrapperTx.repository = repository
	wrapperTx.backend = tx
	return wrapperTx, nil
}

func (repository *Repository) GetIndexes() ([]uuid.UUID, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetIndexes", time.Since(t0))
		logger.Trace("storage", "GetIndexes(): %s", time.Since(t0))
	}()
	return repository.backend.GetIndexes()
}

func (repository *Repository) GetMetadata(indexID uuid.UUID) ([]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetMetadata", time.Since(t0))
		logger.Trace("storage", "GetMetadata(%s): %s", indexID, time.Since(t0))
	}()

	data, err := repository.backend.GetMetadata(indexID)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))

	return data, nil
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

func (repository *Repository) PutMetadata(indexID uuid.UUID, data []byte) error {
	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.PutMetadata", time.Since(t0))
		logger.Trace("storage", "PutMetadata(%s): %s", indexID, time.Since(t0))
	}()

	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutMetadata(indexID, data)
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

func (repository *Repository) GetObjects() ([][32]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetObjects", time.Since(t0))
		logger.Trace("storage", "GetObjects(): %s", time.Since(t0))
	}()
	return repository.backend.GetObjects()
}

func (repository *Repository) GetObject(checksum [32]byte) ([]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetObject", time.Since(t0))
		logger.Trace("storage", "GetObject(%064x): %s", checksum, time.Since(t0))
	}()

	data, err := repository.backend.GetObject(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}

func (repository *Repository) PutObject(checksum [32]byte) error {
	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.PutObject", time.Since(t0))
		logger.Trace("storage", "PutObject(%064x): %s", checksum, time.Since(t0))
	}()

	return repository.backend.PutObject(checksum)
}

func (repository *Repository) DeleteObject(checksum [32]byte) error {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.DeleteObject", time.Since(t0))
		logger.Trace("storage", "DeleteObject(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.DeleteObject(checksum)
}

func (repository *Repository) GetChunks() ([][32]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetChunks", time.Since(t0))
		logger.Trace("storage", "GetChunks(): %s", time.Since(t0))
	}()
	return repository.backend.GetChunks()
}

func (repository *Repository) GetChunk(checksum [32]byte) ([]byte, error) {
	repository.rLock()
	defer repository.rUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetChunk", time.Since(t0))
		logger.Trace("storage", "GetChunk(%064x): %s", checksum, time.Since(t0))
	}()

	data, err := repository.backend.GetChunk(checksum)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&repository.rBytes, uint64(len(data)))
	return data, nil
}

func (repository *Repository) PutChunk(checksum [32]byte, data []byte) (int, error) {
	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.PutChunk", time.Since(t0))
		logger.Trace("storage", "PutChunk(%064x): %s", checksum, time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return len(data), repository.backend.PutChunk(checksum, data)
}

func (repository *Repository) DeleteChunk(checksum [32]byte) error {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.DeleteChunk", time.Since(t0))
		logger.Trace("storage", "DeleteChunk(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.DeleteChunk(checksum)
}

func (repository *Repository) CheckObject(checksum [32]byte) (bool, error) {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.GetObject", time.Since(t0))
		logger.Trace("storage", "CheckObject(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.CheckObject(checksum)
}

func (repository *Repository) CheckChunk(checksum [32]byte) (bool, error) {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.CheckChunk", time.Since(t0))
		logger.Trace("storage", "CheckChunk(%064x): %s", checksum, time.Since(t0))
	}()
	return repository.backend.CheckChunk(checksum)
}

func (repository *Repository) Purge(indexID uuid.UUID) error {
	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.Purge", time.Since(t0))
		logger.Trace("storage", "Purge(%s): %s", indexID, time.Since(t0))
	}()
	return repository.backend.Purge(indexID)
}

func (repository *Repository) Close() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.Close", time.Since(t0))
		logger.Trace("storage", "Close(): %s", time.Since(t0))
	}()
	return repository.backend.Close()
}

func (transaction *Transaction) GetUuid() uuid.UUID {
	return transaction.backend.GetUuid()
}

func (transaction *Transaction) PutMetadata(data []byte) error {
	repository := transaction.repository

	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.tx.PutMetadata", time.Since(t0))
		logger.Trace("storage", "%s.PutMetadata() <- %d bytes: %s", transaction.GetUuid(), len(data), time.Since(t0))
	}()
	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return transaction.backend.PutMetadata(data)
}

func (transaction *Transaction) PutBlob(checksum [32]byte, data []byte) error {
	repository := transaction.repository

	repository.wLock()
	defer repository.wUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.tx.PutBlob", time.Since(t0))
		logger.Trace("storage", "%s.PutBlob(%016x) <- %d bytes: %s", transaction.GetUuid(), checksum, len(data), time.Since(t0))
	}()

	atomic.AddUint64(&repository.wBytes, uint64(len(data)))
	return repository.backend.PutBlob(checksum, data)
}

func (transaction *Transaction) Commit() error {
	repository := transaction.repository

	repository.sLock()
	defer repository.sUnlock()

	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("storage.tx.Commit", time.Since(t0))
		logger.Trace("storage", "%s.Commit(): %s", transaction.GetUuid(), time.Since(t0))
	}()
	return transaction.backend.Commit()
}
