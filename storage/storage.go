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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/logger"
)

const VERSION string = "0.1.0"

type RepositoryConfig struct {
	Uuid        string
	Version     string
	Encryption  string
	Compression string
}

type RepositoryBackend interface {
	Create(repository string, configuration RepositoryConfig) error
	Open(repository string) error
	Configuration() RepositoryConfig

	Transaction() (TransactionBackend, error)

	GetIndexes() ([]string, error)
	GetMetadata(id string) ([]byte, error)
	PutMetadata(id string, data []byte) error
	GetIndex(id string) ([]byte, error)
	PutIndex(id string, data []byte) error
	GetIndexObject(id string, checksum string) ([]byte, error)
	CheckIndexObject(id string, checksum string) (bool, error)
	GetIndexChunk(id string, checksum string) ([]byte, error)
	CheckIndexChunk(id string, checksum string) (bool, error)
	ReferenceIndexObject(id string, checksum string) error
	ReferenceIndexChunk(id string, checksum string) error

	GetObjects() ([]string, error)
	GetObject(checksum string) ([]byte, error)
	CheckObject(checksum string) (bool, error)
	PutObject(checksum string, data []byte) error
	GetObjectRefCount(checksum string) (uint64, error)
	GetObjectSize(checksum string) (uint64, error)

	GetChunks() ([]string, error)
	GetChunk(checksum string) ([]byte, error)
	CheckChunk(checksum string) (bool, error)
	PutChunk(checksum string, data []byte) error
	GetChunkRefCount(checksum string) (uint64, error)
	GetChunkSize(checksum string) (uint64, error)

	Purge(id string) error

	Close() error
}

type TransactionBackend interface {
	GetUuid() string

	ReferenceObjects(keys []string) ([]bool, error)
	PutObject(checksum string, data []byte) error

	ReferenceChunks(keys []string) ([]bool, error)
	PutChunk(checksum string, data []byte) error

	PutMetadata(data []byte) error
	PutIndex(data []byte) error
	Commit() error
}

var muBackends sync.Mutex
var backends map[string]func() RepositoryBackend = make(map[string]func() RepositoryBackend)

type Repository struct {
	backend RepositoryBackend

	Username    string
	Hostname    string
	CommandLine string
	MachineID   string

	Cache *cache.Cache
	//Keypair *encryption.Keypair
	Key []byte
}

type Transaction struct {
	backend TransactionBackend
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
		} else {
			return nil, fmt.Errorf("unsupported plakar protocol")
		}
	} else {
		backendName = "filesystem"
	}

	if backend, exists := backends[backendName]; !exists {
		return nil, fmt.Errorf("backend '%s' does not exist", backendName)
	} else {
		repository := &Repository{}
		repository.backend = backend()
		return repository, nil
	}
}

func Open(location string) (*Repository, error) {
	repository, err := New(location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Open(%s): %s", location, time.Since(t0))
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
		logger.Profile("storage: Create(%s): %s", location, time.Since(t0))
	}()

	err = repository.backend.Create(location, configuration)
	if err != nil {
		return nil, err
	}
	return repository, nil
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

func (repository *Repository) Transaction() (*Transaction, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Transaction(): %s", time.Since(t0))
	}()
	tx, err := repository.backend.Transaction()
	if err != nil {
		return nil, err
	}

	wrapperTx := &Transaction{}
	wrapperTx.backend = tx
	return wrapperTx, nil
}

func (repository *Repository) GetIndexes() ([]string, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndexes(): %s", time.Since(t0))
	}()
	return repository.backend.GetIndexes()
}

func (repository *Repository) GetMetadata(id string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetMetadata(%s): %s", id, time.Since(t0))
	}()
	return repository.backend.GetMetadata(id)
}

func (repository *Repository) GetIndex(id string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndex(%s): %s", id, time.Since(t0))
	}()
	return repository.backend.GetIndex(id)
}

func (repository *Repository) PutMetadata(id string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: PutMetadata(%s): %s", id, time.Since(t0))
	}()
	return repository.backend.PutMetadata(id, data)
}

func (repository *Repository) PutIndex(id string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: PutIndex(%s): %s", id, time.Since(t0))
	}()
	return repository.backend.PutIndex(id, data)
}

func (repository *Repository) GetIndexObject(id string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndexObject(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return repository.backend.GetIndexObject(id, checksum)
}

func (repository *Repository) GetIndexChunk(id string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndexObject(%s): %s", id, checksum, time.Since(t0))
	}()
	return repository.backend.GetIndexChunk(id, checksum)
}

func (repository *Repository) ReferenceIndexChunk(id string, checksum string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: RefIndexChunk(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return repository.backend.ReferenceIndexChunk(id, checksum)
}

func (repository *Repository) ReferenceIndexObject(id string, checksum string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: RefIndexObject(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return repository.backend.ReferenceIndexObject(id, checksum)
}

func (repository *Repository) GetObjects() ([]string, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetObjects(): %s", time.Since(t0))
	}()
	return repository.backend.GetObjects()
}

func (repository *Repository) GetObject(checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetObject(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.GetObject(checksum)
}

func (repository *Repository) PutObject(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: PutObject(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.PutObject(checksum, data)
}

func (repository *Repository) GetObjectRefCount(checksum string) (uint64, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetObjectRefCount(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.GetObjectRefCount(checksum)
}

func (repository *Repository) GetObjectSize(checksum string) (uint64, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetObjectSize(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.GetObjectSize(checksum)
}

func (repository *Repository) GetChunks() ([]string, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetChunks(): %s", time.Since(t0))
	}()
	return repository.backend.GetChunks()
}

func (repository *Repository) GetChunk(checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetChunk(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.GetChunk(checksum)
}

func (repository *Repository) PutChunk(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: PutChunk(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.PutChunk(checksum, data)
}

func (repository *Repository) GetChunkRefCount(checksum string) (uint64, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetChunkRefCount(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.GetChunkRefCount(checksum)
}

func (repository *Repository) GetChunkSize(checksum string) (uint64, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetChunkSize(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.GetChunkSize(checksum)
}

func (repository *Repository) CheckIndexObject(id string, checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckIndexObject(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return repository.backend.CheckIndexObject(id, checksum)
}

func (repository *Repository) CheckIndexChunk(id string, checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckIndexChunk(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return repository.backend.CheckIndexChunk(id, checksum)
}

func (repository *Repository) CheckObject(checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckObject(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.CheckObject(checksum)
}

func (repository *Repository) CheckChunk(checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckChunk(%s): %s", checksum, time.Since(t0))
	}()
	return repository.backend.CheckChunk(checksum)
}

func (repository *Repository) Purge(id string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Purge(%s): %s", id, time.Since(t0))
	}()
	return repository.backend.Purge(id)
}

func (repository *Repository) Close() error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Close(): %s", time.Since(t0))
	}()
	return repository.backend.Close()
}

func (transaction *Transaction) GetUuid() string {
	return transaction.backend.GetUuid()
}

func (transaction *Transaction) ReferenceObjects(keys []string) ([]bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.ReferenceObjects([%d keys]): %s", transaction.GetUuid(), len(keys), time.Since(t0))
	}()
	return transaction.backend.ReferenceObjects(keys)
}

func (transaction *Transaction) PutObject(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.PutObject(%s) <- %d bytes: %s", transaction.GetUuid(), checksum, len(data), time.Since(t0))
	}()
	return transaction.backend.PutObject(checksum, data)
}

func (transaction *Transaction) ReferenceChunks(keys []string) ([]bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.ReferenceChunks([%d keys]): %s", transaction.GetUuid(), len(keys), time.Since(t0))
	}()
	return transaction.backend.ReferenceChunks(keys)
}

func (transaction *Transaction) PutChunk(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.PutChunk(%s) <- %d bytes: %s", transaction.GetUuid(), checksum, len(data), time.Since(t0))
	}()
	return transaction.backend.PutChunk(checksum, data)
}

func (transaction *Transaction) PutMetadata(data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.PutMetadata() <- %d bytes: %s", transaction.GetUuid(), len(data), time.Since(t0))
	}()
	return transaction.backend.PutMetadata(data)
}

func (transaction *Transaction) PutIndex(data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.PutIndex() <- %d bytes: %s", transaction.GetUuid(), len(data), time.Since(t0))
	}()
	return transaction.backend.PutIndex(data)
}

func (transaction *Transaction) Commit() error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.Commit(): %s", transaction.GetUuid(), time.Since(t0))
	}()
	return transaction.backend.Commit()
}
