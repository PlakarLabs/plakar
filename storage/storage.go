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
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/logger"
)

var muBackends sync.Mutex
var backends map[string]func() StoreBackend = make(map[string]func() StoreBackend)

func Register(name string, backend func() StoreBackend) {
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

func New(name string) (*Store, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if backend, exists := backends[name]; !exists {
		return nil, fmt.Errorf("backend '%s' registered twice", name)
	} else {
		store := &Store{}
		store.backend = backend()
		return store, nil
	}
}

type Store struct {
	backend StoreBackend

	Username    string
	Hostname    string
	CommandLine string

	Cache   *cache.Cache
	Keypair *encryption.Keypair
}

func (store *Store) GetCache() *cache.Cache {
	return store.Cache
}

func (store *Store) GetKeypair() *encryption.Keypair {
	return store.Keypair
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

func (store *Store) SetCache(localCache *cache.Cache) error {
	store.Cache = localCache
	return nil
}

func (store *Store) SetKeypair(localKeypair *encryption.Keypair) error {
	store.Keypair = localKeypair
	return nil
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

func (store *Store) Create(repository string, configuration StoreConfig) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Create(%s): %s", repository, time.Since(t0))
	}()
	return store.backend.Create(repository, configuration)
}

func (store *Store) Open(repository string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Open(%s): %s", repository, time.Since(t0))
	}()
	return store.backend.Open(repository)
}

func (store *Store) Configuration() StoreConfig {
	return store.backend.Configuration()
}

func (store *Store) Transaction() (*Transaction, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Transaction(): %s", time.Since(t0))
	}()
	tx, err := store.backend.Transaction()
	if err != nil {
		return nil, err
	}

	wrapperTx := &Transaction{}
	wrapperTx.backend = tx
	return wrapperTx, nil
}

func (store *Store) GetIndexes() ([]string, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndexes(): %s", time.Since(t0))
	}()
	return store.backend.GetIndexes()
}

func (store *Store) GetIndex(id string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndex(%s): %s", id, time.Since(t0))
	}()
	return store.backend.GetIndex(id)
}

func (store *Store) PutIndex(id string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: PutIndex(%s): %s", id, time.Since(t0))
	}()
	return store.backend.PutIndex(id, data)
}

func (store *Store) GetIndexObject(id string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndexObject(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return store.backend.GetIndexObject(id, checksum)
}

func (store *Store) GetIndexChunk(id string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndexObject(%s): %s", id, checksum, time.Since(t0))
	}()
	return store.backend.GetIndexChunk(id, checksum)
}

func (store *Store) ReferenceIndexChunk(id string, checksum string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: RefIndexChunk(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return store.backend.ReferenceIndexChunk(id, checksum)
}

func (store *Store) ReferenceIndexObject(id string, checksum string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: RefIndexObject(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return store.backend.ReferenceIndexObject(id, checksum)
}

func (store *Store) GetObjects() ([]string, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetObjects(): %s", time.Since(t0))
	}()
	return store.backend.GetObjects()
}

func (store *Store) GetObject(checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetObject(%s): %s", checksum, time.Since(t0))
	}()
	return store.backend.GetObject(checksum)
}

func (store *Store) PutObject(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: PutObject(%s): %s", checksum, time.Since(t0))
	}()
	return store.backend.PutObject(checksum, data)
}

func (store *Store) GetObjectRefCount(checksum string) (uint16, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetObjectRefCount(%s): %s", checksum, time.Since(t0))
	}()
	return store.backend.GetObjectRefCount(checksum)
}

func (store *Store) GetChunks() ([]string, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetChunks(): %s", time.Since(t0))
	}()
	return store.backend.GetChunks()
}

func (store *Store) GetChunk(checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetChunk(%s): %s", checksum, time.Since(t0))
	}()
	return store.backend.GetChunk(checksum)
}

func (store *Store) PutChunk(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: PutChunk(%s): %s", checksum, time.Since(t0))
	}()
	return store.backend.PutChunk(checksum, data)
}

func (store *Store) GetChunkRefCount(checksum string) (uint16, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetChunkRefCount(%s): %s", checksum, time.Since(t0))
	}()
	return store.backend.GetChunkRefCount(checksum)
}

func (store *Store) CheckIndexObject(id string, checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckIndexObject(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return store.backend.CheckIndexObject(id, checksum)
}

func (store *Store) CheckIndexChunk(id string, checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckIndexChunk(%s, %s): %s", id, checksum, time.Since(t0))
	}()
	return store.backend.CheckIndexChunk(id, checksum)
}

func (store *Store) CheckObject(checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckObject(%s): %s", checksum, time.Since(t0))
	}()
	return store.backend.CheckObject(checksum)
}

func (store *Store) CheckChunk(checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckChunk(%s): %s", checksum, time.Since(t0))
	}()
	return store.backend.CheckChunk(checksum)
}

func (store *Store) Purge(id string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Purge(%s): %s", id, time.Since(t0))
	}()
	return store.backend.Purge(id)
}

func (store *Store) Close() error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Close(): %s", time.Since(t0))
	}()
	return store.backend.Close()
}

type Transaction struct {
	backend TransactionBackend
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
