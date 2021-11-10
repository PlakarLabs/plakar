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
var backends map[string]Store = make(map[string]Store)

func Register(name string, backend Store) {
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

func New(name string) (Store, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if backend, exists := backends[name]; !exists {
		return nil, fmt.Errorf("backend '%s' registered twice", name)
	} else {
		wrapper := &WrapperStore{}
		wrapper.store = backend
		return wrapper, nil
	}
}

type WrapperStore struct {
	store Store

	Cache   *cache.Cache
	Keypair *encryption.Keypair
}

func (wrapper *WrapperStore) GetCache() *cache.Cache {
	return wrapper.Cache
}

func (wrapper *WrapperStore) GetKeypair() *encryption.Keypair {
	return wrapper.Keypair
}

func (wrapper *WrapperStore) SetCache(localCache *cache.Cache) error {
	wrapper.Cache = localCache
	return nil
}

func (wrapper *WrapperStore) SetKeypair(localKeypair *encryption.Keypair) error {
	wrapper.Keypair = localKeypair
	return nil
}

func (wrapper *WrapperStore) Create(repository string, configuration StoreConfig) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Create(%s): %s", repository, time.Since(t0))
	}()
	return wrapper.store.Create(repository, configuration)
}

func (wrapper *WrapperStore) Open(repository string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Open(%s): %s", repository, time.Since(t0))
	}()
	return wrapper.store.Open(repository)
}

func (wrapper *WrapperStore) Configuration() StoreConfig {
	return wrapper.store.Configuration()
}

func (wrapper *WrapperStore) Transaction() (Transaction, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Transaction(): %s", time.Since(t0))
	}()
	tx, err := wrapper.store.Transaction()
	if err != nil {
		return nil, err
	}

	wrapperTx := &WrapperTransaction{}
	wrapperTx.transaction = tx
	return wrapperTx, nil
}

func (wrapper *WrapperStore) GetIndexes() ([]string, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndexes(): %s", time.Since(t0))
	}()
	return wrapper.store.GetIndexes()
}
func (wrapper *WrapperStore) GetIndex(id string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetIndex(%s): %s", id, time.Since(t0))
	}()
	return wrapper.store.GetIndex(id)
}

func (wrapper *WrapperStore) GetObject(checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetObject(%s): %s", checksum, time.Since(t0))
	}()
	return wrapper.store.GetObject(checksum)
}

func (wrapper *WrapperStore) GetChunk(checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: GetChunk(%s): %s", checksum, time.Since(t0))
	}()
	return wrapper.store.GetChunk(checksum)
}

func (wrapper *WrapperStore) CheckObject(checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckObject(%s): %s", checksum, time.Since(t0))
	}()
	return wrapper.store.CheckObject(checksum)
}

func (wrapper *WrapperStore) CheckChunk(checksum string) (bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: CheckChunk(%s): %s", checksum, time.Since(t0))
	}()
	return wrapper.store.CheckChunk(checksum)
}

func (wrapper *WrapperStore) Purge(id string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: Purge(%s): %s", id, time.Since(t0))
	}()
	return wrapper.store.Purge(id)
}

type WrapperTransaction struct {
	transaction Transaction
}

func (wrapper *WrapperTransaction) GetUuid() string {
	return wrapper.transaction.GetUuid()
}

func (wrapper *WrapperTransaction) ReferenceObjects(keys []string) ([]bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.ReferenceObjects([%d keys]): %s", wrapper.GetUuid(), len(keys), time.Since(t0))
	}()
	return wrapper.transaction.ReferenceObjects(keys)
}

func (wrapper *WrapperTransaction) PutObject(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.PutObject(%s) <- %d bytes: %s", wrapper.GetUuid(), checksum, len(data), time.Since(t0))
	}()
	return wrapper.transaction.PutObject(checksum, data)
}

func (wrapper *WrapperTransaction) ReferenceChunks(keys []string) ([]bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.ReferenceChunks([%d keys]): %s", wrapper.GetUuid(), len(keys), time.Since(t0))
	}()
	return wrapper.transaction.ReferenceChunks(keys)
}

func (wrapper *WrapperTransaction) PutChunk(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.PutChunk(%s) <- %d bytes: %s", wrapper.GetUuid(), checksum, len(data), time.Since(t0))
	}()
	return wrapper.transaction.PutChunk(checksum, data)
}

func (wrapper *WrapperTransaction) PutIndex(data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.PutIndex() <- %d bytes: %s", wrapper.GetUuid(), len(data), time.Since(t0))
	}()
	return wrapper.transaction.PutIndex(data)
}

func (wrapper *WrapperTransaction) Commit() error {
	t0 := time.Now()
	defer func() {
		logger.Profile("storage: %s.Commit(): %s", wrapper.GetUuid(), time.Since(t0))
	}()
	return wrapper.transaction.Commit()
}
