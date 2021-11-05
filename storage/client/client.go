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

package client

import (
	"encoding/gob"
	"log"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/network"
	"github.com/poolpOrg/plakar/storage"
)

func (store *ClientStore) connect(addr string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Fatal(err)
	}

	store.conn = conn
	store.encoder = gob.NewEncoder(conn)
	store.decoder = gob.NewDecoder(conn)

	store.inflightRequests = make(map[string]chan network.Request)
	store.notifications = make(chan network.Request)

	//store.maxConcurrentRequest = make(chan bool, 1024)

	go func() {
		for m := range store.notifications {
			store.mu.Lock()
			notify := store.inflightRequests[m.Uuid]
			store.mu.Unlock()
			notify <- m
		}
	}()

	go func() {
		for {
			result := network.Request{}
			err = store.decoder.Decode(&result)
			if err != nil {
				store.conn.Close()
				return
			}
			store.notifications <- result
		}
	}()

	return err
}

func (store *ClientStore) sendRequest(Type string, Payload interface{}) (*network.Request, error) {
	//store.maxConcurrentRequest <- true
	//defer func() { <-store.maxConcurrentRequest }()

	Uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	request := network.Request{
		Uuid:    Uuid.String(),
		Type:    Type,
		Payload: Payload,
	}

	notify := make(chan network.Request)
	store.mu.Lock()
	store.inflightRequests[request.Uuid] = notify
	store.mu.Unlock()

	err = store.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := <-notify

	store.mu.Lock()
	delete(store.inflightRequests, request.Uuid)
	store.mu.Unlock()
	close(notify)

	return &result, nil
}

func (store *ClientStore) Create(repository string, config storage.StoreConfig) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("Create(%s): %s", repository, time.Since(t0))
	}()
	return nil
}

func (store *ClientStore) GetCache() *cache.Cache {
	return store.Cache
}

func (store *ClientStore) GetKeypair() *encryption.Keypair {
	return store.Keypair
}

func (store *ClientStore) SetCache(localCache *cache.Cache) error {
	store.Cache = localCache
	return nil
}

func (store *ClientStore) SetKeypair(localKeypair *encryption.Keypair) error {
	store.Keypair = localKeypair
	return nil
}

func (store *ClientStore) Open(repository string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("Open(%s): %s", repository, time.Since(t0))
	}()

	addr := repository[9:]
	if !strings.Contains(addr, ":") {
		addr = addr + ":9876"
	}

	err := store.connect(addr)
	if err != nil {
		return err
	}

	result, err := store.sendRequest("ReqOpen", nil)
	if err != nil {
		return err
	}

	store.config = result.Payload.(network.ResOpen).StoreConfig

	return nil

}

func (store *ClientStore) Configuration() storage.StoreConfig {
	return store.config
}

func (store *ClientStore) Transaction() (storage.Transaction, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("Transaction(): %s", time.Since(t0))
	}()

	result, err := store.sendRequest("ReqTransaction", nil)
	if err != nil {
		return nil, err
	}

	Uuid, err := result.Payload.(network.ResTransaction).Uuid, result.Payload.(network.ResTransaction).Err
	if err != nil {
		return nil, err
	}
	tx := &ClientTransaction{}
	tx.Uuid = Uuid
	tx.store = store
	return tx, nil
}

func (store *ClientStore) GetIndexes() ([]string, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("GetIndexes(): %s", time.Since(t0))
	}()

	result, err := store.sendRequest("ReqGetIndexes", nil)
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResGetIndexes).Indexes, result.Payload.(network.ResGetIndexes).Err
}

func (store *ClientStore) GetIndex(Uuid string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("GetIndex(%s): %s", Uuid, time.Since(t0))
	}()

	result, err := store.sendRequest("ReqGetIndex", network.ReqGetIndex{
		Uuid: Uuid,
	})
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResGetIndex).Data, result.Payload.(network.ResGetIndex).Err
}

func (store *ClientStore) GetObject(checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("GetObject(%s): %s", checksum, time.Since(t0))
	}()

	result, err := store.sendRequest("ReqGetObject", network.ReqGetObject{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResGetObject).Data, result.Payload.(network.ResGetObject).Err
}

func (store *ClientStore) GetChunk(checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("GetChunk(%s): %s", checksum, time.Since(t0))
	}()

	result, err := store.sendRequest("ReqGetChunk", network.ReqGetChunk{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResGetChunk).Data, result.Payload.(network.ResGetChunk).Err
}

func (store *ClientStore) Purge(id string) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("Purge(%s): %s", id, time.Since(t0))
	}()

	result, err := store.sendRequest("ReqPurge", network.ReqPurge{
		Uuid: id,
	})
	if err != nil {
		return err
	}

	return result.Payload.(network.ResPurge).Err
}

//////

func (transaction *ClientTransaction) GetUuid() string {
	return transaction.Uuid
}
func (transaction *ClientTransaction) ReferenceChunks(keys []string) ([]bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("tx[%s].ReferenceChunks([%d keys]): %s", transaction.GetUuid(), len(keys), time.Since(t0))
	}()

	store := transaction.store
	result, err := store.sendRequest("ReqReferenceChunks", network.ReqReferenceChunks{
		Transaction: transaction.GetUuid(),
		Keys:        keys,
	})
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResReferenceChunks).Exists, result.Payload.(network.ResReferenceChunks).Err
}

func (transaction *ClientTransaction) ReferenceObjects(keys []string) ([]bool, error) {
	t0 := time.Now()
	defer func() {
		logger.Profile("tx[%s].ReferenceObjects([%d keys]): %s", transaction.GetUuid(), len(keys), time.Since(t0))
	}()

	store := transaction.store
	result, err := store.sendRequest("ReqReferenceObjects", network.ReqReferenceObjects{
		Transaction: transaction.GetUuid(),
		Keys:        keys,
	})
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResReferenceObjects).Exists, result.Payload.(network.ResReferenceObjects).Err
}

func (transaction *ClientTransaction) PutObject(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("tx[%s].PutObject(%s) <- %d bytes: %s", transaction.GetUuid(), checksum, len(data), time.Since(t0))
	}()

	store := transaction.store
	result, err := store.sendRequest("ReqPutObject", network.ReqPutObject{
		Transaction: transaction.GetUuid(),
		Checksum:    checksum,
		Data:        data,
	})
	if err != nil {
		return err
	}

	return result.Payload.(network.ResPutObject).Err
}

func (transaction *ClientTransaction) PutChunk(checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("tx[%s].PutChunk(%s) <- %d bytes: %s", transaction.GetUuid(), checksum, len(data), time.Since(t0))
	}()

	store := transaction.store
	result, err := store.sendRequest("ReqPutChunk", network.ReqPutChunk{
		Transaction: transaction.GetUuid(),
		Checksum:    checksum,
		Data:        data,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResPutChunk).Err
}

func (transaction *ClientTransaction) PutIndex(data []byte) error {
	t0 := time.Now()
	defer func() {
		logger.Profile("tx[%s].PutIndex() <- %d bytes: %s", transaction.GetUuid(), len(data), time.Since(t0))
	}()

	store := transaction.store
	result, err := store.sendRequest("ReqPutIndex", network.ReqPutIndex{
		Transaction: transaction.GetUuid(),
		Data:        data,
	})
	if err != nil {
		return err
	}

	return result.Payload.(network.ResPutIndex).Err
}

func (transaction *ClientTransaction) Commit() error {
	t0 := time.Now()
	defer func() {
		logger.Profile("tx[%s].Commit(): %s", transaction.GetUuid(), time.Since(t0))
	}()

	store := transaction.store
	result, err := store.sendRequest("ReqCommit", network.ReqCommit{
		Transaction: transaction.GetUuid(),
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResCommit).Err
}