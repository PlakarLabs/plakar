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

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/network"
	"github.com/poolpOrg/plakar/storage"
)

func (store *ClientStore) connect(addr string) error {
	servAddr := ":9876"
	tcpAddr, err := net.ResolveTCPAddr("tcp", servAddr)
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

	return err
}

func (store *ClientStore) Create(repository string, config storage.StoreConfig) error {
	store.mu.Lock()
	defer store.mu.Unlock()
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
	store.mu.Lock()
	defer store.mu.Unlock()

	addr := repository[9:]
	if !strings.Contains(addr, ":") {
		addr = addr + ":9876"
	}

	err := store.connect(addr)
	if err != nil {
		return err
	}

	request := network.Request{
		Type:    "ReqOpen",
		Payload: nil,
	}
	err = store.encoder.Encode(&request)
	if err != nil {
		return err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
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
	store.mu.Lock()
	defer store.mu.Unlock()

	request := network.Request{
		Type:    "ReqTransaction",
		Payload: nil,
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	Uuid, err := result.Payload.(network.ResTransaction).Uuid, result.Payload.(network.ResTransaction).Err
	if err != nil {
		return nil, err
	}
	tx := &ClientTransaction{}
	tx.Uuid = Uuid
	tx.store = *store
	return tx, nil
}

func (store *ClientStore) GetIndexes() ([]string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	request := network.Request{
		Type:    "ReqGetIndexes",
		Payload: nil,
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResGetIndexes).Indexes, result.Payload.(network.ResGetIndexes).Err
}

func (store *ClientStore) GetIndex(Uuid string) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	request := network.Request{
		Type: "ReqGetIndex",
		Payload: network.ReqGetIndex{
			Uuid: Uuid,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResGetIndex).Data, result.Payload.(network.ResGetIndex).Err
}

func (store *ClientStore) GetObject(checksum string) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	request := network.Request{
		Type: "ReqGetObject",
		Payload: network.ReqGetObject{
			Checksum: checksum,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResGetObject).Data, result.Payload.(network.ResGetObject).Err
}

func (store *ClientStore) GetChunk(checksum string) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	request := network.Request{
		Type: "ReqGetChunk",
		Payload: network.ReqGetChunk{
			Checksum: checksum,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResGetChunk).Data, result.Payload.(network.ResGetChunk).Err
}

func (store *ClientStore) Purge(id string) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	request := network.Request{
		Type: "ReqPurge",
		Payload: network.ReqPurge{
			Uuid: id,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
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
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	store := &transaction.store

	request := network.Request{
		Type: "ReqReferenceChunks",
		Payload: network.ReqReferenceChunks{
			Transaction: transaction.GetUuid(),
			Keys:        keys,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResReferenceChunks).Exists, result.Payload.(network.ResReferenceChunks).Err
}

func (transaction *ClientTransaction) ReferenceObjects(keys []string) ([]bool, error) {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	store := &transaction.store

	request := network.Request{
		Type: "ReqReferenceObjects",
		Payload: network.ReqReferenceObjects{
			Transaction: transaction.GetUuid(),
			Keys:        keys,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return nil, err
	}

	return result.Payload.(network.ResReferenceObjects).Exists, result.Payload.(network.ResReferenceObjects).Err
}

func (transaction *ClientTransaction) PutObject(checksum string, data []byte) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	store := &transaction.store

	request := network.Request{
		Type: "ReqPutObject",
		Payload: network.ReqPutObject{
			Transaction: transaction.GetUuid(),
			Checksum:    checksum,
			Data:        data,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return err
	}

	return result.Payload.(network.ResPutObject).Err
}

func (transaction *ClientTransaction) PutChunk(checksum string, data []byte) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	store := &transaction.store

	request := network.Request{
		Type: "ReqPutChunk",
		Payload: network.ReqPutChunk{
			Transaction: transaction.GetUuid(),
			Checksum:    checksum,
			Data:        data,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return err
	}

	return result.Payload.(network.ResPutChunk).Err
}

func (transaction *ClientTransaction) PutIndex(data []byte) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	store := &transaction.store

	request := network.Request{
		Type: "ReqPutIndex",
		Payload: network.ReqPutIndex{
			Transaction: transaction.GetUuid(),
			Data:        data,
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return err
	}

	return result.Payload.(network.ResPutIndex).Err
}

func (transaction *ClientTransaction) Commit() error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	store := &transaction.store

	request := network.Request{
		Type: "ReqCommit",
		Payload: network.ReqCommit{
			Transaction: transaction.GetUuid(),
		},
	}
	err := store.encoder.Encode(&request)
	if err != nil {
		return err
	}

	result := network.Request{}
	err = store.decoder.Decode(&result)
	if err != nil {
		return err
	}

	return result.Payload.(network.ResCommit).Err
}
