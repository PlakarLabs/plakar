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
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/storage"
	"golang.org/x/crypto/ssh"
)

func (store *ClientStore) connect(addr string) error {
	config := &ssh.ClientConfig{
		User: "username",
		Auth: []ssh.AuthMethod{
			// ...
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return err
	}

	// pass public key as part of extra data
	//pk := store.GetKeypair().PublicKey
	//fmt.Println(pk)

	sshChannel, sshReq, err := conn.OpenChannel("plakar", nil)
	if err != nil {
		log.Fatal(err)
	}

	store.conn = conn
	store.sshChannel = sshChannel
	store.sshReq = sshReq

	return err
}

func (store *ClientStore) Request(name string, data []byte) ([]byte, error) {
	var msg *ssh.Request
	store.mu.Lock()
	_, err := store.sshChannel.SendRequest(name, false, data)
	if err == nil {
		msg = <-store.sshReq
	}
	store.mu.Unlock()
	if err != nil {
		return nil, err
	}
	fmt.Println(msg)
	if msg.Type != name {
		panic("mismatch")
	}
	return msg.Payload, nil
}

func (store *ClientStore) Create(repository string, config storage.StoreConfig) error {
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
	fmt.Println("connect", repository)

	addr := repository[9:]
	if !strings.Contains(addr, ":") {
		addr = addr + ":9876"
	}

	err := store.connect(addr)
	if err != nil {
		return err
	}

	msg, err := store.Request("Open", nil)
	if err != nil {
		return err
	}

	err = json.Unmarshal(msg, &store.config)
	if err != nil {
		return err
	}
	fmt.Println(store.Configuration())
	return nil

}

func (store *ClientStore) Configuration() storage.StoreConfig {
	return store.config
}

func (store *ClientStore) Transaction() storage.Transaction {
	msg, err := store.Request("Transaction", nil)
	if err != nil {
		return nil
	}

	tx := &ClientTransaction{}
	tx.Uuid = string(msg)
	tx.store = *store
	return tx
}

func (store *ClientStore) GetIndexes() ([]string, error) {
	msg, err := store.Request("GetIndexes", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Indexes []string
		err     error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return nil, err
	}
	return result.Indexes, result.err
}

func (store *ClientStore) GetIndex(Uuid string) ([]byte, error) {
	msg, err := store.Request("GetIndex", []byte(Uuid))

	if err != nil {
		return nil, err
	}

	var result struct {
		Data []byte
		Err  error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return nil, err
	}

	return result.Data, result.Err
}

func (store *ClientStore) GetObject(checksum string) ([]byte, error) {
	msg, err := store.Request("GetObject", []byte(checksum))
	if err != nil {
		return nil, err
	}

	var result struct {
		Data []byte
		Err  error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return nil, err
	}
	return result.Data, result.Err
}

func (store *ClientStore) GetChunk(checksum string) ([]byte, error) {
	msg, err := store.Request("GetChunk", []byte(checksum))
	if err != nil {
		return nil, err
	}

	var result struct {
		Data []byte
		Err  error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return nil, err
	}
	return result.Data, result.Err
}

func (store *ClientStore) Purge(id string) error {
	msg, err := store.Request("Purge", []byte(id))
	if err != nil {
		return err
	}

	var result struct {
		Err error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return err
	}
	return result.Err
}

//////

func (transaction *ClientTransaction) GetUuid() string {
	return transaction.Uuid
}
func (transaction *ClientTransaction) ReferenceChunks(keys []string) ([]bool, error) {
	fmt.Println("ReferenceChunks")
	store := transaction.store

	marshalled, m_err := json.Marshal(keys)
	if m_err != nil {
		return nil, m_err
	}

	msg, err := store.Request("ReferenceChunks", marshalled)
	if err != nil {
		return nil, err
	}

	var result struct {
		Exists []bool
		Err    error
	}

	err = json.Unmarshal(msg, &result)
	if err != nil {
		return nil, err
	}
	fmt.Println(result.Exists)
	return result.Exists, result.Err
}

func (transaction *ClientTransaction) ReferenceObjects(keys []string) ([]bool, error) {
	fmt.Println("ReferenceObject")
	store := transaction.store

	marshalled, m_err := json.Marshal(keys)
	if m_err != nil {
		return nil, m_err
	}

	msg, err := store.Request("ReferenceObjects", marshalled)
	if err != nil {
		return nil, err
	}

	var result struct {
		Exists []bool
		Err    error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return nil, err
	}
	return result.Exists, result.Err
}

func (transaction *ClientTransaction) PutObject(checksum string, data []byte) error {
	fmt.Println("PutObject")
	store := transaction.store

	object := struct {
		Checksum string
		Data     []byte
	}{checksum, data}
	marshalled, m_err := json.Marshal(&object)
	if m_err != nil {
		return m_err
	}

	msg, err := store.Request("PutObject", marshalled)
	if err != nil {
		return err
	}

	var result struct {
		Err error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return err
	}
	return result.Err
}

func (transaction *ClientTransaction) PutChunk(checksum string, data []byte) error {
	fmt.Println("PutChunk")
	store := transaction.store

	chunk := struct {
		Checksum string
		Data     []byte
	}{checksum, data}
	marshalled, m_err := json.Marshal(&chunk)
	if m_err != nil {
		return m_err
	}

	msg, err := store.Request("PutChunk", marshalled)
	if err != nil {
		return err
	}

	var result struct {
		Err error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return err
	}
	return result.Err
}

func (transaction *ClientTransaction) PutIndex(data []byte) error {
	fmt.Println("PutIndex")
	store := transaction.store

	msg, err := store.Request("PutIndex", data)
	if err != nil {
		return err
	}

	var result struct {
		Err error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return err
	}
	return result.Err
}

func (transaction *ClientTransaction) Commit() error {
	fmt.Println("Commit")
	store := transaction.store

	msg, err := store.Request("Commit", nil)
	if err != nil {
		return err
	}

	var result struct {
		Err error
	}
	err = json.Unmarshal(msg, &result)
	if err != nil {
		return err
	}
	return result.Err
}
