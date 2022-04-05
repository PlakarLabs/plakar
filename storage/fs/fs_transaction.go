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

package fs

import (
	"fmt"
	"os"
)

func (transaction *FSTransaction) CreateChunkBucket(checksum string) error {
	transaction.muChunkBucket.Lock()
	defer transaction.muChunkBucket.Unlock()

	bucket := checksum[0:2]
	if _, exists := transaction.chunkBucket[bucket]; !exists {
		err := os.Mkdir(transaction.PathChunkBucket(checksum), 0700)
		if err != nil {
			return err
		}
		transaction.chunkBucket[bucket] = true
	}
	return nil
}

func (transaction *FSTransaction) CreateObjectBucket(checksum string) error {
	transaction.muObjectBucket.Lock()
	defer transaction.muObjectBucket.Unlock()

	bucket := checksum[0:2]
	if _, exists := transaction.objectBucket[bucket]; !exists {
		err := os.Mkdir(transaction.PathObjectBucket(checksum), 0700)
		if err != nil {
			return err
		}
		transaction.objectBucket[bucket] = true
	}
	return nil
}

func (transaction *FSTransaction) GetUuid() string {
	return transaction.Uuid
}

func (transaction *FSTransaction) prepare() {
	os.MkdirAll(transaction.store.root, 0700)
	os.MkdirAll(fmt.Sprintf("%s/%s", transaction.store.PathTransactions(),
		transaction.Uuid[0:2]), 0700)
	os.MkdirAll(transaction.Path(), 0700)
	os.MkdirAll(fmt.Sprintf("%s/chunks", transaction.Path()), 0700)
	os.MkdirAll(fmt.Sprintf("%s/objects", transaction.Path()), 0700)
}

func (transaction *FSTransaction) ReferenceChunks(keys []string) ([]bool, error) {
	ret := make([]bool, 0)
	for _, key := range keys {
		err := transaction.CreateChunkBucket(key)
		if err != nil {
			return nil, err
		}
		err = os.Link(transaction.store.PathChunk(key), transaction.PathChunk(key))
		if err != nil {
			if os.IsNotExist(err) {
				ret = append(ret, false)
			} else {
				ret = append(ret, true)
				transaction.chunksMutex.Lock()
				transaction.chunks[key] = true
				transaction.chunksMutex.Unlock()
			}
		} else {
			ret = append(ret, true)
			transaction.chunksMutex.Lock()
			transaction.chunks[key] = true
			transaction.chunksMutex.Unlock()
		}
	}

	return ret, nil
}

func (transaction *FSTransaction) ReferenceObjects(keys []string) ([]bool, error) {
	ret := make([]bool, 0)
	for _, key := range keys {
		err := transaction.CreateObjectBucket(key)
		if err != nil {
			return nil, err
		}
		err = os.Link(transaction.store.PathObject(key), transaction.PathObject(key))
		if err != nil {
			if os.IsNotExist(err) {
				ret = append(ret, false)
			} else {
				ret = append(ret, true)
				transaction.objectsMutex.Lock()
				transaction.objects[key] = true
				transaction.objectsMutex.Unlock()
			}
		} else {
			ret = append(ret, true)
			transaction.objectsMutex.Lock()
			transaction.objects[key] = true
			transaction.objectsMutex.Unlock()
		}
	}

	return ret, nil
}

func (transaction *FSTransaction) PutObject(checksum string, data []byte) error {
	store := transaction.store

	err := transaction.CreateObjectBucket(checksum)
	if err != nil {
		return err
	}

	err = store.PutObjectSafe(checksum, data, transaction.PathObject(checksum))
	if err != nil {
		return err
	}

	transaction.objectsMutex.Lock()
	transaction.objects[checksum] = true
	transaction.objectsMutex.Unlock()
	return nil
}

func (transaction *FSTransaction) PutChunk(checksum string, data []byte) error {
	store := transaction.store

	err := transaction.CreateChunkBucket(checksum)
	if err != nil {
		return err
	}

	err = store.PutChunkSafe(checksum, data, transaction.PathChunk(checksum))
	if err != nil {
		return err
	}

	transaction.chunksMutex.Lock()
	transaction.chunks[checksum] = true
	transaction.chunksMutex.Unlock()
	return nil
}

func (transaction *FSTransaction) PutMetadata(data []byte) error {
	f, err := os.Create(fmt.Sprintf("%s/METADATA", transaction.Path()))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (transaction *FSTransaction) PutIndex(data []byte) error {
	f, err := os.Create(fmt.Sprintf("%s/INDEX", transaction.Path()))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (transaction *FSTransaction) Commit() error {
	return os.Rename(transaction.Path(), transaction.store.PathIndex(transaction.Uuid))
}
