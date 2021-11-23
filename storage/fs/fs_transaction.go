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
	"sync"
)

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
	if !transaction.prepared {
		transaction.prepare()
	}

	ret := make([]bool, 0)
	for _, key := range keys {
		os.Mkdir(transaction.PathChunkBucket(key), 0700)
		err := os.Link(transaction.store.PathChunk(key), transaction.PathChunk(key))
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
	if !transaction.prepared {
		transaction.prepare()
	}

	ret := make([]bool, 0)
	for _, key := range keys {
		os.Mkdir(transaction.PathObjectBucket(key), 0700)
		err := os.Link(transaction.store.PathObject(key), transaction.PathObject(key))
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
	if !transaction.prepared {
		transaction.prepare()
	}

	os.Mkdir(transaction.PathObjectBucket(checksum), 0700)
	f, err := os.Create(transaction.PathObject(checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	transaction.objectsMutex.Lock()
	transaction.objects[checksum] = true
	transaction.objectsMutex.Unlock()
	return nil
}

func (transaction *FSTransaction) PutChunk(checksum string, data []byte) error {
	if !transaction.prepared {
		transaction.prepare()
	}
	os.Mkdir(transaction.PathChunkBucket(checksum), 0700)
	f, err := os.Create(transaction.PathChunk(checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	transaction.chunksMutex.Lock()
	transaction.chunks[checksum] = true
	transaction.chunksMutex.Unlock()
	return nil
}

func (transaction *FSTransaction) PutIndex(data []byte) error {
	if !transaction.prepared {
		transaction.prepare()
	}
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
	if !transaction.prepared {
		transaction.prepare()
	}

	var wg sync.WaitGroup

	// first pass, link chunks to store
	parallelChunksMax := make(chan int, 64)
	for chunk, _ := range transaction.chunks {
		parallelChunksMax <- 1
		wg.Add(1)
		go func(chunk string) {
			if !transaction.store.chunkExists(chunk) {
				os.Mkdir(transaction.store.PathChunkBucket(chunk), 0700)
				os.Rename(transaction.PathChunk(chunk), transaction.store.PathChunk(chunk))
			} else {
				os.Remove(transaction.PathChunk(chunk))
			}
			os.Link(transaction.store.PathChunk(chunk), transaction.PathChunk(chunk))
			<-parallelChunksMax
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	// second pass, link objects to store
	parallelObjectsMax := make(chan int, 64)
	for object, _ := range transaction.objects {
		parallelObjectsMax <- 1
		wg.Add(1)
		go func(object string) {
			if !transaction.store.objectExists(object) {
				os.Mkdir(transaction.store.PathObjectBucket(object), 0700)
				os.Rename(transaction.PathObject(object), transaction.store.PathObject(object))
			} else {
				os.Remove(transaction.PathObject(object))
			}
			os.Link(transaction.store.PathObject(object), transaction.PathObject(object))
			<-parallelObjectsMax
			wg.Done()
		}(object)
	}
	wg.Wait()

	// final pass, move snapshot to store
	os.Mkdir(transaction.store.PathIndexBucket(transaction.Uuid), 0700)
	os.Rename(transaction.Path(), transaction.store.PathIndex(transaction.Uuid))

	return nil
}
