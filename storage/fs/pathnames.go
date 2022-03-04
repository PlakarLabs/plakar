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

func pathnameExists(pathname string) bool {
	_, err := os.Stat(pathname)
	return !os.IsNotExist(err)
}

func (store *FSStore) PathPurge() string {
	return fmt.Sprintf("%s/purge", store.root)
}

func (store *FSStore) PathChunks() string {
	return fmt.Sprintf("%s/chunks", store.root)
}

func (store *FSStore) PathObjects() string {
	return fmt.Sprintf("%s/objects", store.root)
}

func (store *FSStore) PathTransactions() string {
	return fmt.Sprintf("%s/transactions", store.root)
}

func (store *FSStore) PathIndexes() string {
	return fmt.Sprintf("%s/snapshots", store.root)
}

func (store *FSStore) PathChunkBucket(checksum string) string {
	return fmt.Sprintf("%s/chunks/%s", store.root, checksum[0:2])
}

func (store *FSStore) PathObjectBucket(checksum string) string {
	return fmt.Sprintf("%s/objects/%s", store.root, checksum[0:2])
}

func (store *FSStore) PathIndexBucket(checksum string) string {
	return fmt.Sprintf("%s/snapshots/%s", store.root, checksum[0:2])
}

func (store *FSStore) PathChunk(checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathChunkBucket(checksum), checksum)
}

func (store *FSStore) PathObject(checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathObjectBucket(checksum), checksum)
}

func (store *FSStore) PathIndex(checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathIndexBucket(checksum), checksum)
}

func (store *FSStore) PathIndexChunks(id string) string {
	return fmt.Sprintf("%s/chunks", store.PathIndex(id))
}

func (store *FSStore) PathIndexChunkBucket(id string, checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathIndexChunks(id), checksum[0:2])
}

func (store *FSStore) PathIndexChunk(id string, checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathIndexChunkBucket(id, checksum), checksum)
}

func (store *FSStore) PathIndexObjects(id string) string {
	return fmt.Sprintf("%s/objects", store.PathIndex(id))
}

func (store *FSStore) PathIndexObjectBucket(id string, checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathIndexObjects(id), checksum[0:2])
}

func (store *FSStore) PathIndexObject(id string, checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathIndexObjectBucket(id, checksum), checksum)
}

func (transaction *FSTransaction) Path() string {
	return fmt.Sprintf("%s/%s/%s", transaction.store.PathTransactions(),
		transaction.Uuid[0:2], transaction.Uuid)
}

func (transaction *FSTransaction) PathObjects() string {
	return fmt.Sprintf("%s/objects", transaction.Path())
}

func (transaction *FSTransaction) PathObjectBucket(checksum string) string {
	return fmt.Sprintf("%s/%s", transaction.PathObjects(), checksum[0:2])
}

func (transaction *FSTransaction) PathObject(checksum string) string {
	return fmt.Sprintf("%s/%s", transaction.PathObjectBucket(checksum), checksum)
}

func (transaction *FSTransaction) PathChunks() string {
	return fmt.Sprintf("%s/chunks", transaction.Path())
}

func (transaction *FSTransaction) PathChunkBucket(checksum string) string {
	return fmt.Sprintf("%s/%s", transaction.PathChunks(), checksum[0:2])
}

func (transaction *FSTransaction) PathChunk(checksum string) string {
	return fmt.Sprintf("%s/%s", transaction.PathChunkBucket(checksum), checksum)
}
