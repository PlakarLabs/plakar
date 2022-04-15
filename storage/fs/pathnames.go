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

func (repository *FSStore) PathPurge() string {
	return fmt.Sprintf("%s/purge", repository.root)
}

func (repository *FSStore) PathChunks() string {
	return fmt.Sprintf("%s/chunks", repository.root)
}

func (repository *FSStore) PathObjects() string {
	return fmt.Sprintf("%s/objects", repository.root)
}

func (repository *FSStore) PathTransactions() string {
	return fmt.Sprintf("%s/transactions", repository.root)
}

func (repository *FSStore) PathIndexes() string {
	return fmt.Sprintf("%s/snapshots", repository.root)
}

func (repository *FSStore) PathChunkBucket(checksum string) string {
	return fmt.Sprintf("%s/chunks/%s", repository.root, checksum[0:2])
}

func (repository *FSStore) PathObjectBucket(checksum string) string {
	return fmt.Sprintf("%s/objects/%s", repository.root, checksum[0:2])
}

func (repository *FSStore) PathIndexBucket(checksum string) string {
	return fmt.Sprintf("%s/snapshots/%s", repository.root, checksum[0:2])
}

func (repository *FSStore) PathChunk(checksum string) string {
	return fmt.Sprintf("%s/%s", repository.PathChunkBucket(checksum), checksum)
}

func (repository *FSStore) PathObject(checksum string) string {
	return fmt.Sprintf("%s/%s", repository.PathObjectBucket(checksum), checksum)
}

func (repository *FSStore) PathIndex(checksum string) string {
	return fmt.Sprintf("%s/%s", repository.PathIndexBucket(checksum), checksum)
}

func (repository *FSStore) PathIndexChunks(id string) string {
	return fmt.Sprintf("%s/chunks", repository.PathIndex(id))
}

func (repository *FSStore) PathIndexChunkBucket(id string, checksum string) string {
	return fmt.Sprintf("%s/%s", repository.PathIndexChunks(id), checksum[0:2])
}

func (repository *FSStore) PathIndexChunk(id string, checksum string) string {
	return fmt.Sprintf("%s/%s", repository.PathIndexChunkBucket(id, checksum), checksum)
}

func (repository *FSStore) PathIndexObjects(id string) string {
	return fmt.Sprintf("%s/objects", repository.PathIndex(id))
}

func (repository *FSStore) PathIndexObjectBucket(id string, checksum string) string {
	return fmt.Sprintf("%s/%s", repository.PathIndexObjects(id), checksum[0:2])
}

func (repository *FSStore) PathIndexObject(id string, checksum string) string {
	return fmt.Sprintf("%s/%s", repository.PathIndexObjectBucket(id, checksum), checksum)
}

func (transaction *FSTransaction) Path() string {
	return fmt.Sprintf("%s/%s/%s", transaction.repository.PathTransactions(),
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
