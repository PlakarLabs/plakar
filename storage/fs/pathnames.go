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

	"github.com/google/uuid"
)

func pathnameExists(pathname string) bool {
	_, err := os.Stat(pathname)
	return !os.IsNotExist(err)
}

func (repository *FSRepository) PathPurge() string {
	return fmt.Sprintf("%s/purge", repository.root)
}

func (repository *FSRepository) PathChunks() string {
	return fmt.Sprintf("%s/chunks", repository.root)
}

func (repository *FSRepository) PathObjects() string {
	return fmt.Sprintf("%s/objects", repository.root)
}

func (repository *FSRepository) PathTransactions() string {
	return fmt.Sprintf("%s/transactions", repository.root)
}

func (repository *FSRepository) PathIndexes() string {
	return fmt.Sprintf("%s/snapshots", repository.root)
}

func (repository *FSRepository) PathChunkBucket(checksum [32]byte) string {
	return fmt.Sprintf("%s/chunks/%02x", repository.root, checksum[0])
}

func (repository *FSRepository) PathObjectBucket(checksum [32]byte) string {
	return fmt.Sprintf("%s/objects/%02x", repository.root, checksum[0])
}

func (repository *FSRepository) PathIndexBucket(indexID uuid.UUID) string {
	return fmt.Sprintf("%s/snapshots/%s", repository.root, indexID.String()[:2])
}

func (repository *FSRepository) PathChunk(checksum [32]byte) string {
	return fmt.Sprintf("%s/%064x", repository.PathChunkBucket(checksum), checksum)
}

func (repository *FSRepository) PathObject(checksum [32]byte) string {
	return fmt.Sprintf("%s/%064x", repository.PathObjectBucket(checksum), checksum)
}

func (repository *FSRepository) PathIndex(indexID uuid.UUID) string {
	return fmt.Sprintf("%s/%s", repository.PathIndexBucket(indexID), indexID)
}

func (repository *FSRepository) PathIndexChunks(indexID uuid.UUID) string {
	return fmt.Sprintf("%s/chunks", repository.PathIndex(indexID))
}

func (repository *FSRepository) PathIndexChunkBucket(indexID uuid.UUID, checksum [32]byte) string {
	return fmt.Sprintf("%s/%02x", repository.PathIndexChunks(indexID), checksum[0])
}

func (repository *FSRepository) PathIndexChunk(indexID uuid.UUID, checksum [32]byte) string {
	return fmt.Sprintf("%s/%064x", repository.PathIndexChunkBucket(indexID, checksum), checksum)
}

func (repository *FSRepository) PathIndexObjects(indexID uuid.UUID) string {
	return fmt.Sprintf("%s/objects", repository.PathIndex(indexID))
}

func (repository *FSRepository) PathIndexObjectBucket(indexID uuid.UUID, checksum [32]byte) string {
	return fmt.Sprintf("%s/%02x", repository.PathIndexObjects(indexID), checksum[0])
}

func (repository *FSRepository) PathIndexObject(indexID uuid.UUID, checksum [32]byte) string {
	return fmt.Sprintf("%s/%064x", repository.PathIndexObjectBucket(indexID, checksum), checksum)
}

func (transaction *FSTransaction) Path() string {
	return fmt.Sprintf("%s/%s/%s", transaction.repository.PathTransactions(),
		transaction.Uuid.String()[:2], transaction.Uuid.String())
}

func (transaction *FSTransaction) PathObjects() string {
	return fmt.Sprintf("%s/objects", transaction.Path())
}

func (transaction *FSTransaction) PathObjectBucket(checksum [32]byte) string {
	return fmt.Sprintf("%s/%02x", transaction.PathObjects(), checksum[0])
}

func (transaction *FSTransaction) PathObject(checksum [32]byte) string {
	return fmt.Sprintf("%s/%064x", transaction.PathObjectBucket(checksum), checksum)
}

func (transaction *FSTransaction) PathChunks() string {
	return fmt.Sprintf("%s/chunks", transaction.Path())
}

func (transaction *FSTransaction) PathChunkBucket(checksum [32]byte) string {
	return fmt.Sprintf("%s/%02x", transaction.PathChunks(), checksum[0])
}

func (transaction *FSTransaction) PathChunk(checksum [32]byte) string {
	return fmt.Sprintf("%s/%64x", transaction.PathChunkBucket(checksum), checksum)
}
