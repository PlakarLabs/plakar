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

type StoreConfig struct {
	Uuid       string
	Encrypted  string
	Compressed string
}

type StoreBackend interface {
	Create(repository string, configuration StoreConfig) error
	Open(repository string) error
	Configuration() StoreConfig

	Transaction() (TransactionBackend, error)

	GetIndexes() ([]string, error)
	GetIndex(id string) ([]byte, error)
	GetObject(checksum string) ([]byte, error)
	GetChunk(checksum string) ([]byte, error)

	CheckObject(checksum string) (bool, error)
	CheckChunk(checksum string) (bool, error)

	Purge(id string) error
}

type TransactionBackend interface {
	GetUuid() string

	ReferenceObjects(keys []string) ([]bool, error)
	PutObject(checksum string, data []byte) error

	ReferenceChunks(keys []string) ([]bool, error)
	PutChunk(checksum string, data []byte) error

	PutIndex(data []byte) error
	Commit() error
}
