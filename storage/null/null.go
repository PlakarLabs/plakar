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
	"strings"
	"time"

	"github.com/PlakarLabs/plakar/cache"
	"github.com/PlakarLabs/plakar/storage"

	"github.com/google/uuid"
)

type NullRepository struct {
	config storage.RepositoryConfig

	Cache *cache.Cache

	Repository string
	root       string

	//storage.RepositoryBackend
}

type NullTransaction struct {
	Uuid       uuid.UUID
	repository NullRepository
	//prepared   bool

	//storage.TransactionBackend
}

func init() {
	storage.Register("null", NewNullRepository)
}

func NewNullRepository() storage.RepositoryBackend {
	return &NullRepository{}
}

func (repository *NullRepository) Create(location string, config storage.RepositoryConfig) error {
	return nil
}

func (repository *NullRepository) Open(location string) error {
	if strings.HasPrefix(location, "null://") {
		location = location[7:]
	}

	repositoryConfig := storage.RepositoryConfig{}
	repositoryConfig.Version = storage.VERSION
	repositoryConfig.RepositoryID = uuid.Must(uuid.NewRandom())
	repositoryConfig.CreationTime = time.Now()
	repositoryConfig.Hashing = "sha256"
	repositoryConfig.Chunking = "fastcdc"
	repositoryConfig.ChunkingMin = 512 << 10
	repositoryConfig.ChunkingNormal = 1024 << 10
	repositoryConfig.ChunkingMax = 8192 << 10

	repository.root = location

	repository.config = repositoryConfig

	return nil
}

func (repository *NullRepository) Configuration() storage.RepositoryConfig {
	return repository.config
}

func (repository *NullRepository) Transaction(indexID uuid.UUID) (storage.TransactionBackend, error) {
	tx := &NullTransaction{}
	tx.Uuid = indexID
	tx.repository = *repository
	return tx, nil
}

func (repository *NullRepository) GetSnapshots() ([]uuid.UUID, error) {
	return []uuid.UUID{}, nil
}

func (repository *NullRepository) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	return []byte{}, nil
}

func (repository *NullRepository) GetBlobs() ([][32]byte, error) {
	return [][32]byte{}, nil
}

func (repository *NullRepository) GetBlob(checksum [32]byte) ([]byte, error) {
	return []byte{}, nil
}

func (repository *NullRepository) DeleteBlob(checksum [32]byte) error {
	return nil
}

func (repository *NullRepository) PutSnapshot(indexID uuid.UUID, data []byte) error {
	return nil
}

func (repository *NullRepository) PutBlob(checksum [32]byte, data []byte) error {
	return nil
}

func (repository *NullRepository) GetObjects() ([][32]byte, error) {
	return [][32]byte{}, nil
}

func (repository *NullRepository) PutObject(checksum [32]byte) error {
	return nil
}

func (repository *NullRepository) DeleteObject(checksum [32]byte) error {
	return nil
}

func (repository *NullRepository) GetChunks() ([][32]byte, error) {
	return [][32]byte{}, nil
}

func (repository *NullRepository) GetChunk(checksum [32]byte) ([]byte, error) {
	return []byte{}, nil
}

func (repository *NullRepository) PutChunk(checksum [32]byte, data []byte) error {
	return nil
}

func (repository *NullRepository) DeleteChunk(checksum [32]byte) error {
	return nil
}

func (repository *NullRepository) CheckObject(checksum [32]byte) (bool, error) {
	return false, nil
}

func (repository *NullRepository) CheckChunk(checksum [32]byte) (bool, error) {
	return false, nil
}

func (repository *NullRepository) DeleteSnapshot(indexID uuid.UUID) error {
	return nil
}

func (repository *NullRepository) Close() error {
	return nil
}

func (transaction *NullTransaction) GetUuid() uuid.UUID {
	return transaction.Uuid
}

func (transaction *NullTransaction) Commit(data []byte) error {
	return nil
}
