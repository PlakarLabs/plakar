/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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
	"bytes"
	"io"

	"github.com/PlakarKorp/plakar/storage"
)

type Repository struct {
	config     storage.Configuration
	Repository string
}

func init() {
	storage.Register("null", NewRepository)
}

func NewRepository() storage.Backend {
	return &Repository{}
}

func (repository *Repository) Create(location string, config storage.Configuration) error {
	return nil
}

func (repository *Repository) Open(location string) error {
	repositoryConfig := storage.NewConfiguration()
	repository.config = *repositoryConfig

	return nil
}

func (repository *Repository) Close() error {
	return nil
}

func (repository *Repository) Configuration() storage.Configuration {
	return repository.config
}

// snapshots
func (repository *Repository) GetSnapshots() ([][32]byte, error) {
	return [][32]byte{}, nil
}

func (repository *Repository) PutSnapshot(snapshotID [32]byte, data []byte) error {
	return nil
}

func (repository *Repository) GetSnapshot(snapshotID [32]byte) ([]byte, error) {
	return []byte{}, nil
}

func (repository *Repository) DeleteSnapshot(snapshotID [32]byte) error {
	return nil
}

// states
func (repository *Repository) GetStates() ([][32]byte, error) {
	return [][32]byte{}, nil
}

func (repository *Repository) PutState(checksum [32]byte, rd io.Reader, size uint64) error {
	return nil
}

func (repository *Repository) GetState(checksum [32]byte) (io.Reader, uint64, error) {
	return bytes.NewBuffer([]byte{}), 0, nil
}

func (repository *Repository) DeleteState(checksum [32]byte) error {
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([][32]byte, error) {
	return [][32]byte{}, nil
}

func (repository *Repository) PutPackfile(checksum [32]byte, rd io.Reader, size uint64) error {
	return nil
}

func (repository *Repository) GetPackfile(checksum [32]byte) (io.Reader, uint64, error) {
	return bytes.NewBuffer([]byte{}), 0, nil
}

func (repository *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) (io.Reader, uint32, error) {
	return bytes.NewBuffer([]byte{}), 0, nil
}

func (repository *Repository) DeletePackfile(checksum [32]byte) error {
	return nil
}

func (repository *Repository) Commit(snapshotID [32]byte, data []byte) error {
	return nil
}
