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
	"path/filepath"

	"github.com/google/uuid"
)

func (repository *Repository) PathTmp() string {
	return filepath.Join(repository.root, "tmp")
}

func (repository *Repository) PathBlobs() string {
	return filepath.Join(repository.root, "blobs")
}

func (repository *Repository) PathIndexes() string {
	return filepath.Join(repository.root, "indexes")
}

func (repository *Repository) PathPackfiles() string {
	return filepath.Join(repository.root, "packfiles")
}

func (repository *Repository) PathLocks() string {
	return filepath.Join(repository.root, "locks")
}

func (repository *Repository) PathSnapshots() string {
	return filepath.Join(repository.root, "snapshots")
}

func (repository *Repository) PathBlobBucket(checksum [32]byte) string {
	return filepath.Join(repository.root, "blobs", fmt.Sprintf("%02x", checksum[0]))
}

func (repository *Repository) PathIndexBucket(checksum [32]byte) string {
	return filepath.Join(repository.root, "indexes", fmt.Sprintf("%02x", checksum[0]))
}

func (repository *Repository) PathPackfileBucket(checksum [32]byte) string {
	return filepath.Join(repository.root, "packfiles", fmt.Sprintf("%02x", checksum[0]))
}

func (repository *Repository) PathSnapshotBucket(indexID uuid.UUID) string {
	return filepath.Join(repository.root, "snapshots", indexID.String()[:2])
}

func (repository *Repository) PathBlob(checksum [32]byte) string {
	return filepath.Join(repository.PathBlobBucket(checksum), fmt.Sprintf("%064x", checksum))
}

func (repository *Repository) PathIndex(checksum [32]byte) string {
	return filepath.Join(repository.PathIndexBucket(checksum), fmt.Sprintf("%064x", checksum))
}

func (repository *Repository) PathPackfile(checksum [32]byte) string {
	return filepath.Join(repository.PathPackfileBucket(checksum), fmt.Sprintf("%064x", checksum))
}

func (repository *Repository) PathLock(indexID uuid.UUID) string {
	return filepath.Join(repository.PathLocks(), indexID.String())
}

func (repository *Repository) PathSnapshot(indexID uuid.UUID) string {
	return filepath.Join(repository.PathSnapshotBucket(indexID), indexID.String())
}
