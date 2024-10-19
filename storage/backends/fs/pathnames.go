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
	"encoding/hex"
	"fmt"
	"path/filepath"
)

func (repository *Repository) PathTmp() string {
	return filepath.Join(repository.root, "tmp")
}

func (repository *Repository) PathStates() string {
	return filepath.Join(repository.root, "states")
}

func (repository *Repository) PathPackfiles() string {
	return filepath.Join(repository.root, "packfiles")
}

func (repository *Repository) PathSnapshots() string {
	return filepath.Join(repository.root, "snapshots")
}

func (repository *Repository) PathStateBucket(checksum [32]byte) string {
	return filepath.Join(repository.root, "states", fmt.Sprintf("%02x", checksum[0]))
}

func (repository *Repository) PathPackfileBucket(checksum [32]byte) string {
	return filepath.Join(repository.root, "packfiles", fmt.Sprintf("%02x", checksum[0]))
}

func (repository *Repository) PathSnapshotBucket(snapshotID [32]byte) string {
	return filepath.Join(repository.root, "snapshots", fmt.Sprintf("%02x", snapshotID[0]))
}

func (repository *Repository) PathState(checksum [32]byte) string {
	return filepath.Join(repository.PathStateBucket(checksum), fmt.Sprintf("%064x", checksum))
}

func (repository *Repository) PathPackfile(checksum [32]byte) string {
	return filepath.Join(repository.PathPackfileBucket(checksum), fmt.Sprintf("%064x", checksum))
}

func (repository *Repository) PathSnapshot(snapshotID [32]byte) string {
	return filepath.Join(repository.PathSnapshotBucket(snapshotID), hex.EncodeToString(snapshotID[:]))
}
