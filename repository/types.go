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

package repository

import (
	"os"
	"time"
)

type Store interface {
	Init()
	Transaction() Transaction
	Snapshot(id string) (*Snapshot, error)
	Snapshots() ([]string, error)

	IndexGet(id string) ([]byte, error)
	ObjectGet(checksum string) ([]byte, error)
	ChunkGet(checksum string) ([]byte, error)

	Purge(id string) error
}

type Transaction interface {
	Snapshot() *Snapshot

	ObjectMark(key string) bool
	ObjectPut(checksum string, buf string) error

	ChunksMark(keys []string) map[string]bool
	ChunkPut(checksum string, buf string) error

	IndexPut(buf string) error
	Commit(snapshot *Snapshot) (*Snapshot, error)
}

type FileInfo struct {
	Name    string
	Size    int64
	Mode    os.FileMode
	ModTime time.Time
	Dev     uint64
	Ino     uint64
	Uid     uint64
	Gid     uint64

	path string
}

type Chunk struct {
	Checksum string
	Start    uint
	Length   uint
}

type Object struct {
	Checksum    string
	Chunks      []*Chunk
	ContentType string

	fp   *os.File
	path string
}

type SnapshotStorage struct {
	Uuid         string
	CreationTime time.Time
	Version      string

	Directories map[string]*FileInfo
	Files       map[string]*FileInfo
	NonRegular  map[string]*FileInfo
	Sums        map[string]string
	Objects     map[string]*Object
	Chunks      map[string]*Chunk

	Size     uint64
	RealSize uint64
}

type Snapshot struct {
	Uuid         string
	CreationTime time.Time
	Version      string

	Directories map[string]*FileInfo
	Files       map[string]*FileInfo
	NonRegular  map[string]*FileInfo
	Sums        map[string]string
	Objects     map[string]*Object
	Chunks      map[string]*Chunk

	Size     uint64
	RealSize uint64

	Quiet bool

	BackingStore       Store
	BackingTransaction Transaction
	SkipDirs           []string
}

type SnapshotSummary struct {
	Uuid         string
	CreationTime time.Time
	Version      string

	Directories uint64
	Files       uint64
	NonRegular  uint64
	Sums        uint64
	Objects     uint64
	Chunks      uint64

	Size     uint64
	RealSize uint64
}
