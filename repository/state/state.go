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

package state

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION = 100

type Metadata struct {
	Version      uint32
	CreationTime time.Time
	Aggregate    bool
	Extends      []objects.Checksum
}

type Location struct {
	Packfile uint64
	Offset   uint32
	Length   uint32
}

type State struct {
	muChecksum   sync.Mutex
	checksumToId map[objects.Checksum]uint64
	IdToChecksum map[uint64]objects.Checksum

	muChunks sync.Mutex
	Chunks   map[uint64]Location

	muObjects sync.Mutex
	Objects   map[uint64]Location

	muFiles sync.Mutex
	Files   map[uint64]Location

	muDirectories sync.Mutex
	Directories   map[uint64]Location

	muDatas sync.Mutex
	Datas   map[uint64]Location

	muSnapshots sync.Mutex
	Snapshots   map[uint64]Location

	muSignatures sync.Mutex
	Signatures   map[uint64]Location

	muErrors sync.Mutex
	Errors   map[uint64]Location

	muDeletedSnapshots sync.Mutex
	DeletedSnapshots   map[uint64]time.Time

	Metadata Metadata

	dirty int32
}

func New() *State {
	return &State{
		IdToChecksum:     make(map[uint64]objects.Checksum),
		checksumToId:     make(map[objects.Checksum]uint64),
		Chunks:           make(map[uint64]Location),
		Objects:          make(map[uint64]Location),
		Files:            make(map[uint64]Location),
		Directories:      make(map[uint64]Location),
		Datas:            make(map[uint64]Location),
		Snapshots:        make(map[uint64]Location),
		Signatures:       make(map[uint64]Location),
		Errors:           make(map[uint64]Location),
		DeletedSnapshots: make(map[uint64]time.Time),
		Metadata: Metadata{
			Version:      VERSION,
			CreationTime: time.Now(),
			Aggregate:    false,
			Extends:      []objects.Checksum{},
		},
	}
}

func (st *State) Derive() *State {
	nst := New()
	nst.Metadata.Extends = st.Metadata.Extends
	return nst
}

func (st *State) rebuildChecksums() {
	st.muChecksum.Lock()
	defer st.muChecksum.Unlock()

	st.checksumToId = make(map[objects.Checksum]uint64)

	// Rebuild checksumToID by reversing the IDToChecksum map
	for id, checksum := range st.IdToChecksum {
		st.checksumToId[checksum] = id
	}
}

func (st *State) getOrCreateIdForChecksum(checksum objects.Checksum) uint64 {
	st.muChecksum.Lock()
	defer st.muChecksum.Unlock()

	if id, exists := st.checksumToId[checksum]; exists {
		return id
	}

	newID := uint64(len(st.IdToChecksum))
	st.checksumToId[checksum] = newID
	st.IdToChecksum[newID] = checksum
	return newID
}

func NewFromBytes(serialized []byte) (*State, error) {
	t0 := time.Now()
	defer func() {
		logger.Trace("state", "NewFromBytes(...): %s", time.Since(t0))
	}()

	if len(serialized) < 4 {
		return nil, fmt.Errorf("invalid state data")
	}

	serialized, versionBytes := serialized[:len(serialized)-4], serialized[len(serialized)-4:]
	version := binary.LittleEndian.Uint32(versionBytes)
	if version != VERSION {
		return nil, fmt.Errorf("invalid state version: %d", version)
	}

	var st State
	if err := msgpack.Unmarshal(serialized, &st); err != nil {
		return nil, err
	}

	st.rebuildChecksums()

	return &st, nil
}

func (st *State) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		logger.Trace("state", "Serialize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(st)
	if err != nil {
		return nil, err
	}

	versionBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(versionBytes, st.Metadata.Version)

	return append(serialized, versionBytes...), nil
}

func (st *State) Extends(stateID objects.Checksum) {
	st.Metadata.Extends = append(st.Metadata.Extends, stateID)
}

func (st *State) mergeLocationMaps(Type packfile.Type, deltaState *State) {
	var mapPtr *map[uint64]Location
	switch Type {
	case packfile.TYPE_SNAPSHOT:
		deltaState.muSnapshots.Lock()
		defer deltaState.muSnapshots.Unlock()
		mapPtr = &deltaState.Snapshots
	case packfile.TYPE_CHUNK:
		deltaState.muChunks.Lock()
		defer deltaState.muChunks.Unlock()
		mapPtr = &deltaState.Chunks
	case packfile.TYPE_OBJECT:
		deltaState.muObjects.Lock()
		defer deltaState.muObjects.Unlock()
		mapPtr = &deltaState.Objects
	case packfile.TYPE_FILE:
		deltaState.muFiles.Lock()
		defer deltaState.muFiles.Unlock()
		mapPtr = &deltaState.Files
	case packfile.TYPE_DIRECTORY:
		deltaState.muDirectories.Lock()
		defer deltaState.muDirectories.Unlock()
		mapPtr = &deltaState.Directories
	case packfile.TYPE_DATA:
		deltaState.muDatas.Lock()
		defer deltaState.muDatas.Unlock()
		mapPtr = &deltaState.Datas
	case packfile.TYPE_SIGNATURE:
		deltaState.muSignatures.Lock()
		defer deltaState.muSignatures.Unlock()
		mapPtr = &deltaState.Signatures
	case packfile.TYPE_ERROR:
		deltaState.muErrors.Lock()
		defer deltaState.muErrors.Unlock()
		mapPtr = &deltaState.Errors
	default:
		panic("invalid blob type")
	}

	for deltaBlobChecksumID, subpart := range *mapPtr {
		packfileChecksum := deltaState.IdToChecksum[subpart.Packfile]
		deltaChunkChecksum := deltaState.IdToChecksum[deltaBlobChecksumID]
		st.SetPackfileForBlob(Type, packfileChecksum, deltaChunkChecksum,
			subpart.Offset,
			subpart.Length,
		)
	}
}

func (st *State) Merge(stateID objects.Checksum, deltaState *State) {
	st.mergeLocationMaps(packfile.TYPE_CHUNK, deltaState)
	st.mergeLocationMaps(packfile.TYPE_OBJECT, deltaState)
	st.mergeLocationMaps(packfile.TYPE_FILE, deltaState)
	st.mergeLocationMaps(packfile.TYPE_FILE, deltaState)
	st.mergeLocationMaps(packfile.TYPE_DIRECTORY, deltaState)
	st.mergeLocationMaps(packfile.TYPE_DATA, deltaState)
	st.mergeLocationMaps(packfile.TYPE_SNAPSHOT, deltaState)
	st.mergeLocationMaps(packfile.TYPE_SIGNATURE, deltaState)
	st.mergeLocationMaps(packfile.TYPE_ERROR, deltaState)

	deltaState.muDeletedSnapshots.Lock()
	for originalSnapshotID, tm := range deltaState.DeletedSnapshots {
		originalChecksum := deltaState.IdToChecksum[originalSnapshotID]
		snapshotID := st.getOrCreateIdForChecksum(originalChecksum)
		st.DeletedSnapshots[snapshotID] = tm
	}
	deltaState.muDeletedSnapshots.Unlock()
}

func (st *State) GetSubpartForBlob(Type packfile.Type, blobChecksum objects.Checksum) (objects.Checksum, uint32, uint32, bool) {
	blobID := st.getOrCreateIdForChecksum(blobChecksum)

	var mapPtr *map[uint64]Location
	switch Type {
	case packfile.TYPE_SNAPSHOT:
		st.muSnapshots.Lock()
		defer st.muSnapshots.Unlock()
		mapPtr = &st.Snapshots
	case packfile.TYPE_CHUNK:
		st.muChunks.Lock()
		defer st.muChunks.Unlock()
		mapPtr = &st.Chunks
	case packfile.TYPE_OBJECT:
		st.muObjects.Lock()
		defer st.muObjects.Unlock()
		mapPtr = &st.Objects
	case packfile.TYPE_FILE:
		st.muFiles.Lock()
		defer st.muFiles.Unlock()
		mapPtr = &st.Files
	case packfile.TYPE_DIRECTORY:
		st.muDirectories.Lock()
		defer st.muDirectories.Unlock()
		mapPtr = &st.Directories
	case packfile.TYPE_DATA:
		st.muDatas.Lock()
		defer st.muDatas.Unlock()
		mapPtr = &st.Datas
	case packfile.TYPE_SIGNATURE:
		st.muSignatures.Lock()
		defer st.muSignatures.Unlock()
		mapPtr = &st.Signatures
	case packfile.TYPE_ERROR:
		st.muErrors.Lock()
		defer st.muErrors.Unlock()
		mapPtr = &st.Errors
	default:
		panic("invalid blob type")
	}

	if blob, exists := (*mapPtr)[blobID]; !exists {
		return objects.Checksum{}, 0, 0, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[blob.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, blob.Offset, blob.Length, true
	}
}

func (st *State) BlobExists(Type packfile.Type, blobChecksum objects.Checksum) bool {
	blobID := st.getOrCreateIdForChecksum(blobChecksum)

	var mapPtr *map[uint64]Location
	switch Type {
	case packfile.TYPE_SNAPSHOT:
		st.muSnapshots.Lock()
		defer st.muSnapshots.Unlock()
		mapPtr = &st.Snapshots
	case packfile.TYPE_CHUNK:
		st.muChunks.Lock()
		defer st.muChunks.Unlock()
		mapPtr = &st.Chunks
	case packfile.TYPE_OBJECT:
		st.muObjects.Lock()
		defer st.muObjects.Unlock()
		mapPtr = &st.Objects
	case packfile.TYPE_FILE:
		st.muFiles.Lock()
		defer st.muFiles.Unlock()
		mapPtr = &st.Files
	case packfile.TYPE_DIRECTORY:
		st.muDirectories.Lock()
		defer st.muDirectories.Unlock()
		mapPtr = &st.Directories
	case packfile.TYPE_DATA:
		st.muDatas.Lock()
		defer st.muDatas.Unlock()
		mapPtr = &st.Datas
	case packfile.TYPE_SIGNATURE:
		st.muSignatures.Lock()
		defer st.muSignatures.Unlock()
		mapPtr = &st.Signatures
	case packfile.TYPE_ERROR:
		st.muErrors.Lock()
		defer st.muErrors.Unlock()
		mapPtr = &st.Errors
	default:
		panic("invalid blob type")
	}

	if _, exists := (*mapPtr)[blobID]; !exists {
		return false
	} else {
		return true
	}
}

func (st *State) Dirty() bool {
	return atomic.LoadInt32(&st.dirty) != 0
}

func (st *State) ResetDirty() {
	atomic.StoreInt32(&st.dirty, 0)
}

func (st *State) SetPackfileForBlob(Type packfile.Type, packfileChecksum objects.Checksum, blobChecksum objects.Checksum, packfileOffset uint32, chunkLength uint32) {
	packfileID := st.getOrCreateIdForChecksum(packfileChecksum)
	blobID := st.getOrCreateIdForChecksum(blobChecksum)

	var mapPtr *map[uint64]Location
	switch Type {
	case packfile.TYPE_SNAPSHOT:
		st.muSnapshots.Lock()
		defer st.muSnapshots.Unlock()
		mapPtr = &st.Snapshots
	case packfile.TYPE_CHUNK:
		st.muChunks.Lock()
		defer st.muChunks.Unlock()
		mapPtr = &st.Chunks
	case packfile.TYPE_OBJECT:
		st.muObjects.Lock()
		defer st.muObjects.Unlock()
		mapPtr = &st.Objects
	case packfile.TYPE_FILE:
		st.muFiles.Lock()
		defer st.muFiles.Unlock()
		mapPtr = &st.Files
	case packfile.TYPE_DIRECTORY:
		st.muDirectories.Lock()
		defer st.muDirectories.Unlock()
		mapPtr = &st.Directories
	case packfile.TYPE_DATA:
		st.muDatas.Lock()
		defer st.muDatas.Unlock()
		mapPtr = &st.Datas
	case packfile.TYPE_SIGNATURE:
		st.muSignatures.Lock()
		defer st.muSignatures.Unlock()
		mapPtr = &st.Signatures
	case packfile.TYPE_ERROR:
		st.muErrors.Lock()
		defer st.muErrors.Unlock()
		mapPtr = &st.Errors
	default:
		panic("invalid blob type")
	}

	if _, exists := (*mapPtr)[blobID]; !exists {
		(*mapPtr)[blobID] = Location{
			Packfile: packfileID,
			Offset:   packfileOffset,
			Length:   chunkLength,
		}
		atomic.StoreInt32(&st.dirty, 1)
	}
}

func (st *State) DeleteSnapshot(snapshotChecksum objects.Checksum) error {
	snapshotID := st.getOrCreateIdForChecksum(snapshotChecksum)

	st.muSnapshots.Lock()
	defer st.muSnapshots.Unlock()
	_, exists := st.Snapshots[snapshotID]

	if !exists {
		return fmt.Errorf("snapshot not found")
	}

	delete(st.Snapshots, snapshotID)

	st.muDeletedSnapshots.Lock()
	st.DeletedSnapshots[snapshotID] = time.Now()
	st.muDeletedSnapshots.Unlock()

	atomic.StoreInt32(&st.dirty, 1)
	return nil
}

func (st *State) ListBlobs(Type packfile.Type) <-chan objects.Checksum {
	ch := make(chan objects.Checksum)
	go func() {
		var mapPtr *map[uint64]Location
		var mtx *sync.Mutex
		switch Type {
		case packfile.TYPE_CHUNK:
			mtx = &st.muChunks
			mapPtr = &st.Chunks
		case packfile.TYPE_OBJECT:
			mtx = &st.muObjects
			mapPtr = &st.Objects
		case packfile.TYPE_FILE:
			mtx = &st.muFiles
			mapPtr = &st.Files
		case packfile.TYPE_DIRECTORY:
			mtx = &st.muDirectories
			mapPtr = &st.Directories
		case packfile.TYPE_DATA:
			mtx = &st.muDatas
			mapPtr = &st.Datas
		case packfile.TYPE_SIGNATURE:
			mtx = &st.muSignatures
			mapPtr = &st.Signatures
		case packfile.TYPE_ERROR:
			mtx = &st.muErrors
			mapPtr = &st.Errors
		default:
			panic("invalid blob type")
		}

		blobsList := make([]objects.Checksum, 0)
		mtx.Lock()
		for k := range *mapPtr {
			blobsList = append(blobsList, st.IdToChecksum[k])
		}
		mtx.Unlock()

		for _, checksum := range blobsList {
			ch <- checksum
		}
		close(ch)
	}()
	return ch
}

func (st *State) ListSnapshots() <-chan objects.Checksum {
	ch := make(chan objects.Checksum)
	go func() {
		snapshotsList := make([]objects.Checksum, 0)
		st.muSnapshots.Lock()
		for k := range st.Snapshots {
			st.muDeletedSnapshots.Lock()
			_, deleted := st.DeletedSnapshots[k]
			st.muDeletedSnapshots.Unlock()
			if !deleted {
				snapshotsList = append(snapshotsList, st.IdToChecksum[k])
			}
		}
		st.muSnapshots.Unlock()

		for _, checksum := range snapshotsList {
			ch <- checksum
		}
		close(ch)
	}()
	return ch
}
