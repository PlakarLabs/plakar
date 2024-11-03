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

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION = 100

type Metadata struct {
	Version      uint32
	CreationTime time.Time
	Aggregate    bool
	Extends      [][32]byte
}

type Location struct {
	Packfile uint64
	Offset   uint32
	Length   uint32
}

type State struct {
	muChecksum   sync.Mutex
	checksumToId map[[32]byte]uint64
	IdToChecksum map[uint64][32]byte

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

	muDeletedSnapshots sync.Mutex
	DeletedSnapshots   map[uint64]time.Time

	Metadata Metadata

	dirty int32
}

func New() *State {
	return &State{
		IdToChecksum:     make(map[uint64][32]byte),
		checksumToId:     make(map[[32]byte]uint64),
		Chunks:           make(map[uint64]Location),
		Objects:          make(map[uint64]Location),
		Files:            make(map[uint64]Location),
		Directories:      make(map[uint64]Location),
		Datas:            make(map[uint64]Location),
		Snapshots:        make(map[uint64]Location),
		DeletedSnapshots: make(map[uint64]time.Time),
		Metadata: Metadata{
			Version:      VERSION,
			CreationTime: time.Now(),
			Aggregate:    false,
			Extends:      [][32]byte{},
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

	st.checksumToId = make(map[[32]byte]uint64)

	// Rebuild checksumToID by reversing the IDToChecksum map
	for id, checksum := range st.IdToChecksum {
		st.checksumToId[checksum] = id
	}
}

func (st *State) getOrCreateIdForChecksum(checksum [32]byte) uint64 {
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
		profiler.RecordEvent("state.NewFromBytes", time.Since(t0))
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
		profiler.RecordEvent("state.Serialize", time.Since(t0))
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

func (st *State) Extends(stateID [32]byte) {
	st.Metadata.Extends = append(st.Metadata.Extends, stateID)
}

func (st *State) Merge(stateID [32]byte, deltaState *State) {
	deltaState.muChunks.Lock()
	for deltaChunkChecksumID, subpart := range deltaState.Chunks {
		packfileChecksum := deltaState.IdToChecksum[subpart.Packfile]
		deltaChunkChecksum := deltaState.IdToChecksum[deltaChunkChecksumID]
		st.SetPackfileForChunk(packfileChecksum, deltaChunkChecksum,
			subpart.Offset,
			subpart.Length,
		)
	}
	deltaState.muChunks.Unlock()

	deltaState.muObjects.Lock()
	for deltaObjectChecksumID, subpart := range deltaState.Objects {
		packfileChecksum := deltaState.IdToChecksum[subpart.Packfile]
		deltaObjectChecksum := deltaState.IdToChecksum[deltaObjectChecksumID]
		st.SetPackfileForObject(packfileChecksum, deltaObjectChecksum,
			subpart.Offset,
			subpart.Length,
		)
	}
	deltaState.muObjects.Unlock()

	deltaState.muFiles.Lock()
	for deltaFileChecksumID, subpart := range deltaState.Files {
		packfileChecksum := deltaState.IdToChecksum[subpart.Packfile]
		deltaFileChecksum := deltaState.IdToChecksum[deltaFileChecksumID]
		st.SetPackfileForFile(packfileChecksum, deltaFileChecksum,
			subpart.Offset,
			subpart.Length,
		)
	}
	deltaState.muFiles.Unlock()

	deltaState.muDirectories.Lock()
	for deltaDirectoryChecksumID, subpart := range deltaState.Directories {
		packfileChecksum := deltaState.IdToChecksum[subpart.Packfile]
		deltaDirectoryChecksum := deltaState.IdToChecksum[deltaDirectoryChecksumID]
		st.SetPackfileForDirectory(packfileChecksum, deltaDirectoryChecksum,
			subpart.Offset,
			subpart.Length,
		)
	}
	deltaState.muDirectories.Unlock()

	deltaState.muDatas.Lock()
	for deltaBlobChecksumID, subpart := range deltaState.Datas {
		packfileChecksum := deltaState.IdToChecksum[subpart.Packfile]
		deltaBlobChecksum := deltaState.IdToChecksum[deltaBlobChecksumID]
		st.SetPackfileForData(packfileChecksum, deltaBlobChecksum,
			subpart.Offset,
			subpart.Length,
		)
	}
	deltaState.muDatas.Unlock()

	deltaState.muSnapshots.Lock()
	for deltaSnapshotID, subpart := range deltaState.Snapshots {
		packfileChecksum := deltaState.IdToChecksum[subpart.Packfile]
		deltaSnapshotID := deltaState.IdToChecksum[deltaSnapshotID]
		st.SetPackfileForSnapshot(packfileChecksum, deltaSnapshotID,
			subpart.Offset,
			subpart.Length,
		)
	}
	deltaState.muSnapshots.Unlock()

	deltaState.muDeletedSnapshots.Lock()
	for originalSnapshotID, tm := range deltaState.DeletedSnapshots {
		originalChecksum := deltaState.IdToChecksum[originalSnapshotID]
		snapshotID := st.getOrCreateIdForChecksum(originalChecksum)
		st.DeletedSnapshots[snapshotID] = tm
	}
	deltaState.muDeletedSnapshots.Unlock()
}

func (st *State) GetPackfileForChunk(chunkChecksum [32]byte) ([32]byte, bool) {
	chunkID := st.getOrCreateIdForChecksum(chunkChecksum)

	st.muChunks.Lock()
	defer st.muChunks.Unlock()

	if subpart, exists := st.Chunks[chunkID]; !exists {
		return [32]byte{}, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, true
	}
}

func (st *State) GetPackfileForObject(objectChecksum [32]byte) ([32]byte, bool) {
	objectID := st.getOrCreateIdForChecksum(objectChecksum)

	st.muObjects.Lock()
	defer st.muObjects.Unlock()

	if subpart, exists := st.Objects[objectID]; !exists {
		return [32]byte{}, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, true
	}
}

func (st *State) GetPackfileForFile(fileChecksum [32]byte) ([32]byte, bool) {
	fileID := st.getOrCreateIdForChecksum(fileChecksum)

	st.muFiles.Lock()
	defer st.muFiles.Unlock()

	if subpart, exists := st.Files[fileID]; !exists {
		return [32]byte{}, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, true
	}
}

func (st *State) GetPackfileForDirectory(directoryChecksum [32]byte) ([32]byte, bool) {
	directoryID := st.getOrCreateIdForChecksum(directoryChecksum)

	st.muDirectories.Lock()
	defer st.muDirectories.Unlock()

	if subpart, exists := st.Directories[directoryID]; !exists {
		return [32]byte{}, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, true
	}
}

func (st *State) GetPackfileForData(blobChecksum [32]byte) ([32]byte, bool) {
	blobID := st.getOrCreateIdForChecksum(blobChecksum)

	st.muDatas.Lock()
	defer st.muDatas.Unlock()

	if subpart, exists := st.Datas[blobID]; !exists {
		return [32]byte{}, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, true
	}
}

func (st *State) GetSubpartForChunk(chunkChecksum [32]byte) ([32]byte, uint32, uint32, bool) {
	chunkID := st.getOrCreateIdForChecksum(chunkChecksum)

	st.muChunks.Lock()
	defer st.muChunks.Unlock()

	if subpart, exists := st.Chunks[chunkID]; !exists {
		return [32]byte{}, 0, 0, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, subpart.Offset, subpart.Length, true
	}
}

func (st *State) GetSubpartForObject(objectChecksum [32]byte) ([32]byte, uint32, uint32, bool) {
	objectID := st.getOrCreateIdForChecksum(objectChecksum)

	st.muObjects.Lock()
	defer st.muObjects.Unlock()

	if subpart, exists := st.Objects[objectID]; !exists {
		return [32]byte{}, 0, 0, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, subpart.Offset, subpart.Length, true
	}
}

func (st *State) GetSubpartForFile(checksum [32]byte) ([32]byte, uint32, uint32, bool) {
	fileID := st.getOrCreateIdForChecksum(checksum)

	st.muFiles.Lock()
	defer st.muFiles.Unlock()

	if subpart, exists := st.Files[fileID]; !exists {
		return [32]byte{}, 0, 0, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, subpart.Offset, subpart.Length, true
	}
}

func (st *State) GetSubpartForDirectory(checksum [32]byte) ([32]byte, uint32, uint32, bool) {
	directoryID := st.getOrCreateIdForChecksum(checksum)

	st.muDirectories.Lock()
	defer st.muDirectories.Unlock()

	if subpart, exists := st.Directories[directoryID]; !exists {
		return [32]byte{}, 0, 0, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, subpart.Offset, subpart.Length, true
	}
}

func (st *State) GetSubpartForData(checksum [32]byte) ([32]byte, uint32, uint32, bool) {
	blobID := st.getOrCreateIdForChecksum(checksum)

	st.muDatas.Lock()
	defer st.muDatas.Unlock()

	if subpart, exists := st.Datas[blobID]; !exists {
		return [32]byte{}, 0, 0, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, subpart.Offset, subpart.Length, true
	}
}

func (st *State) GetSubpartForSnapshot(checksum [32]byte) ([32]byte, uint32, uint32, bool) {
	blobID := st.getOrCreateIdForChecksum(checksum)

	st.muSnapshots.Lock()
	defer st.muSnapshots.Unlock()

	if subpart, exists := st.Snapshots[blobID]; !exists {
		return [32]byte{}, 0, 0, false
	} else {
		st.muChecksum.Lock()
		packfileChecksum := st.IdToChecksum[subpart.Packfile]
		st.muChecksum.Unlock()
		return packfileChecksum, subpart.Offset, subpart.Length, true
	}
}

func (st *State) ChunkExists(chunkChecksum [32]byte) bool {
	chunkID := st.getOrCreateIdForChecksum(chunkChecksum)

	st.muChunks.Lock()
	defer st.muChunks.Unlock()

	if _, exists := st.Chunks[chunkID]; !exists {
		return false
	} else {
		return true
	}
}

func (st *State) ObjectExists(objectChecksum [32]byte) bool {
	objectID := st.getOrCreateIdForChecksum(objectChecksum)

	st.muObjects.Lock()
	defer st.muObjects.Unlock()

	if _, exists := st.Objects[objectID]; !exists {
		return false
	} else {
		return true
	}
}

func (st *State) FileExists(checksum [32]byte) bool {
	checksumID := st.getOrCreateIdForChecksum(checksum)

	st.muFiles.Lock()
	defer st.muFiles.Unlock()

	if _, exists := st.Files[checksumID]; !exists {
		return false
	} else {
		return true
	}
}

func (st *State) DirectoryExists(checksum [32]byte) bool {
	checksumID := st.getOrCreateIdForChecksum(checksum)

	st.muDirectories.Lock()
	defer st.muDirectories.Unlock()

	if _, exists := st.Directories[checksumID]; !exists {
		return false
	} else {
		return true
	}
}

func (st *State) DataExists(checksum [32]byte) bool {
	checksumID := st.getOrCreateIdForChecksum(checksum)

	st.muDatas.Lock()
	defer st.muDatas.Unlock()

	if _, exists := st.Datas[checksumID]; !exists {
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

func (st *State) SetPackfileForChunk(packfileChecksum [32]byte, chunkChecksum [32]byte, packfileOffset uint32, chunkLength uint32) {
	packfileID := st.getOrCreateIdForChecksum(packfileChecksum)
	chunkID := st.getOrCreateIdForChecksum(chunkChecksum)

	st.muChunks.Lock()
	if _, exists := st.Chunks[chunkID]; !exists {
		st.Chunks[chunkID] = Location{
			Packfile: packfileID,
			Offset:   packfileOffset,
			Length:   chunkLength,
		}
		st.muChunks.Unlock()
		atomic.StoreInt32(&st.dirty, 1)
	} else {
		st.muChunks.Unlock()
	}
}

func (st *State) SetPackfileForObject(packfileChecksum [32]byte, objectChecksum [32]byte, packfileOffset uint32, chunkLength uint32) {
	packfileID := st.getOrCreateIdForChecksum(packfileChecksum)
	objectID := st.getOrCreateIdForChecksum(objectChecksum)

	st.muObjects.Lock()
	if _, exists := st.Objects[objectID]; !exists {
		st.Objects[objectID] = Location{
			Packfile: packfileID,
			Offset:   packfileOffset,
			Length:   chunkLength,
		}
		st.muObjects.Unlock()
		atomic.StoreInt32(&st.dirty, 1)
	} else {
		st.muObjects.Unlock()
	}
}

func (st *State) SetPackfileForFile(packfileChecksum [32]byte, fileChecksum [32]byte, packfileOffset uint32, chunkLength uint32) {
	packfileID := st.getOrCreateIdForChecksum(packfileChecksum)
	fileID := st.getOrCreateIdForChecksum(fileChecksum)

	st.muFiles.Lock()
	if _, exists := st.Files[fileID]; !exists {
		st.Files[fileID] = Location{
			Packfile: packfileID,
			Offset:   packfileOffset,
			Length:   chunkLength,
		}
		st.muFiles.Unlock()
		atomic.StoreInt32(&st.dirty, 1)
	} else {
		st.muFiles.Unlock()
	}
}

func (st *State) SetPackfileForDirectory(packfileChecksum [32]byte, directoryChecksum [32]byte, packfileOffset uint32, chunkLength uint32) {
	packfileID := st.getOrCreateIdForChecksum(packfileChecksum)
	directoryID := st.getOrCreateIdForChecksum(directoryChecksum)

	st.muDirectories.Lock()
	if _, exists := st.Directories[directoryID]; !exists {
		st.Directories[directoryID] = Location{
			Packfile: packfileID,
			Offset:   packfileOffset,
			Length:   chunkLength,
		}
		st.muDirectories.Unlock()
		atomic.StoreInt32(&st.dirty, 1)
	} else {
		st.muDirectories.Unlock()
	}
}

func (st *State) SetPackfileForData(packfileChecksum [32]byte, blobChecksum [32]byte, packfileOffset uint32, chunkLength uint32) {
	packfileID := st.getOrCreateIdForChecksum(packfileChecksum)
	blobID := st.getOrCreateIdForChecksum(blobChecksum)

	st.muDatas.Lock()
	if _, exists := st.Datas[blobID]; !exists {
		st.Datas[blobID] = Location{
			Packfile: packfileID,
			Offset:   packfileOffset,
			Length:   chunkLength,
		}
		st.muDatas.Unlock()
		atomic.StoreInt32(&st.dirty, 1)
	} else {
		st.muDatas.Unlock()
	}
}

func (st *State) SetPackfileForSnapshot(packfileChecksum [32]byte, blobChecksum [32]byte, packfileOffset uint32, chunkLength uint32) {
	packfileID := st.getOrCreateIdForChecksum(packfileChecksum)
	blobID := st.getOrCreateIdForChecksum(blobChecksum)

	st.muSnapshots.Lock()
	if _, exists := st.Snapshots[blobID]; !exists {
		st.Snapshots[blobID] = Location{
			Packfile: packfileID,
			Offset:   packfileOffset,
			Length:   chunkLength,
		}
		st.muSnapshots.Unlock()
		atomic.StoreInt32(&st.dirty, 1)
	} else {
		st.muSnapshots.Unlock()
	}
}

func (st *State) DeleteSnapshot(snapshotChecksum [32]byte) error {
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

func (st *State) ListSnapshots() <-chan [32]byte {
	ch := make(chan [32]byte)
	go func() {
		snapshotsList := make([][32]byte, 0)
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

func (st *State) ListChunks() <-chan [32]byte {
	ch := make(chan [32]byte)
	go func() {
		chunksList := make([][32]byte, 0)
		st.muChunks.Lock()
		for k := range st.Chunks {
			chunksList = append(chunksList, st.IdToChecksum[k])
		}
		st.muChunks.Unlock()

		for _, checksum := range chunksList {
			ch <- checksum
		}
		close(ch)
	}()
	return ch
}

func (st *State) ListObjects() <-chan [32]byte {
	ch := make(chan [32]byte)
	go func() {
		objectsList := make([][32]byte, 0)
		st.muObjects.Lock()
		for k := range st.Objects {
			objectsList = append(objectsList, st.IdToChecksum[k])
		}
		st.muObjects.Unlock()

		for _, checksum := range objectsList {
			ch <- checksum
		}
		close(ch)
	}()
	return ch
}
