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
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/vmihailenco/msgpack/v5"
)

type Subpart struct {
	PackfileID uint32
	Offset     uint32
	Length     uint32
}

type State struct {
	muChecksums      sync.Mutex
	checksumID       uint32
	Checksums        map[[32]byte]uint32
	checksumsInverse map[uint32][32]byte

	muChunks sync.Mutex
	Chunks   map[uint32]Subpart

	muObjects sync.Mutex
	Objects   map[uint32]Subpart

	muContains sync.Mutex
	Contains   map[uint32]struct{}

	dirty int32
}

func New() *State {
	return &State{
		Checksums:        make(map[[32]byte]uint32),
		checksumsInverse: make(map[uint32][32]byte),
		Chunks:           make(map[uint32]Subpart),
		Objects:          make(map[uint32]Subpart),
		Contains:         make(map[uint32]struct{}),
	}
}

func NewFromBytes(serialized []byte) (*State, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("state.NewFromBytes", time.Since(t0))
		logger.Trace("state", "NewFromBytes(...): %s", time.Since(t0))
	}()

	var st State
	if err := msgpack.Unmarshal(serialized, &st); err != nil {
		return nil, err
	}

	st.checksumsInverse = make(map[uint32][32]byte)
	for checksum, checksumID := range st.Checksums {
		st.checksumsInverse[checksumID] = checksum
	}

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
	return serialized, nil
}

func (st *State) addChecksum(checksum [32]byte) uint32 {
	st.muChecksums.Lock()
	defer st.muChecksums.Unlock()

	if checksumID, exists := st.Checksums[checksum]; !exists {
		st.Checksums[checksum] = st.checksumID
		st.checksumsInverse[st.checksumID] = checksum
		checksumID = st.checksumID
		st.checksumID++
		atomic.StoreInt32(&st.dirty, 1)
		return checksumID
	} else {
		return checksumID
	}
}

func (st *State) LookupChecksum(checksumID uint32) [32]byte {
	st.muChecksums.Lock()
	defer st.muChecksums.Unlock()

	checksum, exists := st.checksumsInverse[checksumID]
	if !exists {
		panic("checksum not found")
	}
	return checksum
}

func (st *State) Merge(stateID [32]byte, deltaState *State) {

	deltaState.muChecksums.Lock()
	for deltaChecksum := range deltaState.Checksums {
		st.muChecksums.Lock()
		_, exists := st.Checksums[deltaChecksum]
		st.muChecksums.Unlock()
		if !exists {
			st.addChecksum(deltaChecksum)
		}
	}
	deltaState.muChecksums.Unlock()

	deltaState.muChunks.Lock()
	for deltaChunkChecksumID, subpart := range deltaState.Chunks {
		st.SetPackfileForChunk(
			deltaState.LookupChecksum(subpart.PackfileID),
			deltaState.LookupChecksum(deltaChunkChecksumID),
			subpart.Offset,
			subpart.Length,
		)
	}
	deltaState.muChunks.Unlock()

	deltaState.muObjects.Lock()
	for deltaObjectChecksumID, subpart := range deltaState.Objects {
		st.SetPackfileForObject(
			deltaState.LookupChecksum(subpart.PackfileID),
			deltaState.LookupChecksum(deltaObjectChecksumID),
			subpart.Offset,
			subpart.Length,
		)
	}
	deltaState.muObjects.Unlock()

	st.muContains.Lock()
	st.Contains[st.addChecksum(stateID)] = struct{}{}
	st.muContains.Unlock()
}

func (st *State) SetPackfileForChunk(packfileChecksum [32]byte, chunkChecksum [32]byte, packfileOffset uint32, chunkLength uint32) {
	st.muChunks.Lock()
	defer st.muChunks.Unlock()

	chunkID := st.addChecksum(chunkChecksum)
	if _, exists := st.Chunks[chunkID]; !exists {
		packfileID := st.addChecksum(packfileChecksum)
		st.Chunks[chunkID] = Subpart{
			PackfileID: packfileID,
			Offset:     packfileOffset,
			Length:     chunkLength,
		}
		atomic.StoreInt32(&st.dirty, 1)
	}
}

func (st *State) GetPackfileForChunk(chunkChecksum [32]byte) ([32]byte, bool) {
	st.muChunks.Lock()
	defer st.muChunks.Unlock()

	chunkID := st.addChecksum(chunkChecksum)
	if subpart, exists := st.Chunks[chunkID]; !exists {
		return [32]byte{}, false
	} else {
		st.muChecksums.Lock()
		packfileChecksum, exists := st.checksumsInverse[subpart.PackfileID]
		st.muChecksums.Unlock()
		if !exists {
			panic("packfile checksum not found")
		}
		return packfileChecksum, true
	}
}

func (st *State) GetSubpartForChunk(chunkChecksum [32]byte) ([32]byte, uint32, uint32, bool) {
	st.muChunks.Lock()
	defer st.muChunks.Unlock()

	chunkID := st.addChecksum(chunkChecksum)
	if subpart, exists := st.Chunks[chunkID]; !exists {
		return [32]byte{}, 0, 0, false
	} else {
		st.muChecksums.Lock()
		packfileChecksum, exists := st.checksumsInverse[subpart.PackfileID]
		st.muChecksums.Unlock()
		if !exists {
			panic("packfile checksum not found")
		}
		return packfileChecksum, subpart.Offset, subpart.Length, true
	}
}

func (st *State) ChunkExists(chunkChecksum [32]byte) bool {
	st.muChunks.Lock()
	defer st.muChunks.Unlock()

	chunkID := st.addChecksum(chunkChecksum)
	if _, exists := st.Chunks[chunkID]; !exists {
		return false
	} else {
		return true
	}
}

func (st *State) SetPackfileForObject(packfileChecksum [32]byte, objectChecksum [32]byte, packfileOffset uint32, chunkLength uint32) {
	st.muObjects.Lock()
	defer st.muObjects.Unlock()

	objectID := st.addChecksum(objectChecksum)
	if _, exists := st.Objects[objectID]; !exists {
		packfileID := st.addChecksum(packfileChecksum)
		st.Objects[objectID] = Subpart{
			PackfileID: packfileID,
			Offset:     packfileOffset,
			Length:     chunkLength,
		}
		atomic.StoreInt32(&st.dirty, 1)
	}
}

func (st *State) GetPackfileForObject(objectChecksum [32]byte) ([32]byte, bool) {
	st.muObjects.Lock()
	defer st.muObjects.Unlock()

	objectID := st.addChecksum(objectChecksum)
	if subpart, exists := st.Objects[objectID]; !exists {
		return [32]byte{}, false
	} else {
		st.muChecksums.Lock()
		packfileChecksum, exists := st.checksumsInverse[subpart.PackfileID]
		st.muChecksums.Unlock()
		if !exists {
			panic("packfile checksum not found")
		}
		return packfileChecksum, true
	}
}

func (st *State) GetSubpartForObject(objectChecksum [32]byte) ([32]byte, uint32, uint32, bool) {
	st.muObjects.Lock()
	defer st.muObjects.Unlock()

	objectID := st.addChecksum(objectChecksum)
	if subpart, exists := st.Objects[objectID]; !exists {
		return [32]byte{}, 0, 0, false
	} else {
		st.muChecksums.Lock()
		packfileChecksum, exists := st.checksumsInverse[subpart.PackfileID]
		st.muChecksums.Unlock()
		if !exists {
			panic("packfile checksum not found")
		}
		return packfileChecksum, subpart.Offset, subpart.Length, true
	}
}

func (st *State) ObjectExists(objectChecksum [32]byte) bool {
	st.muObjects.Lock()
	defer st.muObjects.Unlock()

	objectID := st.addChecksum(objectChecksum)
	if _, exists := st.Objects[objectID]; !exists {
		return false
	} else {
		return true
	}
}

func (st *State) ListContains() [][32]byte {
	st.muContains.Lock()
	defer st.muContains.Unlock()
	ret := make([][32]byte, 0)
	for checksumID := range st.Contains {
		ret = append(ret, st.LookupChecksum(checksumID))
	}
	return ret
}

func (st *State) IsDirty() bool {
	return atomic.LoadInt32(&st.dirty) != 0
}

func (st *State) ResetDirty() {
	atomic.StoreInt32(&st.dirty, 0)
}
