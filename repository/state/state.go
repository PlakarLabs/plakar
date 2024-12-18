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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
)

const VERSION = 100

type Metadata struct {
	Version   uint32
	Timestamp time.Time
	Aggregate bool
	Extends   []objects.Checksum
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

	muChildren sync.Mutex
	Children   map[uint64]Location

	muDatas sync.Mutex
	Datas   map[uint64]Location

	muSnapshots sync.Mutex
	Snapshots   map[uint64]Location

	muSignatures sync.Mutex
	Signatures   map[uint64]Location

	muErrors sync.Mutex
	Errors   map[uint64]Location

	muList sync.Mutex
	List   map[uint64]Location

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
		Children:         make(map[uint64]Location),
		Datas:            make(map[uint64]Location),
		Snapshots:        make(map[uint64]Location),
		Signatures:       make(map[uint64]Location),
		Errors:           make(map[uint64]Location),
		DeletedSnapshots: make(map[uint64]time.Time),
		Metadata: Metadata{
			Version:   VERSION,
			Timestamp: time.Now(),
			Aggregate: false,
			Extends:   []objects.Checksum{},
		},
	}
}

func (st *State) Derive() *State {
	nst := New()
	nst.Metadata.Extends = st.Metadata.Extends
	return nst
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

func (st *State) SerializeStream(w io.Writer) error {
	// Helper function to write a uint64
	writeUint64 := func(value uint64) error {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, value)
		_, err := w.Write(buf)
		return err
	}

	// Helper function to write a uint32
	writeUint32 := func(value uint32) error {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, value)
		_, err := w.Write(buf)
		return err
	}

	writeLocation := func(loc Location) error {
		if err := writeUint64(loc.Packfile); err != nil {
			return err
		}
		if err := writeUint32(loc.Offset); err != nil {
			return err
		}
		return writeUint32(loc.Length)
	}

	// Serialize Metadata
	if err := writeUint32(st.Metadata.Version); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}
	timestamp := st.Metadata.Timestamp.UnixNano()
	if err := writeUint64(uint64(timestamp)); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}
	if st.Metadata.Aggregate {
		if _, err := w.Write([]byte{1}); err != nil {
			return fmt.Errorf("failed to write aggregate flag: %w", err)
		}
	} else {
		if _, err := w.Write([]byte{0}); err != nil {
			return fmt.Errorf("failed to write aggregate flag: %w", err)
		}
	}
	if err := writeUint64(uint64(len(st.Metadata.Extends))); err != nil {
		return fmt.Errorf("failed to write extends length: %w", err)
	}
	for _, checksum := range st.Metadata.Extends {
		if _, err := w.Write(checksum[:]); err != nil {
			return fmt.Errorf("failed to write checksum: %w", err)
		}
	}

	if err := serializeMapping(w, st.DeletedSnapshots, func(key uint64) error {
		return writeUint64(key)
	}, func(value time.Time) error {
		return writeUint64(uint64(value.UnixNano()))
	}); err != nil {
		return fmt.Errorf("failed to serialize DeletedSnapshots: %w", err)
	}

	// Serialize each mapping
	if err := serializeMapping(w, st.IdToChecksum, writeUint64, func(v objects.Checksum) error { _, err := w.Write(v[:]); return err }); err != nil {
		return fmt.Errorf("failed to serialize IdToChecksum: %w", err)
	}

	mappings := []struct {
		name string
		data map[uint64]Location
	}{
		{"Chunks", st.Chunks},
		{"Objects", st.Objects},
		{"Files", st.Files},
		{"Directories", st.Directories},
		{"Children", st.Children},
		{"Datas", st.Datas},
		{"Snapshots", st.Snapshots},
		{"Signatures", st.Signatures},
		{"Errors", st.Errors},
	}

	for _, m := range mappings {
		if err := serializeMapping(w, m.data, writeUint64, func(v Location) error {
			return writeLocation(v)
		}); err != nil {
			return fmt.Errorf("failed to serialize %s: %w", m.name, err)
		}
	}

	return nil
}

func serializeMapping[K comparable, V any](w io.Writer, mapping map[K]V, writeKey func(K) error, writeValue func(V) error) error {
	// Write the size of the mapping
	if err := binary.Write(w, binary.LittleEndian, uint64(len(mapping))); err != nil {
		return fmt.Errorf("failed to write map size: %w", err)
	}
	// Write each key-value pair
	for key, value := range mapping {
		if err := writeKey(key); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}
		if err := writeValue(value); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}
	}
	return nil
}

func DeserializeStream(r io.Reader) (*State, error) {
	readUint64 := func() (uint64, error) {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(buf), nil
	}

	readUint32 := func() (uint32, error) {
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(buf), nil
	}

	readLocation := func() (Location, error) {
		packfile, err := readUint64()
		if err != nil {
			return Location{}, err
		}
		offset, err := readUint32()
		if err != nil {
			return Location{}, err
		}
		length, err := readUint32()
		if err != nil {
			return Location{}, err
		}
		return Location{Packfile: packfile, Offset: offset, Length: length}, nil
	}

	st := &State{}

	// Deserialize Metadata
	version, err := readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	st.Metadata.Version = version

	timestamp, err := readUint64()
	if err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}
	st.Metadata.Timestamp = time.Unix(0, int64(timestamp))

	aggregate := make([]byte, 1)
	if _, err := io.ReadFull(r, aggregate); err != nil {
		return nil, fmt.Errorf("failed to read aggregate flag: %w", err)
	}
	st.Metadata.Aggregate = aggregate[0] == 1

	extendsLen, err := readUint64()
	if err != nil {
		return nil, fmt.Errorf("failed to read extends length: %w", err)
	}
	st.Metadata.Extends = make([]objects.Checksum, extendsLen)
	for i := uint64(0); i < extendsLen; i++ {
		var checksum objects.Checksum
		if _, err := io.ReadFull(r, checksum[:]); err != nil {
			return nil, fmt.Errorf("failed to read checksum: %w", err)
		}
		st.Metadata.Extends[i] = checksum
	}

	// Deserialize DeletedSnapshots
	st.DeletedSnapshots = make(map[uint64]time.Time)
	if err := deserializeMapping(r, st.DeletedSnapshots, readUint64, func() (time.Time, error) {
		timestamp, err := readUint64()
		if err != nil {
			return time.Time{}, err
		}
		return time.Unix(0, int64(timestamp)), nil
	}); err != nil {
		return nil, fmt.Errorf("failed to deserialize DeletedSnapshots: %w", err)
	}

	// Deserialize IdToChecksum
	st.IdToChecksum = make(map[uint64]objects.Checksum)
	if err := deserializeMapping(r, st.IdToChecksum, readUint64, func() (objects.Checksum, error) {
		var checksum objects.Checksum
		if _, err := io.ReadFull(r, checksum[:]); err != nil {
			return objects.Checksum{}, err
		}
		return checksum, nil
	}); err != nil {
		return nil, fmt.Errorf("failed to deserialize IdToChecksum: %w", err)
	}

	// Deserialize each mapping
	mappings := []struct {
		name string
		data *map[uint64]Location
	}{
		{"Chunks", &st.Chunks},
		{"Objects", &st.Objects},
		{"Files", &st.Files},
		{"Directories", &st.Directories},
		{"Children", &st.Children},
		{"Datas", &st.Datas},
		{"Snapshots", &st.Snapshots},
		{"Signatures", &st.Signatures},
		{"Errors", &st.Errors},
	}

	for _, m := range mappings {
		*m.data = make(map[uint64]Location)
		if err := deserializeMapping(r, *m.data, readUint64, readLocation); err != nil {
			return nil, fmt.Errorf("failed to deserialize %s: %w", m.name, err)
		}
	}

	return st, nil
}

func deserializeMapping[K comparable, V any](r io.Reader, mapping map[K]V, readKey func() (K, error), readValue func() (V, error)) error {
	readUint64 := func(r io.Reader) (uint64, error) {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(buf), nil
	}

	// Read the size of the mapping
	length, err := readUint64(r)
	if err != nil {
		return fmt.Errorf("failed to read map size: %w", err)
	}

	// Read each key-value pair
	for i := uint64(0); i < length; i++ {
		key, err := readKey()
		if err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}
		value, err := readValue()
		if err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}
		mapping[key] = value
	}
	return nil
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
	case packfile.TYPE_CHILD:
		deltaState.muChildren.Lock()
		defer deltaState.muChildren.Unlock()
		mapPtr = &deltaState.Children
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
	st.mergeLocationMaps(packfile.TYPE_DIRECTORY, deltaState)
	st.mergeLocationMaps(packfile.TYPE_CHILD, deltaState)
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
	case packfile.TYPE_CHILD:
		st.muChildren.Lock()
		defer st.muChildren.Unlock()
		mapPtr = &st.Children
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
	case packfile.TYPE_CHILD:
		st.muChildren.Lock()
		defer st.muChildren.Unlock()
		mapPtr = &st.Children
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
	case packfile.TYPE_CHILD:
		st.muChildren.Lock()
		defer st.muChildren.Unlock()
		mapPtr = &st.Children
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
		case packfile.TYPE_CHILD:
			mtx = &st.muChildren
			mapPtr = &st.Children
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
