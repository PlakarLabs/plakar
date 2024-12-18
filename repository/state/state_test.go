package state

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
)

func TestNew(t *testing.T) {
	st := New()
	if len(st.Chunks) != 0 {
		t.Errorf("Expected Chunks to be empty, got %d", len(st.Chunks))
	}
	if len(st.Objects) != 0 {
		t.Errorf("Expected Objects to be empty, got %d", len(st.Objects))
	}
	if st.dirty != 0 {
		t.Errorf("Expected dirty to be 0, got %d", st.dirty)
	}
}

func TestMerge(t *testing.T) {
	st1 := New()
	st2 := New()

	checksumA := [32]byte{10, 20, 30}
	checksumB := [32]byte{40, 50, 60}
	stID := [32]byte{70, 80, 90}

	st1.SetPackfileForBlob(packfile.TYPE_CHUNK, checksumA, checksumB, 100, 200)
	st1.SetPackfileForBlob(packfile.TYPE_OBJECT, checksumA, checksumB, 300, 400)

	newChecksum := [32]byte{11, 22, 33}
	st2.SetPackfileForBlob(packfile.TYPE_CHUNK, checksumA, newChecksum, 500, 600)
	st2.SetPackfileForBlob(packfile.TYPE_OBJECT, checksumA, newChecksum, 700, 800)

	st1.Merge(stID, st2)

	// Verify Chunks
	expectedChunks := 2
	if len(st1.Chunks) != expectedChunks {
		t.Errorf("Expected %d Chunks, got %d", expectedChunks, len(st1.Chunks))
	}

	// Verify Objects
	expectedObjects := 2
	if len(st1.Objects) != expectedObjects {
		t.Errorf("Expected %d Objects, got %d", expectedObjects, len(st1.Objects))
	}

}

func TestIsDirtyAndResetDirty(t *testing.T) {
	st := New()

	if st.Dirty() {
		t.Errorf("Expected IsDirty to be false initially")
	}

	checksum := [32]byte{200, 201, 202}
	st.SetPackfileForBlob(packfile.TYPE_CHUNK, checksum, checksum, 300, 400)

	if !st.Dirty() {
		t.Errorf("Expected IsDirty to be true after adding a checksum")
	}

	st.ResetDirty()
	if st.Dirty() {
		t.Errorf("Expected IsDirty to be false after ResetDirty")
	}
}

func TestGetSubpartForChunk(t *testing.T) {
	st := New()

	packfileChecksum := [32]byte{1, 2, 3}
	chunkChecksum := [32]byte{4, 5, 6}
	offset := uint32(700)
	length := uint32(800)

	st.SetPackfileForBlob(packfile.TYPE_CHUNK, packfileChecksum, chunkChecksum, offset, length)

	pf, off, len_, exists := st.GetSubpartForBlob(packfile.TYPE_CHUNK, chunkChecksum)
	if !exists {
		t.Fatalf("Expected subpart for chunk %v to exist", chunkChecksum)
	}
	if pf != packfileChecksum {
		t.Errorf("Expected packfile checksum %v, got %v", packfileChecksum, pf)
	}
	if off != offset {
		t.Errorf("Expected offset %d, got %d", offset, off)
	}
	if len_ != length {
		t.Errorf("Expected length %d, got %d", length, len_)
	}

	// Test non-existing chunk
	nonExisting := [32]byte{7, 8, 9}
	_, _, _, exists = st.GetSubpartForBlob(packfile.TYPE_CHUNK, nonExisting)
	if exists {
		t.Errorf("Expected GetSubpartForChunk to return false for %v", nonExisting)
	}
}

func TestGetSubpartForObject(t *testing.T) {
	st := New()

	packfileChecksum := [32]byte{10, 11, 12}
	objectChecksum := [32]byte{13, 14, 15}
	offset := uint32(900)
	length := uint32(1000)

	st.SetPackfileForBlob(packfile.TYPE_OBJECT, packfileChecksum, objectChecksum, offset, length)

	pf, off, len_, exists := st.GetSubpartForBlob(packfile.TYPE_OBJECT, objectChecksum)
	if !exists {
		t.Fatalf("Expected subpart for object %v to exist", objectChecksum)
	}
	if pf != packfileChecksum {
		t.Errorf("Expected packfile checksum %v, got %v", packfileChecksum, pf)
	}
	if off != offset {
		t.Errorf("Expected offset %d, got %d", offset, off)
	}
	if len_ != length {
		t.Errorf("Expected length %d, got %d", length, len_)
	}

	// Test non-existing object
	nonExisting := [32]byte{16, 17, 18}
	_, _, _, exists = st.GetSubpartForBlob(packfile.TYPE_OBJECT, nonExisting)
	if exists {
		t.Errorf("Expected GetSubpartForObject to return false for %v", nonExisting)
	}
}

func TestSerializeDeserialize(t *testing.T) {
	// Create a test State object
	originalState := &State{
		Metadata: Metadata{
			Version:   1,
			Timestamp: time.Now(),
			Aggregate: true,
			Extends:   []objects.Checksum{{0x01}, {0x02}, {0x03}},
		},
		DeletedSnapshots: map[uint64]time.Time{
			123: time.Unix(1697045400, 0), // Example timestamp
			456: time.Unix(1697046000, 0),
		},
		IdToChecksum: map[uint64]objects.Checksum{
			1: {0x10},
			2: {0x20},
		},
		Chunks: map[uint64]Location{
			1: {Packfile: 100, Offset: 10, Length: 500},
			2: {Packfile: 200, Offset: 20, Length: 600},
		},
	}

	// Serialize the State object
	var buffer bytes.Buffer
	if err := originalState.SerializeStream(&buffer); err != nil {
		t.Fatalf("Failed to serialize state: %v", err)
	}

	// Deserialize the buffer back into a State object
	deserializedState, err := DeserializeStream(&buffer)
	if err != nil {
		t.Fatalf("Failed to deserialize state: %v", err)
	}

	// Verify that the original and deserialized states match
	if !compareStates(originalState, deserializedState) {
		t.Fatalf("Original and deserialized states do not match.\nOriginal: %+v\nDeserialized: %+v",
			originalState, deserializedState)
	}
}

// Helper function to compare two State objects
func compareStates(a, b *State) bool {
	if a.Metadata.Version != b.Metadata.Version ||
		!a.Metadata.Timestamp.Equal(b.Metadata.Timestamp) ||
		a.Metadata.Aggregate != b.Metadata.Aggregate ||
		len(a.Metadata.Extends) != len(b.Metadata.Extends) {
		return false
	}
	for i := range a.Metadata.Extends {
		if a.Metadata.Extends[i] != b.Metadata.Extends[i] {
			return false
		}
	}

	if len(a.DeletedSnapshots) != len(b.DeletedSnapshots) {
		return false
	}
	for k, v := range a.DeletedSnapshots {
		if bv, ok := b.DeletedSnapshots[k]; !ok || !v.Equal(bv) {
			return false
		}
	}

	if len(a.IdToChecksum) != len(b.IdToChecksum) {
		return false
	}
	for k, v := range a.IdToChecksum {
		if bv, ok := b.IdToChecksum[k]; !ok || bv != v {
			return false
		}
	}

	if len(a.Chunks) != len(b.Chunks) {
		return false
	}
	for k, v := range a.Chunks {
		if bv, ok := b.Chunks[k]; !ok || bv != v {
			return false
		}
	}

	return true
}

func TestIdToChecksumSerialization(t *testing.T) {
	originalState := &State{
		IdToChecksum: map[uint64]objects.Checksum{
			1: {0x10, 0x11, 0x12, 0x13},
			2: {0x20, 0x21, 0x22, 0x23},
		},
	}

	var buffer bytes.Buffer
	if err := originalState.SerializeStream(&buffer); err != nil {
		t.Fatalf("Failed to serialize state: %v", err)
	}

	deserializedState, err := DeserializeStream(&buffer)
	if err != nil {
		t.Fatalf("Failed to deserialize state: %v", err)
	}

	if !reflect.DeepEqual(originalState.IdToChecksum, deserializedState.IdToChecksum) {
		t.Fatalf("IdToChecksum mismatch.\nOriginal: %+v\nDeserialized: %+v",
			originalState.IdToChecksum, deserializedState.IdToChecksum)
	}
}
