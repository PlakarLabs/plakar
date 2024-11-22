package state

import (
	"testing"
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

func TestSerializeAndDeserialize(t *testing.T) {
	st := New()

	checksum1 := [32]byte{1, 2, 3}
	checksum2 := [32]byte{4, 5, 6}
	chunkSubpart := Location{
		Offset: 100,
		Length: 200,
	}
	objectSubpart := Location{
		Offset: 300,
		Length: 400,
	}

	st.SetPackfileForChunk(checksum1, checksum2, chunkSubpart.Offset, chunkSubpart.Length)
	st.SetPackfileForObject(checksum1, checksum2, objectSubpart.Offset, objectSubpart.Length)

	serialized, err := st.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserializedSt, err := NewFromBytes(serialized)
	if err != nil {
		t.Fatalf("NewFromBytes failed: %v", err)
	}

	if len(deserializedSt.Chunks) != len(st.Chunks) {
		t.Errorf("Expected Chunks length %d, got %d", len(st.Chunks), len(deserializedSt.Chunks))
	}

	for id, subpart := range st.Chunks {
		deserializedSubpart, exists := deserializedSt.Chunks[id]
		if !exists {
			t.Errorf("Chunk ID %d not found in deserialized Chunks", id)
		}
		if subpart != deserializedSubpart {
			t.Errorf("Chunk Subpart mismatch for ID %d: expected %+v, got %+v", id, subpart, deserializedSubpart)
		}
	}

	if len(deserializedSt.Objects) != len(st.Objects) {
		t.Errorf("Expected Objects length %d, got %d", len(st.Objects), len(deserializedSt.Objects))
	}

	for id, subpart := range st.Objects {
		deserializedSubpart, exists := deserializedSt.Objects[id]
		if !exists {
			t.Errorf("Object ID %d not found in deserialized Objects", id)
		}
		if subpart != deserializedSubpart {
			t.Errorf("Object Subpart mismatch for ID %d: expected %+v, got %+v", id, subpart, deserializedSubpart)
		}
	}
}

func TestNewFromBytesError(t *testing.T) {
	invalidData := []byte{0x00, 0x01, 0x02}

	_, err := NewFromBytes(invalidData)
	if err == nil {
		t.Fatalf("Expected error when deserializing invalid data, got nil")
	}
}

func TestMerge(t *testing.T) {
	st1 := New()
	st2 := New()

	checksumA := [32]byte{10, 20, 30}
	checksumB := [32]byte{40, 50, 60}
	stID := [32]byte{70, 80, 90}

	st1.SetPackfileForChunk(checksumA, checksumB, 100, 200)
	st1.SetPackfileForObject(checksumA, checksumB, 300, 400)

	newChecksum := [32]byte{11, 22, 33}
	st2.SetPackfileForChunk(checksumA, newChecksum, 500, 600)
	st2.SetPackfileForObject(checksumA, newChecksum, 700, 800)

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
	st.SetPackfileForChunk(checksum, checksum, 300, 400)

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

	st.SetPackfileForChunk(packfileChecksum, chunkChecksum, offset, length)

	pf, off, len_, exists := st.GetSubpartForChunk(chunkChecksum)
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
	_, _, _, exists = st.GetSubpartForChunk(nonExisting)
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

	st.SetPackfileForObject(packfileChecksum, objectChecksum, offset, length)

	pf, off, len_, exists := st.GetSubpartForObject(objectChecksum)
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
	_, _, _, exists = st.GetSubpartForObject(nonExisting)
	if exists {
		t.Errorf("Expected GetSubpartForObject to return false for %v", nonExisting)
	}
}
