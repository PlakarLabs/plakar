package state

import (
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	st := New()

	if len(st.Checksums) != 0 {
		t.Errorf("Expected Checksums to be empty, got %d", len(st.Checksums))
	}
	if len(st.checksumsInverse) != 0 {
		t.Errorf("Expected checksumsInverse to be empty, got %d", len(st.checksumsInverse))
	}
	if len(st.Chunks) != 0 {
		t.Errorf("Expected Chunks to be empty, got %d", len(st.Chunks))
	}
	if len(st.Objects) != 0 {
		t.Errorf("Expected Objects to be empty, got %d", len(st.Objects))
	}
	if len(st.Contains) != 0 {
		t.Errorf("Expected Contains to be empty, got %d", len(st.Contains))
	}
	if st.dirty != 0 {
		t.Errorf("Expected dirty to be 0, got %d", st.dirty)
	}
}

func TestSerializeAndDeserialize(t *testing.T) {
	st := New()

	checksum1 := [32]byte{1, 2, 3}
	checksum2 := [32]byte{4, 5, 6}
	chunkSubpart := Subpart{
		PackfileID: 1,
		Offset:     100,
		Length:     200,
	}
	objectSubpart := Subpart{
		PackfileID: 2,
		Offset:     300,
		Length:     400,
	}

	st.addChecksum(checksum1)
	st.addChecksum(checksum2)
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

	if len(deserializedSt.Checksums) != len(st.Checksums) {
		t.Errorf("Expected Checksums length %d, got %d", len(st.Checksums), len(deserializedSt.Checksums))
	}

	for checksum, id := range st.Checksums {
		deserializedID, exists := deserializedSt.Checksums[checksum]
		if !exists {
			t.Errorf("Checksum %v not found in deserialized Checksums", checksum)
		}
		if deserializedID != id {
			t.Errorf("Checksum ID mismatch for %v: expected %d, got %d", checksum, id, deserializedID)
		}
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

	if len(deserializedSt.Contains) != len(st.Contains) {
		t.Errorf("Expected Contains length %d, got %d", len(st.Contains), len(deserializedSt.Contains))
	}

	for id := range st.Contains {
		if _, exists := deserializedSt.Contains[id]; !exists {
			t.Errorf("Contains ID %d not found in deserialized Contains", id)
		}
	}
}

func TestNewFromBytesError(t *testing.T) {
	invalidData := []byte{0x00, 0x01, 0x02}

	_, err := NewFromBytes(invalidData)
	if err == nil {
		t.Fatalf("Expected error when deserializing invalid data, got nil")
	}

	if !strings.Contains(err.Error(), "msgpack") {
		t.Errorf("Expected msgpack error, got %v", err)
	}
}

func TestAddAndLookupChecksum(t *testing.T) {
	st := New()

	checksum := [32]byte{10, 20, 30}
	id := st.addChecksum(checksum)

	if id != 0 {
		t.Errorf("Expected checksum ID to be 0, got %d", id)
	}

	lookup := st.LookupChecksum(id)
	if lookup != checksum {
		t.Errorf("Expected checksum %v, got %v", checksum, lookup)
	}

	// Add the same checksum again
	id2 := st.addChecksum(checksum)
	if id2 != id {
		t.Errorf("Expected same checksum ID %d, got %d", id, id2)
	}
}

func TestSetAndGetPackfileForChunk(t *testing.T) {
	st := New()

	packfileChecksum := [32]byte{1, 1, 1}
	chunkChecksum := [32]byte{2, 2, 2}
	offset := uint32(500)
	length := uint32(1000)

	st.SetPackfileForChunk(packfileChecksum, chunkChecksum, offset, length)

	pf, exists := st.GetPackfileForChunk(chunkChecksum)
	if !exists {
		t.Fatalf("Expected packfile for chunk %v to exist", chunkChecksum)
	}
	if pf != packfileChecksum {
		t.Errorf("Expected packfile checksum %v, got %v", packfileChecksum, pf)
	}

	exists = st.ChunkExists(chunkChecksum)
	if !exists {
		t.Errorf("Expected ChunkExists to return true for %v", chunkChecksum)
	}

	// Test non-existing chunk
	nonExisting := [32]byte{3, 3, 3}
	exists = st.ChunkExists(nonExisting)
	if exists {
		t.Errorf("Expected ChunkExists to return false for %v", nonExisting)
	}
}

func TestSetAndGetPackfileForObject(t *testing.T) {
	st := New()

	packfileChecksum := [32]byte{4, 4, 4}
	objectChecksum := [32]byte{5, 5, 5}
	offset := uint32(1500)
	length := uint32(2000)

	st.SetPackfileForObject(packfileChecksum, objectChecksum, offset, length)

	pf, exists := st.GetPackfileForObject(objectChecksum)
	if !exists {
		t.Fatalf("Expected packfile for object %v to exist", objectChecksum)
	}
	if pf != packfileChecksum {
		t.Errorf("Expected packfile checksum %v, got %v", packfileChecksum, pf)
	}

	exists = st.ObjectExists(objectChecksum)
	if !exists {
		t.Errorf("Expected ObjectExists to return true for %v", objectChecksum)
	}

	// Test non-existing object
	nonExisting := [32]byte{6, 6, 6}
	exists = st.ObjectExists(nonExisting)
	if exists {
		t.Errorf("Expected ObjectExists to return false for %v", nonExisting)
	}
}

func TestMerge(t *testing.T) {
	st1 := New()
	st2 := New()

	checksumA := [32]byte{10, 20, 30}
	checksumB := [32]byte{40, 50, 60}
	stID := [32]byte{70, 80, 90}

	st1.addChecksum(checksumA) // ID 0
	st1.addChecksum(checksumB) // ID 1
	st1.SetPackfileForChunk(checksumA, checksumB, 100, 200)
	st1.SetPackfileForObject(checksumA, checksumB, 300, 400)
	st1.Contains[st1.addChecksum(stID)] = struct{}{} // ID 2

	st2.addChecksum(checksumA) // Already exists in st1, no new ID
	newChecksum := [32]byte{11, 22, 33}
	st2.addChecksum(newChecksum) // ID 1 in st2
	st2.SetPackfileForChunk(checksumA, newChecksum, 500, 600)
	st2.SetPackfileForObject(checksumA, newChecksum, 700, 800)
	st2.Contains[st2.addChecksum(stID)] = struct{}{} // ID 2 in st2

	st1.Merge(stID, st2)

	// Verify Checksums
	expectedChecksums := 4 // checksumA, checksumB, newChecksum, stID
	if len(st1.Checksums) != expectedChecksums {
		t.Errorf("Expected %d checksums, got %d", expectedChecksums, len(st1.Checksums))
	}

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

	// Verify Contains
	expectedContains := 1
	if len(st1.Contains) != expectedContains {
		t.Errorf("Expected %d Contains entry, got %d", expectedContains, len(st1.Contains))
	}

	// Check specific checksums
	for _, checksum := range [][32]byte{checksumA, checksumB, newChecksum, stID} {
		id, exists := st1.Checksums[checksum]
		if !exists {
			t.Errorf("Checksum %v should exist after merge", checksum)
		}
		lookup := st1.LookupChecksum(id)
		if lookup != checksum {
			t.Errorf("LookupChecksum mismatch for ID %d: expected %v, got %v", id, checksum, lookup)
		}
	}
}

func TestListContains(t *testing.T) {
	st := New()

	checksum1 := [32]byte{100, 101, 102}
	checksum2 := [32]byte{103, 104, 105}

	id1 := st.addChecksum(checksum1)
	id2 := st.addChecksum(checksum2)

	st.Contains[id1] = struct{}{}
	st.Contains[id2] = struct{}{}

	contains := st.ListContains()
	if len(contains) != 2 {
		t.Errorf("Expected ListContains to return 2 checksums, got %d", len(contains))
	}

	expected := map[[32]byte]bool{
		checksum1: true,
		checksum2: true,
	}

	for _, checksum := range contains {
		if !expected[checksum] {
			t.Errorf("Unexpected checksum in ListContains: %v", checksum)
		}
	}
}

func TestIsDirtyAndResetDirty(t *testing.T) {
	st := New()

	if st.IsDirty() {
		t.Errorf("Expected IsDirty to be false initially")
	}

	checksum := [32]byte{200, 201, 202}
	st.addChecksum(checksum)

	if !st.IsDirty() {
		t.Errorf("Expected IsDirty to be true after adding a checksum")
	}

	st.ResetDirty()
	if st.IsDirty() {
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
