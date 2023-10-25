package packfile

import (
	"bytes"
	"testing"
)

func TestPackFile(t *testing.T) {
	p := New()

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	checksum1 := [32]byte{1} // Mock checksum for chunk1
	checksum2 := [32]byte{2} // Mock checksum for chunk2

	// Test AddChunk
	p.AddData(1, checksum1, chunk1)
	p.AddData(2, checksum2, chunk2)

	// Test GetChunk
	retrievedChunk1, exists := p.GetChunk(checksum1)
	if !exists || !bytes.Equal(retrievedChunk1, chunk1) {
		t.Fatalf("Expected %s but got %s", chunk1, retrievedChunk1)
	}

	retrievedChunk2, exists := p.GetChunk(checksum2)
	if !exists || !bytes.Equal(retrievedChunk2, chunk2) {
		t.Fatalf("Expected %s but got %s", chunk2, retrievedChunk2)
	}
}

func TestPackFileSerialization(t *testing.T) {
	p := New()

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	checksum1 := [32]byte{1} // Mock checksum for chunk1
	checksum2 := [32]byte{2} // Mock checksum for chunk2

	// Test AddChunk
	p.AddData(1, checksum1, chunk1)
	p.AddData(2, checksum2, chunk2)

	// Test Serialize and NewFromBytes
	serialized, err := p.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	p2, err := NewFromBytes(serialized)
	if err != nil {
		t.Fatalf("Failed to create PackFile from bytes: %v", err)
	}

	// Test that chunks are still retrievable after serialization and deserialization
	retrievedChunk1, exists := p2.GetChunk(checksum1)
	if !exists || !bytes.Equal(retrievedChunk1, chunk1) {
		t.Fatalf("Expected %s but got %s", chunk1, retrievedChunk1)
	}

	retrievedChunk2, exists := p2.GetChunk(checksum2)
	if !exists || !bytes.Equal(retrievedChunk2, chunk2) {
		t.Fatalf("Expected %s but got %s", chunk2, retrievedChunk2)
	}
}
