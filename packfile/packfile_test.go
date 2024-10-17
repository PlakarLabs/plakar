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

	// Test AddBlob
	p.AddBlob(TYPE_CHUNK, checksum1, chunk1)
	p.AddBlob(TYPE_CHUNK, checksum2, chunk2)

	// Test GetBlob
	retrievedChunk1, exists := p.GetBlob(checksum1)
	if !exists || !bytes.Equal(retrievedChunk1, chunk1) {
		t.Fatalf("Expected %s but got %s", chunk1, retrievedChunk1)
	}

	retrievedChunk2, exists := p.GetBlob(checksum2)
	if !exists || !bytes.Equal(retrievedChunk2, chunk2) {
		t.Fatalf("Expected %s but got %s", chunk2, retrievedChunk2)
	}

	// Check PackFile Metadata
	if p.Footer.Count != 2 {
		t.Fatalf("Expected Footer.Count to be 2 but got %d", p.Footer.Count)
	}
	if p.Footer.IndexOffset != uint32(len(p.Blobs)) {
		t.Fatalf("Expected Footer.Length to be %d but got %d", len(p.Blobs), p.Footer.IndexOffset)
	}
}

func TestPackFileSerialization(t *testing.T) {
	p := New()

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	checksum1 := [32]byte{1} // Mock checksum for chunk1
	checksum2 := [32]byte{2} // Mock checksum for chunk2

	// Test AddBlob
	p.AddBlob(TYPE_CHUNK, checksum1, chunk1)
	p.AddBlob(TYPE_CHUNK, checksum2, chunk2)

	// Test Serialize and NewFromBytes
	serialized, err := p.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize PackFile: %v", err)
	}

	p2, err := NewFromBytes(serialized)
	if err != nil {
		t.Fatalf("Failed to create PackFile from bytes: %v", err)
	}

	// Check that metadata is correctly restored after deserialization
	if p2.Footer.Version != p.Footer.Version {
		t.Fatalf("Expected Footer.Version to be %d but got %d", p.Footer.Version, p2.Footer.Version)
	}
	if p2.Footer.Count != p.Footer.Count {
		t.Fatalf("Expected Footer.Count to be %d but got %d", p.Footer.Count, p2.Footer.Count)
	}
	if p2.Footer.IndexOffset != p.Footer.IndexOffset {
		t.Fatalf("Expected Footer.Length to be %d but got %d", p.Footer.IndexOffset, p2.Footer.IndexOffset)
	}
	if p2.Footer.Timestamp != p.Footer.Timestamp {
		t.Fatalf("Expected Footer.Timestamp to be %d but got %d", p.Footer.Timestamp, p2.Footer.Timestamp)
	}

	// Test that chunks are still retrievable after serialization and deserialization
	retrievedChunk1, exists := p2.GetBlob(checksum1)
	if !exists || !bytes.Equal(retrievedChunk1, chunk1) {
		t.Fatalf("Expected %s but got %s", chunk1, retrievedChunk1)
	}

	retrievedChunk2, exists := p2.GetBlob(checksum2)
	if !exists || !bytes.Equal(retrievedChunk2, chunk2) {
		t.Fatalf("Expected %s but got %s", chunk2, retrievedChunk2)
	}
}
