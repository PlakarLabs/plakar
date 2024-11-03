package compression

import (
	"bytes"
	"io"
	"testing"
)

// Helper function to compress and then decompress data and verify correctness
func testCompressionDecompression(t *testing.T, algorithm string, data []byte) {
	// Compress data
	compressedReader, err := DeflateStream(algorithm, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DeflateStream failed for %s: %v", algorithm, err)
	}

	// Decompress data
	decompressedReader, err := InflateStream(algorithm, compressedReader)
	if err != nil {
		t.Fatalf("InflateStream failed for %s: %v", algorithm, err)
	}

	// Read decompressed data
	var decompressedData bytes.Buffer
	_, err = io.Copy(&decompressedData, decompressedReader)
	if err != nil {
		t.Fatalf("Reading decompressed data failed for %s: %v", algorithm, err)
	}

	// Compare original and decompressed data
	if !bytes.Equal(data, decompressedData.Bytes()) {
		t.Errorf("Decompressed data does not match original for %s. Got: %v, Want: %v", algorithm, decompressedData.Bytes(), data)
	}
}

func TestCompression(t *testing.T) {
	tests := []struct {
		algorithm string
		data      []byte
	}{
		{"gzip", []byte("Hello, world!")},
		{"gzip", []byte{}}, // Test empty buffer for gzip
		{"lz4", []byte("Hello, world!")},
		{"lz4", []byte{}}, // Test empty buffer for lz4
	}

	for _, tt := range tests {
		t.Run(tt.algorithm, func(t *testing.T) {
			testCompressionDecompression(t, tt.algorithm, tt.data)
		})
	}
}
