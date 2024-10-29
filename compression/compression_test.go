package compression

import (
	"io"
	"strings"
	"testing"
)

func TestDeflateInflateGzipStream(t *testing.T) {
	originalData := "This is some test data to compress and decompress using gzip."
	r := strings.NewReader(originalData)

	// Compress the data
	compressedReader, err := DeflateStream("gzip", r)
	if err != nil {
		t.Fatalf("Failed to compress using gzip: %v", err)
	}

	// Decompress the data
	decompressedReader, err := InflateStream("gzip", compressedReader)
	if err != nil {
		t.Fatalf("Failed to decompress using gzip: %v", err)
	}

	// Read the decompressed data
	decompressedData, err := io.ReadAll(decompressedReader)
	if err != nil {
		t.Fatalf("Failed to read decompressed gzip data: %v", err)
	}

	// Verify the decompressed data matches the original data
	if string(decompressedData) != originalData {
		t.Errorf("Gzip decompressed data mismatch. Got: %q, want: %q", decompressedData, originalData)
	}
}

func TestDeflateInflateLZ4Stream(t *testing.T) {
	originalData := "This is some test data to compress and decompress using lz4."
	r := strings.NewReader(originalData)

	// Compress the data
	compressedReader, err := DeflateStream("lz4", r)
	if err != nil {
		t.Fatalf("Failed to compress using lz4: %v", err)
	}

	// Decompress the data
	decompressedReader, err := InflateStream("lz4", compressedReader)
	if err != nil {
		t.Fatalf("Failed to decompress using lz4: %v", err)
	}

	// Read the decompressed data
	decompressedData, err := io.ReadAll(decompressedReader)
	if err != nil {
		t.Fatalf("Failed to read decompressed lz4 data: %v", err)
	}

	// Verify the decompressed data matches the original data
	if string(decompressedData) != originalData {
		t.Errorf("LZ4 decompressed data mismatch. Got: %q, want: %q", decompressedData, originalData)
	}
}

func TestUnsupportedCompressionMethod(t *testing.T) {
	r := strings.NewReader("test data")

	// Attempt to use an unsupported compression method
	_, err := DeflateStream("unsupported", r)
	if err == nil {
		t.Fatal("Expected an error for unsupported compression method, got nil")
	}

	// Attempt to use an unsupported decompression method
	_, err = InflateStream("unsupported", r)
	if err == nil {
		t.Fatal("Expected an error for unsupported decompression method, got nil")
	}
}
