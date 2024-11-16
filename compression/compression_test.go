package compression

import (
	"bytes"
	"errors"
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
		{"GZIP", []byte("Hello, world!")},
		{"GZIP", []byte{}}, // Test empty buffer for gzip
		{"LZ4", []byte("Hello, world!")},
		{"LZ4", []byte{}}, // Test empty buffer for lz4
	}

	for _, tt := range tests {
		t.Run(tt.algorithm, func(t *testing.T) {
			testCompressionDecompression(t, tt.algorithm, tt.data)
		})
	}
}

func TestDefaultAlgorithm(t *testing.T) {
	expected := "LZ4"
	result := DefaultConfiguration().Algorithm

	if result != expected {
		t.Errorf("DefaultAlgorithm failed: expected %v, got %v", expected, result)
	}
}
func TestUnsupportedAlgorithm(t *testing.T) {
	_, err := DeflateStream("unsupported", bytes.NewReader([]byte("test data")))
	if err == nil {
		t.Error("Expected error for unsupported compression method, got nil")
	}

	_, err = InflateStream("unsupported", bytes.NewReader([]byte("test data")))
	if err == nil {
		t.Error("Expected error for unsupported compression method, got nil")
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("forced read error")
}

func TestDeflateStreamErrorHandling(t *testing.T) {
	_, err := DeflateStream("unsupported", bytes.NewReader([]byte("test data")))
	if err == nil {
		t.Error("Expected error for unsupported compression method, got nil")
	}

	_, err = DeflateStream("gzip", &errorReader{})
	if err == nil {
		t.Error("Expected error for reader failure, got nil")
	}
}

func TestInflateStreamErrorHandling(t *testing.T) {
	_, err := InflateStream("unsupported", bytes.NewReader([]byte("test data")))
	if err == nil {
		t.Error("Expected error for unsupported compression method, got nil")
	}

	_, err = InflateStream("gzip", &errorReader{})
	if err == nil {
		t.Error("Expected error for reader failure, got nil")
	}
}

func TestDeflateStreamRewindLogic(t *testing.T) {
	data := []byte("test rewind logic")
	compressedReader, err := DeflateStream("GZIP", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DeflateStream failed: %v", err)
	}

	buf := make([]byte, 1)
	n, err := compressedReader.Read(buf)
	if err != nil || n != 1 {
		t.Fatalf("Rewind logic test failed: expected 1 byte read, got %d, error: %v", n, err)
	}
}

func TestLargeDataCompression(t *testing.T) {
	largeData := make([]byte, 10*1024*1024) // 10MB of data
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	compressedReader, err := DeflateStream("LZ4", bytes.NewReader(largeData))
	if err != nil {
		t.Fatalf("DeflateStream failed for large data: %v", err)
	}

	decompressedReader, err := InflateStream("LZ4", compressedReader)
	if err != nil {
		t.Fatalf("InflateStream failed for large data: %v", err)
	}

	var decompressedData bytes.Buffer
	_, err = io.Copy(&decompressedData, decompressedReader)
	if err != nil {
		t.Fatalf("Reading decompressed data failed for large data: %v", err)
	}

	if !bytes.Equal(largeData, decompressedData.Bytes()) {
		t.Errorf("Decompressed large data does not match original. Lengths differ")
	}
}
