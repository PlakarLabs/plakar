package chunking

import (
	"testing"

	chunkers "github.com/PlakarLabs/go-cdc-chunkers"
)

func TestDefaultAlgorithm(t *testing.T) {
	expected := "fastcdc"
	result := DefaultAlgorithm()

	if result != expected {
		t.Errorf("DefaultAlgorithm failed: expected %v, got %v", expected, result)
	}
}

func TestDefaultConfiguration(t *testing.T) {
	expected := &chunkers.ChunkerOpts{
		MinSize:    64 * 1024,
		NormalSize: 1 * 1024 * 1024,
		MaxSize:    4 * 1024 * 1024,
	}

	result := DefaultConfiguration()

	if result.MinSize != expected.MinSize {
		t.Errorf("DefaultConfiguration MinSize failed: expected %v, got %v", expected.MinSize, result.MinSize)
	}
	if result.NormalSize != expected.NormalSize {
		t.Errorf("DefaultConfiguration NormalSize failed: expected %v, got %v", expected.NormalSize, result.NormalSize)
	}
	if result.MaxSize != expected.MaxSize {
		t.Errorf("DefaultConfiguration MaxSize failed: expected %v, got %v", expected.MaxSize, result.MaxSize)
	}
}
