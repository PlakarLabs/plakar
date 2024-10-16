package hashing

import (
	"testing"

	"github.com/zeebo/blake3"
)

func TestDefaultAlgorithm(t *testing.T) {
	expected := "sha256"
	actual := DefaultAlgorithm()
	if actual != expected {
		t.Errorf("Expected default algorithm %s, but got %s", expected, actual)
	}
}

func TestGetHasher(t *testing.T) {
	// Test for sha256
	hasher := GetHasher("sha256")
	if hasher == nil {
		t.Error("Expected sha256 hasher, but got nil")
	}

	// Test for blake3
	hasher = GetHasher("blake3")
	if hasher == nil {
		t.Error("Expected blake3 hasher, but got nil")
	}
	if _, ok := hasher.(*blake3.Hasher); !ok {
		t.Errorf("Expected blake3 hasher type, but got %T", hasher)
	}

	// Test for unknown algorithm
	hasher = GetHasher("unknown")
	if hasher != nil {
		t.Error("Expected nil for unknown algorithm, but got non-nil")
	}
}
