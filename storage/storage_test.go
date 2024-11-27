package storage

import (
	"bytes"
	"io"
	"testing"

	"github.com/PlakarKorp/plakar/context"
)

func init() {
	Register("fs", func() Backend { return &MockBackend{} })
}

// MockBackend implements the Backend interface for testing purposes
type MockBackend struct {
	configuration Configuration
}

func (mb *MockBackend) Create(repository string, configuration Configuration) error {
	mb.configuration = configuration
	return nil
}

func (mb *MockBackend) Open(repository string) error {
	return nil
}

func (mb *MockBackend) Configuration() Configuration {
	return mb.configuration
}

func (mb *MockBackend) GetStates() ([][32]byte, error) {
	return nil, nil
}

func (mb *MockBackend) PutState(checksum [32]byte, rd io.Reader, size uint64) error {
	return nil
}

func (mb *MockBackend) GetState(checksum [32]byte) (io.Reader, uint64, error) {
	return bytes.NewReader([]byte("test data")), 8, nil
}

func (mb *MockBackend) DeleteState(checksum [32]byte) error {
	return nil
}

func (mb *MockBackend) GetPackfiles() ([][32]byte, error) {
	return nil, nil
}

func (mb *MockBackend) PutPackfile(checksum [32]byte, rd io.Reader, size uint64) error {
	return nil
}

func (mb *MockBackend) GetPackfile(checksum [32]byte) (io.Reader, uint64, error) {
	return bytes.NewReader([]byte("packfile data")), 13, nil
}

func (mb *MockBackend) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) (io.Reader, uint32, error) {
	return bytes.NewReader([]byte("blob data")), 9, nil
}

func (mb *MockBackend) DeletePackfile(checksum [32]byte) error {
	return nil
}

func (mb *MockBackend) Close() error {
	return nil
}

func TestNewStore(t *testing.T) {
	ctx := context.NewContext()

	store, err := NewStore(ctx, "fs", "/test/location")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.location != "/test/location" {
		t.Errorf("expected location to be '/test/location', got %v", store.location)
	}
}

func TestCreateStore(t *testing.T) {
	ctx := context.NewContext()

	config := NewConfiguration()
	store, err := Create(ctx, "/test/location", *config)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.Configuration().RepositoryID != config.RepositoryID {
		t.Errorf("expected RepositoryID to match, got %v and %v", store.Configuration().RepositoryID, config.RepositoryID)
	}
}

func TestOpenStore(t *testing.T) {
	ctx := context.NewContext()

	store, err := Open(ctx, "/test/location")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.location != "/test/location" {
		t.Errorf("expected location to be '/test/location', got %v", store.location)
	}
}
