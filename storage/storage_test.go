package storage

import (
	"bytes"
	"io"
	"os"
	"runtime"
	"testing"

	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/objects"
)

func init() {
	Register("fs", func(location string) Store { return &MockBackend{location: location} })
}

// MockBackend implements the Backend interface for testing purposes
type MockBackend struct {
	configuration Configuration
	location      string
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

func (mb *MockBackend) Location() string {
	return mb.location
}

func (mb *MockBackend) GetStates() ([]objects.Checksum, error) {
	return nil, nil
}

func (mb *MockBackend) PutState(checksum objects.Checksum, rd io.Reader, size uint64) error {
	return nil
}

func (mb *MockBackend) GetState(checksum objects.Checksum) (io.Reader, uint64, error) {
	return bytes.NewReader([]byte("test data")), 8, nil
}

func (mb *MockBackend) DeleteState(checksum objects.Checksum) error {
	return nil
}

func (mb *MockBackend) GetPackfiles() ([]objects.Checksum, error) {
	return nil, nil
}

func (mb *MockBackend) PutPackfile(checksum objects.Checksum, rd io.Reader, size uint64) error {
	return nil
}

func (mb *MockBackend) GetPackfile(checksum objects.Checksum) (io.Reader, uint64, error) {
	return bytes.NewReader([]byte("packfile data")), 13, nil
}

func (mb *MockBackend) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, uint32, error) {
	return bytes.NewReader([]byte("blob data")), 9, nil
}

func (mb *MockBackend) DeletePackfile(checksum objects.Checksum) error {
	return nil
}

func (mb *MockBackend) Close() error {
	return nil
}

func TestNewStore(t *testing.T) {
	ctx := context.NewContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.SetMaxConcurrency(runtime.NumCPU()*8 + 1)

	store, err := NewStore("fs", "/test/location")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.Location() != "/test/location" {
		t.Errorf("expected location to be '/test/location', got %v", store.Location())
	}
}

func TestCreateStore(t *testing.T) {
	ctx := context.NewContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.SetMaxConcurrency(runtime.NumCPU()*8 + 1)

	config := NewConfiguration()
	store, err := Create("/test/location", *config)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.Configuration().RepositoryID != config.RepositoryID {
		t.Errorf("expected RepositoryID to match, got %v and %v", store.Configuration().RepositoryID, config.RepositoryID)
	}
}

func TestOpenStore(t *testing.T) {
	ctx := context.NewContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.SetMaxConcurrency(runtime.NumCPU()*8 + 1)

	store, err := Open("/test/location")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.Location() != "/test/location" {
		t.Errorf("expected location to be '/test/location', got %v", store.Location())
	}
}
