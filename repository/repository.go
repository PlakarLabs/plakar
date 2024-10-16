package repository

import (
	"time"

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
)

type Repository struct {
	store         *storage.Store
	configuration storage.Configuration
}

func New(store *storage.Store) *Repository {
	return &Repository{
		store:         store,
		configuration: store.Configuration(),
	}
}

func (r *Repository) Close() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.Close", time.Since(t0))
	}()

	return r.store.Close()
}

func (r *Repository) Secret() []byte {
	return r.store.GetSecret()
}

func (r *Repository) Store() *storage.Store {
	return r.store
}

func (r *Repository) Location() string {
	return r.store.Location
}

func (r *Repository) Configuration() storage.Configuration {
	return r.configuration
}

func (r *Repository) Decode(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.Decode", time.Since(t0))
	}()

	if r.store.GetSecret() != nil {
		tmp, err := encryption.Decrypt(r.store.GetSecret(), buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if r.configuration.Compression != "" {
		tmp, err := compression.Inflate(r.configuration.Compression, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}
	return buffer, nil
}

func (r *Repository) Encode(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.Encode", time.Since(t0))
	}()

	if r.configuration.Compression != "" {
		tmp, err := compression.Deflate(r.configuration.Compression, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if r.store.GetSecret() != nil {
		tmp, err := encryption.Encrypt(r.store.GetSecret(), buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	return buffer, nil
}

func (r *Repository) GetSnapshots() ([]uuid.UUID, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetSnapshots", time.Since(t0))
	}()

	return r.store.GetSnapshots()
}

func (r *Repository) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetSnapshot", time.Since(t0))
	}()

	buffer, err := r.store.GetSnapshot(indexID)
	if err != nil {
		return nil, err
	}

	return r.Decode(buffer)
}

func (r *Repository) PutSnapshot(indexID uuid.UUID, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.PutSnapshot", time.Since(t0))
	}()

	data, err := r.Encode(data)
	if err != nil {
		return err
	}

	return r.store.PutSnapshot(indexID, data)
}

func (r *Repository) GetBlobs() ([][32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetBlobs", time.Since(t0))
	}()

	return r.store.GetBlobs()
}

func (r *Repository) GetBlob(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetBlob", time.Since(t0))
	}()

	buffer, err := r.store.GetBlob(checksum)
	if err != nil {
		return nil, err
	}

	return r.Decode(buffer)
}

func (r *Repository) PutBlob(checksum [32]byte, data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.PutBlob", time.Since(t0))
	}()

	data, err := r.Encode(data)
	if err != nil {
		return 0, err
	}

	return len(data), r.store.PutBlob(checksum, data)
}

func (r *Repository) CheckBlob(checksum [32]byte) (bool, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.CheckBlob", time.Since(t0))
	}()

	return r.store.CheckBlob(checksum)
}

func (r *Repository) DeleteBlob(checksum [32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeleteBlob", time.Since(t0))
	}()

	return r.store.DeleteBlob(checksum)
}

func (r *Repository) GetStates() ([][32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetStates", time.Since(t0))
	}()

	return r.store.GetStates()
}

func (r *Repository) GetState(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetState", time.Since(t0))
	}()

	buffer, err := r.store.GetState(checksum)
	if err != nil {
		return nil, err
	}

	return r.Decode(buffer)
}

func (r *Repository) PutState(checksum [32]byte, data []byte) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.PutState", time.Since(t0))
	}()

	data, err := r.Encode(data)
	if err != nil {
		return 0, err
	}

	return len(data), r.store.PutState(checksum, data)
}

func (r *Repository) DeleteState(checksum [32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeleteState", time.Since(t0))
	}()

	return r.store.DeleteState(checksum)
}

func (r *Repository) GetPackfiles() ([][32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetPackfiles", time.Since(t0))
	}()

	return r.store.GetPackfiles()
}

func (r *Repository) GetPackfile(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetPackfile", time.Since(t0))
	}()

	return r.store.GetPackfile(checksum)
}

func (r *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetPackfileBlob", time.Since(t0))
	}()

	return r.store.GetPackfileBlob(checksum, offset, length)
}

func (r *Repository) PutPackfile(checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.PutPackfile", time.Since(t0))
	}()

	return r.store.PutPackfile(checksum, data)
}

func (r *Repository) DeletePackfile(checksum [32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeletePackfile", time.Since(t0))
	}()

	return r.store.DeletePackfile(checksum)
}

func (r *Repository) DeleteSnapshot(indexID uuid.UUID) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeleteSnapshot", time.Since(t0))
	}()

	return r.store.DeleteSnapshot(indexID)
}

func (r *Repository) Commit(indexID uuid.UUID, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.Commit", time.Since(t0))
	}()

	data, err := r.Encode(data)
	if err != nil {
		return err
	}
	return r.store.Commit(indexID, data)
}
