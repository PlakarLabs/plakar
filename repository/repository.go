package repository

import (
	"hash"
	"time"

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/hashing"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/repository/state"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
)

type Repository struct {
	store         *storage.Store
	state         *state.State
	configuration storage.Configuration

	secret []byte
}

func New(store *storage.Store, secret []byte) (*Repository, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.New", time.Since(t0))
		logger.Trace("repository", "New(store=%p): %s", store, time.Since(t0))
	}()

	r := &Repository{
		store:         store,
		configuration: store.Configuration(),
		secret:        secret,
	}
	if err := r.rebuildState(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repository) rebuildState() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.rebuildState", time.Since(t0))
		logger.Trace("repository", "rebuildState(): %s", time.Since(t0))
	}()

	indexes, err := r.GetStates()
	if err != nil {
		return err
	}

	aggregateState := state.New()
	for _, indexID := range indexes {
		idx, err := r.GetState(indexID)
		if err != nil {
			return err
		}
		tmp, err := state.NewFromBytes(idx)
		if err != nil {
			return err
		}
		aggregateState.Merge(indexID, tmp)
		aggregateState.Extends(indexID)
	}

	aggregateState.ResetDirty()
	r.state = aggregateState
	return nil
}

func (r *Repository) Close() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.Close", time.Since(t0))
		logger.Trace("repository", "Close(): %s", time.Since(t0))
	}()

	if r.state.Dirty() {
	}

	return nil
}

func (r *Repository) Decode(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.Decode", time.Since(t0))
		logger.Trace("repository", "Decode(%d bytes): %s", len(buffer), time.Since(t0))
	}()

	if r.secret != nil {
		tmp, err := encryption.Decrypt(r.secret, buffer)
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
		logger.Trace("repository", "Encode(%d): %s", len(buffer), time.Since(t0))
	}()

	if r.configuration.Compression != "" {
		tmp, err := compression.Deflate(r.configuration.Compression, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	if r.secret != nil {
		tmp, err := encryption.Encrypt(r.secret, buffer)
		if err != nil {
			return nil, err
		}
		buffer = tmp
	}

	return buffer, nil
}

func (r *Repository) Hasher() hash.Hash {
	return hashing.GetHasher(r.Configuration().Hashing)
}

func (r *Repository) Checksum(data []byte) []byte {
	hasher := r.Hasher()
	hasher.Write(data)
	return hasher.Sum(nil)
}

func (r *Repository) State() *state.State {
	return r.state
}

func (r *Repository) Location() string {
	return r.store.Location
}

func (r *Repository) Configuration() storage.Configuration {
	return r.configuration
}

func (r *Repository) GetSnapshots() ([]uuid.UUID, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetSnapshots", time.Since(t0))
		logger.Trace("repository", "GetSnapshots(): %s", time.Since(t0))
	}()

	return r.store.GetSnapshots()
}

func (r *Repository) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetSnapshot", time.Since(t0))
		logger.Trace("repository", "GetSnapshot(%s): %s", indexID, time.Since(t0))
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
		logger.Trace("repository", "PutSnapshot(%s, ...): %s", indexID, time.Since(t0))
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
		logger.Trace("repository", "GetBlobs(): %s", time.Since(t0))
	}()

	return r.store.GetBlobs()
}

func (r *Repository) GetBlob(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetBlob", time.Since(t0))
		logger.Trace("repository", "GetBlob(%x): %s", checksum, time.Since(t0))
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
		logger.Trace("repository", "PutBlob(%x, ...): %s", checksum, time.Since(t0))
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
		logger.Trace("repository", "CheckBlob(%x): %s", checksum, time.Since(t0))
	}()

	return r.store.CheckBlob(checksum)
}

func (r *Repository) DeleteBlob(checksum [32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeleteBlob", time.Since(t0))
		logger.Trace("repository", "DeleteBlob(%x): %s", checksum, time.Since(t0))
	}()

	return r.store.DeleteBlob(checksum)
}

func (r *Repository) GetStates() ([][32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetStates", time.Since(t0))
		logger.Trace("repository", "GetStates(): %s", time.Since(t0))
	}()

	return r.store.GetStates()
}

func (r *Repository) GetState(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetState", time.Since(t0))
		logger.Trace("repository", "GetState(%x): %s", checksum, time.Since(t0))
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
		logger.Trace("repository", "PutState(%x, ...): %s", checksum, time.Since(t0))
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
		logger.Trace("repository", "DeleteState(%x, ...): %s", checksum, time.Since(t0))
	}()

	return r.store.DeleteState(checksum)
}

func (r *Repository) GetPackfiles() ([][32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetPackfiles", time.Since(t0))
		logger.Trace("repository", "GetPackfiles(): %s", time.Since(t0))
	}()

	return r.store.GetPackfiles()
}

func (r *Repository) GetPackfile(checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetPackfile", time.Since(t0))
		logger.Trace("repository", "GetPackfile(%x, ...): %s", checksum, time.Since(t0))
	}()

	return r.store.GetPackfile(checksum)
}

func (r *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetPackfileBlob", time.Since(t0))
		logger.Trace("repository", "GetPackfileBlob(%x, %d, %d): %s", checksum, offset, length, time.Since(t0))
	}()

	return r.store.GetPackfileBlob(checksum, offset, length)
}

func (r *Repository) PutPackfile(checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.PutPackfile", time.Since(t0))
		logger.Trace("repository", "PutPackfile(%x, ...): %s", checksum, time.Since(t0))
	}()

	return r.store.PutPackfile(checksum, data)
}

func (r *Repository) DeletePackfile(checksum [32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeletePackfile", time.Since(t0))
		logger.Trace("repository", "DeletePackfile(%x): %s", checksum, time.Since(t0))
	}()

	return r.store.DeletePackfile(checksum)
}

func (r *Repository) DeleteSnapshot(indexID uuid.UUID) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeleteSnapshot", time.Since(t0))
		logger.Trace("repository", "DeleteSnapshot(%s): %s", indexID, time.Since(t0))
	}()

	return r.store.DeleteSnapshot(indexID)
}

func (r *Repository) Commit(indexID uuid.UUID, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.Commit", time.Since(t0))
		logger.Trace("repository", "Commit(%s, ...): %s", indexID, time.Since(t0))
	}()

	data, err := r.Encode(data)
	if err != nil {
		return err
	}
	return r.store.Commit(indexID, data)
}
