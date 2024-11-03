package repository

import (
	"bytes"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"time"

	chunkers "github.com/PlakarLabs/go-cdc-chunkers"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/ultracdc"
	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/hashing"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/repository/cache"
	"github.com/PlakarLabs/plakar/repository/events"
	"github.com/PlakarLabs/plakar/repository/state"
	"github.com/PlakarLabs/plakar/storage"
)

type Repository struct {
	store         *storage.Store
	cache         *cache.Cache
	state         *state.State
	events        *events.EventsReceiver
	configuration storage.Configuration

	secret []byte
}

func New(store *storage.Store, secret []byte) (*Repository, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.New", time.Since(t0))
		logger.Trace("repository", "New(store=%p): %s", store, time.Since(t0))
	}()

	cacheDir := filepath.Join(store.Context().GetCacheDir(), "repository", store.Configuration().StoreID.String(), "states")
	cacheInstance, err := cache.New(cacheDir)
	if err != nil {
		return nil, err
	}

	r := &Repository{
		store:         store,
		cache:         cacheInstance,
		events:        events.New(),
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

	// identify local states
	localStates := make(map[[32]byte]struct{})
	for stateID := range r.cache.List() {
		localStates[stateID] = struct{}{}
	}

	// identify remote states
	remoteStates, err := r.GetStates()
	if err != nil {
		return err
	}
	remoteStatesMap := make(map[[32]byte]struct{})
	for _, stateID := range remoteStates {
		remoteStatesMap[stateID] = struct{}{}
	}

	desynchronized := false

	// build delta of local and remote states
	missingStates := make([][32]byte, 0)
	for _, stateID := range remoteStates {
		if _, exists := localStates[stateID]; !exists {
			missingStates = append(missingStates, stateID)
			desynchronized = true
		}
	}

	outdatedStates := make([][32]byte, 0)
	for stateID := range localStates {
		if _, exists := remoteStatesMap[stateID]; !exists {
			outdatedStates = append(outdatedStates, stateID)
			desynchronized = true
		}
	}

	if desynchronized {
		// synchronize local state with unknown remote states
		for _, stateID := range missingStates {
			remoteState, _, err := r.GetState(stateID)
			if err != nil {
				return err
			}
			if r.cache != nil {
				r.cache.Put(stateID, remoteState)
			}
			localStates[stateID] = struct{}{}
		}

		// delete local states that are not present in remote
		for _, stateID := range outdatedStates {
			delete(localStates, stateID)
			r.cache.Delete(stateID)
		}
	}

	// merge all local states into a new aggregate state
	aggregateState := state.New()

	for stateID := range localStates {
		idx, _, err := r.GetState(stateID)
		if err != nil {
			return err
		}
		tmp, err := state.NewFromBytes(idx)
		if err != nil {
			return err
		}
		aggregateState.Merge(stateID, tmp)
		aggregateState.Extends(stateID)
	}
	aggregateState.ResetDirty()
	r.state = aggregateState
	return nil
}

func (r *Repository) Store() *storage.Store {
	return r.store
}

func (r *Repository) Events() *events.EventsReceiver {
	return r.events
}

func (r *Repository) Close() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.Close", time.Since(t0))
		logger.Trace("repository", "Close(): %s", time.Since(t0))
	}()

	r.events.Close()

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
		tmp, err := encryption.DecryptStream(r.secret, bytes.NewReader(buffer))
		if err != nil {
			return nil, err
		}
		buffer, err = io.ReadAll(tmp)
		if err != nil {
			return nil, err
		}
	}

	if r.configuration.Compression != "" {
		tmp, err := compression.InflateStream(r.configuration.Compression, bytes.NewReader(buffer))
		if err != nil {
			return nil, err
		}
		buffer, err = io.ReadAll(tmp)
		if err != nil {
			return nil, err
		}
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
		tmp, err := compression.DeflateStream(r.configuration.Compression, bytes.NewReader(buffer))
		if err != nil {
			return nil, err
		}
		buffer, err = io.ReadAll(tmp)
		if err != nil {
			return nil, err
		}
	}

	if r.secret != nil {
		tmp, err := encryption.EncryptStream(r.secret, bytes.NewReader(buffer))
		if err != nil {
			return nil, err
		}
		buffer, err = io.ReadAll(tmp)
		if err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

func (r *Repository) Hasher() hash.Hash {
	return hashing.GetHasher(r.Configuration().Hashing)
}

func (r *Repository) Checksum(data []byte) [32]byte {
	hasher := r.Hasher()
	hasher.Write(data)
	result := hasher.Sum(nil)

	if len(result) != 32 {
		panic("hasher returned invalid length")
	}

	var checksum [32]byte
	copy(checksum[:], result)

	return checksum
}

func (r *Repository) Chunker(rd io.ReadCloser) (*chunkers.Chunker, error) {
	chunkingAlgorithm := r.configuration.Chunking
	chunkingMinSize := r.configuration.ChunkingMin
	chunkingNormalSize := r.configuration.ChunkingNormal
	chunkingMaxSize := r.configuration.ChunkingMax

	return chunkers.NewChunker(chunkingAlgorithm, rd, &chunkers.ChunkerOpts{
		MinSize:    chunkingMinSize,
		NormalSize: chunkingNormalSize,
		MaxSize:    chunkingMaxSize,
	})
}

func (r *Repository) State() *state.State {
	return r.state
}

func (r *Repository) Location() string {
	return r.store.Location()
}

func (r *Repository) Configuration() storage.Configuration {
	return r.configuration
}

func (r *Repository) GetSnapshots() ([][32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetSnapshots", time.Since(t0))
		logger.Trace("repository", "GetSnapshots(): %s", time.Since(t0))
	}()

	ret := make([][32]byte, 0)
	for snapshotID := range r.state.ListSnapshots() {
		ret = append(ret, snapshotID)
	}
	return ret, nil
}

func (r *Repository) GetSnapshot(snapshotID [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetSnapshot", time.Since(t0))
		logger.Trace("repository", "GetSnapshot(%x): %s", snapshotID, time.Since(t0))
	}()

	packfile, offset, length, exists := r.state.GetSubpartForSnapshot(snapshotID)
	if !exists {
		return nil, fmt.Errorf("snapshot not found")
	}

	blob, _, err := r.GetPackfileBlob(packfile, offset, length)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(blob)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (r *Repository) DeleteSnapshot(snapshotID [32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeleteSnapshot", time.Since(t0))
		logger.Trace("repository", "DeleteSnapshot(%x): %s", snapshotID, time.Since(t0))
	}()

	ret := r.state.DeleteSnapshot(snapshotID)
	if ret != nil {
		return ret
	}

	buffer, err := r.state.Serialize()
	if err != nil {
		return err
	}

	checksum := r.Checksum(buffer)
	if _, err := r.PutState(checksum, bytes.NewBuffer(buffer), int64(len(buffer))); err != nil {
		return err
	}
	return nil
}

func (r *Repository) GetStates() ([][32]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetStates", time.Since(t0))
		logger.Trace("repository", "GetStates(): %s", time.Since(t0))
	}()

	return r.store.GetStates()
}

func (r *Repository) GetState(checksum [32]byte) ([]byte, int64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetState", time.Since(t0))
		logger.Trace("repository", "GetState(%x): %s", checksum, time.Since(t0))
	}()

	if r.cache != nil {
		buffer, err := r.cache.Get(checksum)
		if err == nil {
			return buffer, 0, nil
		}
	}

	rd, _, err := r.store.GetState(checksum)
	if err != nil {
		return nil, 0, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, 0, err
	}

	data, err := r.Decode(buffer)
	if err != nil {
		return nil, 0, err
	}

	return data, int64(len(data)), err
}

func (r *Repository) PutState(checksum [32]byte, rd io.Reader, size int64) (int, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.PutState", time.Since(t0))
		logger.Trace("repository", "PutState(%x, ...): %s", checksum, time.Since(t0))
	}()

	data, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}

	encoded, err := r.Encode(data)
	if err != nil {
		return 0, err
	}

	ret := r.store.PutState(checksum, bytes.NewReader(encoded), uint64(len(encoded)))

	if ret == nil && r.cache != nil {
		r.cache.Put(checksum, data)
	}

	return len(encoded), ret
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

func (r *Repository) GetPackfile(checksum [32]byte) (io.Reader, uint64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetPackfile", time.Since(t0))
		logger.Trace("repository", "GetPackfile(%x, ...): %s", checksum, time.Since(t0))
	}()

	return r.store.GetPackfile(checksum)
}

func (r *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) (io.Reader, int64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetPackfileBlob", time.Since(t0))
		logger.Trace("repository", "GetPackfileBlob(%x, %d, %d): %s", checksum, offset, length, time.Since(t0))
	}()

	rd, _, err := r.store.GetPackfileBlob(checksum, offset, length)
	if err != nil {
		return nil, 0, err
	}

	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, 0, err
	}

	decoded, err := r.Decode(data)
	if err != nil {
		return nil, 0, err
	}

	return bytes.NewBuffer(decoded), int64(len(decoded)), nil
}

func (r *Repository) PutPackfile(checksum [32]byte, rd io.Reader, size uint64) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.PutPackfile", time.Since(t0))
		logger.Trace("repository", "PutPackfile(%x, ...): %s", checksum, time.Since(t0))
	}()

	return r.store.PutPackfile(checksum, rd, size)
}

func (r *Repository) DeletePackfile(checksum [32]byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DeletePackfile", time.Since(t0))
		logger.Trace("repository", "DeletePackfile(%x): %s", checksum, time.Since(t0))
	}()

	return r.store.DeletePackfile(checksum)
}
