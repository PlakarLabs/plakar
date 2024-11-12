package repository

import (
	"bytes"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"time"

	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/profiler"
	"github.com/PlakarKorp/plakar/repository/cache"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/storage"
	chunkers "github.com/PlakarLabs/go-cdc-chunkers"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/ultracdc"
)

type Repository struct {
	store         *storage.Store
	cache         *cache.Cache
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

	cacheDir := filepath.Join(store.Context().GetCacheDir(), "repository", store.Configuration().RepositoryID.String(), "states")
	cacheInstance, err := cache.New(cacheDir)
	if err != nil {
		return nil, err
	}

	r := &Repository{
		store:         store,
		cache:         cacheInstance,
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

func (r *Repository) Context() *context.Context {
	return r.store.Context()
}

func (r *Repository) Store() *storage.Store {
	return r.store
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

func (r *Repository) NewStateDelta() *state.State {
	return r.state.Derive()
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

func (r *Repository) GetChunk(checksum [32]byte) (io.Reader, uint64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetChunk", time.Since(t0))
		logger.Trace("repository", "GetChunk(%x): %s", checksum, time.Since(t0))
	}()

	packfileChecksum, offset, length, exists := r.state.GetSubpartForChunk(checksum)
	if !exists {
		return nil, 0, fmt.Errorf("packfile not found")
	}

	rd, len, err := r.GetPackfileBlob(packfileChecksum, offset, length)
	if err != nil {
		return nil, 0, err
	}

	return rd, uint64(len), nil
}

func (r *Repository) GetObject(checksum [32]byte) (io.Reader, uint64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetObject", time.Since(t0))
		logger.Trace("repository", "GetObject(%x): %s", checksum, time.Since(t0))
	}()

	packfileChecksum, offset, length, exists := r.state.GetSubpartForObject(checksum)
	if !exists {
		return nil, 0, fmt.Errorf("packfile not found")
	}

	rd, len, err := r.GetPackfileBlob(packfileChecksum, offset, length)
	if err != nil {
		return nil, 0, err
	}

	return rd, uint64(len), nil
}

func (r *Repository) GetFile(checksum [32]byte) (io.Reader, uint64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetFile", time.Since(t0))
		logger.Trace("repository", "GetFile(%x): %s", checksum, time.Since(t0))
	}()

	packfileChecksum, offset, length, exists := r.state.GetSubpartForFile(checksum)
	if !exists {
		return nil, 0, fmt.Errorf("packfile not found")
	}

	rd, len, err := r.GetPackfileBlob(packfileChecksum, offset, length)
	if err != nil {
		return nil, 0, err
	}

	return rd, uint64(len), nil
}

func (r *Repository) GetDirectory(checksum [32]byte) (io.Reader, uint64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetDirectory", time.Since(t0))
		logger.Trace("repository", "GetDirectory(%x): %s", checksum, time.Since(t0))
	}()

	packfileChecksum, offset, length, exists := r.state.GetSubpartForDirectory(checksum)
	if !exists {
		return nil, 0, fmt.Errorf("packfile not found")
	}

	rd, len, err := r.GetPackfileBlob(packfileChecksum, offset, length)
	if err != nil {
		return nil, 0, err
	}

	return rd, uint64(len), nil
}

func (r *Repository) GetData(checksum [32]byte) (io.Reader, uint64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetData", time.Since(t0))
		logger.Trace("repository", "GetData(%x): %s", checksum, time.Since(t0))
	}()

	packfileChecksum, offset, length, exists := r.state.GetSubpartForData(checksum)
	if !exists {
		return nil, 0, fmt.Errorf("packfile not found")
	}

	rd, len, err := r.GetPackfileBlob(packfileChecksum, offset, length)
	if err != nil {
		return nil, 0, err
	}

	return rd, uint64(len), nil
}

func (r *Repository) GetSnapshot(snapshotID [32]byte) (io.Reader, uint64, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.GetSnapshot", time.Since(t0))
		logger.Trace("repository", "GetSnapshot(%x): %s", snapshotID, time.Since(t0))
	}()

	packfile, offset, length, exists := r.state.GetSubpartForSnapshot(snapshotID)
	if !exists {
		return nil, 0, fmt.Errorf("snapshot not found")
	}

	rd, len, err := r.GetPackfileBlob(packfile, offset, length)
	if err != nil {
		return nil, 0, err
	}

	return rd, uint64(len), nil
}

func (r *Repository) ChunkExists(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.ChunkExists", time.Since(t0))
		logger.Trace("repository", "ChunkExists(%x): %s", checksum, time.Since(t0))
	}()

	return r.state.ChunkExists(checksum)
}

func (r *Repository) ObjectExists(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.ObjectExists", time.Since(t0))
		logger.Trace("repository", "ObjectExists(%x): %s", checksum, time.Since(t0))
	}()

	return r.state.ObjectExists(checksum)
}

func (r *Repository) FileExists(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.ObjectExists", time.Since(t0))
		logger.Trace("repository", "ObjectExists(%x): %s", checksum, time.Since(t0))
	}()

	return r.state.FileExists(checksum)
}

func (r *Repository) DirectoryExists(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DirectoryExists", time.Since(t0))
		logger.Trace("repository", "DirectoryExists(%x): %s", checksum, time.Since(t0))
	}()

	return r.state.DirectoryExists(checksum)
}

func (r *Repository) DataExists(checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.DataExists", time.Since(t0))
		logger.Trace("repository", "DataExists(%x): %s", checksum, time.Since(t0))
	}()

	return r.state.DataExists(checksum)
}

func (r *Repository) ListSnapshots() <-chan [32]byte {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.ListSnapshots", time.Since(t0))
		logger.Trace("repository", "ListSnapshots(): %s", time.Since(t0))
	}()
	return r.state.ListSnapshots()
}

func (r *Repository) SetPackfileForChunk(packfileChecksum [32]byte, chunkChecksum [32]byte, offset uint32, length uint32) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.SetPackfileForChunk", time.Since(t0))
		logger.Trace("repository", "SetPackfileForChunk(%x, %x, %d, %d): %s", packfileChecksum, chunkChecksum, offset, length, time.Since(t0))
	}()

	r.state.SetPackfileForChunk(packfileChecksum, chunkChecksum, offset, length)
}

func (r *Repository) SetPackfileForObject(packfileChecksum [32]byte, objectChecksum [32]byte, offset uint32, length uint32) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.SetPackfileForObject", time.Since(t0))
		logger.Trace("repository", "SetPackfileForObject(%x, %x, %d, %d): %s", packfileChecksum, objectChecksum, offset, length, time.Since(t0))
	}()

	r.state.SetPackfileForObject(packfileChecksum, objectChecksum, offset, length)
}

func (r *Repository) SetPackfileForFile(packfileChecksum [32]byte, fileChecksum [32]byte, offset uint32, length uint32) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.SetPackfileForFile", time.Since(t0))
		logger.Trace("repository", "SetPackfileForFile(%x, %x, %d, %d): %s", packfileChecksum, fileChecksum, offset, length, time.Since(t0))
	}()

	r.state.SetPackfileForFile(packfileChecksum, fileChecksum, offset, length)
}

func (r *Repository) SetPackfileForDirectory(packfileChecksum [32]byte, directoryChecksum [32]byte, offset uint32, length uint32) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.SetPackfileForDirectory", time.Since(t0))
		logger.Trace("repository", "SetPackfileForDirectory(%x, %x, %d, %d): %s", packfileChecksum, directoryChecksum, offset, length, time.Since(t0))
	}()

	r.state.SetPackfileForDirectory(packfileChecksum, directoryChecksum, offset, length)
}

func (r *Repository) SetPackfileForData(packfileChecksum [32]byte, dataChecksum [32]byte, offset uint32, length uint32) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.SetPackfileForData", time.Since(t0))
		logger.Trace("repository", "SetPackfileForData(%x, %x, %d, %d): %s", packfileChecksum, dataChecksum, offset, length, time.Since(t0))
	}()

	r.state.SetPackfileForData(packfileChecksum, dataChecksum, offset, length)
}

func (r *Repository) SetPackfileForSnapshot(packfileChecksum [32]byte, snapshotID [32]byte, offset uint32, length uint32) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.SetPackfileForSnapshot", time.Since(t0))
		logger.Trace("repository", "SetPackfileForSnapshot(%x, %x, %d, %d): %s", packfileChecksum, snapshotID, offset, length, time.Since(t0))
	}()

	r.state.SetPackfileForSnapshot(packfileChecksum, snapshotID, offset, length)
}
