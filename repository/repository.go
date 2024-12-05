package repository

import (
	"bytes"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
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
	if err := r.RebuildState(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repository) RebuildState() error {
	// identify local states
	localStates := make(map[objects.Checksum]struct{})
	for stateID := range r.cache.List() {
		localStates[stateID] = struct{}{}
	}

	// identify remote states
	remoteStates, err := r.GetStates()
	if err != nil {
		return err
	}
	remoteStatesMap := make(map[objects.Checksum]struct{})
	for _, stateID := range remoteStates {
		remoteStatesMap[stateID] = struct{}{}
	}

	desynchronized := false

	// build delta of local and remote states
	missingStates := make([]objects.Checksum, 0)
	for _, stateID := range remoteStates {
		if _, exists := localStates[stateID]; !exists {
			missingStates = append(missingStates, stateID)
			desynchronized = true
		}
	}

	outdatedStates := make([]objects.Checksum, 0)
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
	if r.state.Dirty() {
	}

	return nil
}

func (r *Repository) Decode(buffer []byte) ([]byte, error) {
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

	if r.configuration.Compression != nil {
		tmp, err := compression.InflateStream(r.configuration.Compression.Algorithm, bytes.NewReader(buffer))
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
	if r.configuration.Compression != nil {
		tmp, err := compression.DeflateStream(r.configuration.Compression.Algorithm, bytes.NewReader(buffer))
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
	return hashing.GetHasher(r.Configuration().Hashing.Algorithm)
}

func (r *Repository) Checksum(data []byte) objects.Checksum {
	hasher := r.Hasher()
	hasher.Write(data)
	result := hasher.Sum(nil)

	if len(result) != 32 {
		panic("hasher returned invalid length")
	}

	var checksum objects.Checksum
	copy(checksum[:], result)

	return checksum
}

func (r *Repository) Chunker(rd io.ReadCloser) (*chunkers.Chunker, error) {
	chunkingAlgorithm := r.configuration.Chunking.Algorithm
	chunkingMinSize := r.configuration.Chunking.MinSize
	chunkingNormalSize := r.configuration.Chunking.NormalSize
	chunkingMaxSize := r.configuration.Chunking.MaxSize

	return chunkers.NewChunker(strings.ToLower(chunkingAlgorithm), rd, &chunkers.ChunkerOpts{
		MinSize:    int(chunkingMinSize),
		NormalSize: int(chunkingNormalSize),
		MaxSize:    int(chunkingMaxSize),
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

func (r *Repository) GetSnapshots() ([]objects.Checksum, error) {
	ret := make([]objects.Checksum, 0)
	for snapshotID := range r.state.ListSnapshots() {
		ret = append(ret, snapshotID)
	}
	return ret, nil
}

func (r *Repository) DeleteSnapshot(snapshotID objects.Checksum) error {
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

func (r *Repository) GetStates() ([]objects.Checksum, error) {
	return r.store.GetStates()
}

func (r *Repository) GetState(checksum objects.Checksum) ([]byte, int64, error) {
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

func (r *Repository) PutState(checksum objects.Checksum, rd io.Reader, size int64) (int, error) {
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

func (r *Repository) DeleteState(checksum objects.Checksum) error {
	return r.store.DeleteState(checksum)
}

func (r *Repository) GetPackfiles() ([]objects.Checksum, error) {
	return r.store.GetPackfiles()
}

func (r *Repository) GetPackfile(checksum objects.Checksum) (io.Reader, uint64, error) {
	return r.store.GetPackfile(checksum)
}

func (r *Repository) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, int64, error) {
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

func (r *Repository) PutPackfile(checksum objects.Checksum, rd io.Reader, size uint64) error {
	return r.store.PutPackfile(checksum, rd, size)
}

func (r *Repository) DeletePackfile(checksum objects.Checksum) error {
	return r.store.DeletePackfile(checksum)
}

func (r *Repository) GetBlob(Type packfile.Type, checksum objects.Checksum) (io.Reader, uint64, error) {
	packfileChecksum, offset, length, exists := r.state.GetSubpartForBlob(Type, checksum)
	if !exists {
		return nil, 0, fmt.Errorf("packfile not found")
	}

	rd, len, err := r.GetPackfileBlob(packfileChecksum, offset, length)
	if err != nil {
		return nil, 0, err
	}

	return rd, uint64(len), nil
}

func (r *Repository) BlobExists(Type packfile.Type, checksum objects.Checksum) bool {
	return r.state.BlobExists(Type, checksum)
}

func (r *Repository) ListSnapshots() <-chan objects.Checksum {
	return r.state.ListSnapshots()
}

func (r *Repository) SetPackfileForBlob(Type packfile.Type, packfileChecksum objects.Checksum, chunkChecksum objects.Checksum, offset uint32, length uint32) {
	r.state.SetPackfileForBlob(Type, packfileChecksum, chunkChecksum, offset, length)
}
