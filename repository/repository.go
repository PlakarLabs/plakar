package repository

import (
	"bytes"
	"errors"
	"hash"
	"io"
	"strings"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/ultracdc"
	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/hashing"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository/state"
	"github.com/PlakarKorp/plakar/storage"
)

var (
	ErrPackfileNotFound = errors.New("packfile not found")
	ErrBlobNotFound     = errors.New("blob not found")
)

type Repository struct {
	store         storage.Store
	state         *state.State
	configuration storage.Configuration

	context *context.Context

	secret []byte
}

func New(ctx *context.Context, store storage.Store, secret []byte) (*Repository, error) {
	t0 := time.Now()
	defer func() {
		ctx.GetLogger().Trace("repository", "New(store=%p): %s", store, time.Since(t0))
	}()

	r := &Repository{
		store:         store,
		configuration: store.Configuration(),
		context:       ctx,
		secret:        secret,
	}
	if err := r.RebuildState(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Repository) RebuildState() error {
	cacheInstance, err := r.Context().GetCache().Repository(r.Configuration().RepositoryID)
	if err != nil {
		return err
	}

	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "rebuildState(): %s", time.Since(t0))
	}()

	// identify local states
	localStates := make(map[objects.Checksum]struct{})
	statesChan, err := cacheInstance.ListStates()
	if err != nil {
		return err
	}

	for stateID := range statesChan {
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
			remoteStateRd, err := r.GetState(stateID)
			if err != nil {
				return err
			}
			remoteState, err := io.ReadAll(remoteStateRd)
			if err != nil {
				return err
			}

			if exists, err := cacheInstance.HasState(stateID); err != nil {
				return err
			} else if !exists {
				if err := cacheInstance.PutState(stateID, remoteState); err != nil {
					return err
				}
			}
			localStates[stateID] = struct{}{}
		}

		// delete local states that are not present in remote
		for _, stateID := range outdatedStates {
			delete(localStates, stateID)
			if err := cacheInstance.DelState(stateID); err != nil {
				return err
			}
		}
	}

	// merge all local states into a new aggregate state
	aggregateState := state.New()

	for stateID := range localStates {
		idxRd, err := r.GetState(stateID)
		if err != nil {
			return err
		}

		idx, err := io.ReadAll(idxRd)
		if err != nil {
			return err
		}

		tmp, err := state.DeserializeStream(bytes.NewReader(idx))
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
	return r.context
}

func (r *Repository) Store() storage.Store {
	return r.store
}

func (r *Repository) Close() error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Close(): %s", time.Since(t0))
	}()

	if r.state.Dirty() {
	}

	return nil
}

func (r *Repository) Decode(input io.Reader) (io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Decode: %s", time.Since(t0))
	}()

	stream := input
	if r.secret != nil {
		tmp, err := encryption.DecryptStream(r.secret, stream)
		if err != nil {
			return nil, err
		}
		stream = tmp
	}

	if r.configuration.Compression != nil {
		tmp, err := compression.InflateStream(r.configuration.Compression.Algorithm, stream)
		if err != nil {
			return nil, err
		}
		stream = tmp
	}

	return stream, nil
}

func (r *Repository) Encode(input io.Reader) (io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Encode: %s", time.Since(t0))
	}()

	stream := input
	if r.configuration.Compression != nil {
		tmp, err := compression.DeflateStream(r.configuration.Compression.Algorithm, stream)
		if err != nil {
			return nil, err
		}
		stream = tmp
	}

	if r.secret != nil {
		tmp, err := encryption.EncryptStream(r.secret, stream)
		if err != nil {
			return nil, err
		}
		stream = tmp
	}

	return stream, nil
}

func (r *Repository) DecodeBuffer(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Decode(%d bytes): %s", len(buffer), time.Since(t0))
	}()

	rd, err := r.Decode(bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}

func (r *Repository) EncodeBuffer(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "Encode(%d): %s", len(buffer), time.Since(t0))
	}()

	rd, err := r.Encode(bytes.NewBuffer(buffer))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
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
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetSnapshots(): %s", time.Since(t0))
	}()

	ret := make([]objects.Checksum, 0)
	for snapshotID := range r.state.ListSnapshots() {
		ret = append(ret, snapshotID)
	}
	return ret, nil
}

func (r *Repository) DeleteSnapshot(snapshotID objects.Checksum) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "DeleteSnapshot(%x): %s", snapshotID, time.Since(t0))
	}()

	ret := r.state.DeleteSnapshot(snapshotID)
	if ret != nil {
		return ret
	}

	var buffer bytes.Buffer
	err := r.state.SerializeStream(&buffer)
	if err != nil {
		return err
	}

	checksum := r.Checksum(buffer.Bytes())
	if err := r.PutState(checksum, &buffer); err != nil {
		return err
	}
	return nil
}

func (r *Repository) GetStates() ([]objects.Checksum, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetStates(): %s", time.Since(t0))
	}()

	return r.store.GetStates()
}

func (r *Repository) GetState(checksum objects.Checksum) (io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetState(%x): %s", checksum, time.Since(t0))
	}()

	rd, err := r.store.GetState(checksum)
	if err != nil {
		return nil, err
	}
	return r.Decode(rd)
}

func (r *Repository) PutState(checksum objects.Checksum, rd io.Reader) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "PutState(%x, ...): %s", checksum, time.Since(t0))
	}()

	rd, err := r.Encode(rd)
	if err != nil {
		return err
	}
	return r.store.PutState(checksum, rd)
}

func (r *Repository) DeleteState(checksum objects.Checksum) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "DeleteState(%x, ...): %s", checksum, time.Since(t0))
	}()

	return r.store.DeleteState(checksum)
}

func (r *Repository) GetPackfiles() ([]objects.Checksum, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetPackfiles(): %s", time.Since(t0))
	}()

	return r.store.GetPackfiles()
}

func (r *Repository) GetPackfile(checksum objects.Checksum) (io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetPackfile(%x, ...): %s", checksum, time.Since(t0))
	}()

	return r.store.GetPackfile(checksum)
}

func (r *Repository) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetPackfileBlob(%x, %d, %d): %s", checksum, offset, length, time.Since(t0))
	}()

	rd, err := r.store.GetPackfileBlob(checksum, offset, length)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	decoded, err := r.DecodeBuffer(data)
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(decoded), nil
}

func (r *Repository) PutPackfile(checksum objects.Checksum, rd io.Reader) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "PutPackfile(%x, ...): %s", checksum, time.Since(t0))
	}()

	return r.store.PutPackfile(checksum, rd)
}

func (r *Repository) DeletePackfile(checksum objects.Checksum) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "DeletePackfile(%x): %s", checksum, time.Since(t0))
	}()

	return r.store.DeletePackfile(checksum)
}

func (r *Repository) GetBlob(Type packfile.Type, checksum objects.Checksum) (io.Reader, error) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "GetBlob(%x): %s", checksum, time.Since(t0))
	}()

	packfileChecksum, offset, length, exists := r.state.GetSubpartForBlob(Type, checksum)
	if !exists {
		return nil, ErrPackfileNotFound
	}

	rd, err := r.GetPackfileBlob(packfileChecksum, offset, length)
	if err != nil {
		return nil, err
	}

	return rd, nil
}

func (r *Repository) BlobExists(Type packfile.Type, checksum objects.Checksum) bool {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "BlobExists(%x): %s", checksum, time.Since(t0))
	}()

	return r.state.BlobExists(Type, checksum)
}

func (r *Repository) ListSnapshots() <-chan objects.Checksum {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "ListSnapshots(): %s", time.Since(t0))
	}()
	return r.state.ListSnapshots()
}

func (r *Repository) SetPackfileForBlob(Type packfile.Type, packfileChecksum objects.Checksum, chunkChecksum objects.Checksum, offset uint32, length uint32) {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "SetPackfileForBlob(%x, %x, %d, %d): %s", packfileChecksum, chunkChecksum, offset, length, time.Since(t0))
	}()

	r.state.SetPackfileForBlob(Type, packfileChecksum, chunkChecksum, offset, length)
}

func (r *Repository) Logger() *logging.Logger {
	return r.Context().GetLogger()
}
