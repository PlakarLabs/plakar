package repository

import (
	"time"

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/storage"
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

func (r *Repository) Store() *storage.Store {
	return r.store
}

func (r *Repository) Configuration() storage.Configuration {
	return r.configuration
}

func (r *Repository) dataDecode(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.dataDecode", time.Since(t0))
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

func (r *Repository) dataEncode(buffer []byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("repository.dataEncode", time.Since(t0))
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
