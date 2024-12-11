package caching

import (
	"encoding/hex"
	"fmt"
	"iter"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
)

type _RepositoryCache struct {
	manager *Manager
	db      *leveldb.DB
}

func newRepositoryCache(cacheManager *Manager, repositoryID uuid.UUID) (*_RepositoryCache, error) {
	cacheDir := filepath.Join(cacheManager.cacheDir, "repository", repositoryID.String())

	db, err := leveldb.OpenFile(cacheDir, nil)
	if err != nil {
		return nil, err
	}

	return &_RepositoryCache{
		manager: cacheManager,
		db:      db,
	}, nil
}

func (c *_RepositoryCache) Close() error {
	return c.db.Close()
}

func (c *_RepositoryCache) put(prefix string, key string, data []byte) error {
	return c.db.Put([]byte(fmt.Sprintf("%s:%s", prefix, key)), data, nil)
}

func (c *_RepositoryCache) has(prefix, key string) (bool, error) {
	return c.db.Has([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
}

func (c *_RepositoryCache) get(prefix, key string) ([]byte, error) {
	data, err := c.db.Get([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func (c *_RepositoryCache) delete(prefix, key string) error {
	return c.db.Delete([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
}

func (c *_RepositoryCache) PutState(stateID [32]byte, data []byte) error {
	return c.put("__state__", fmt.Sprintf("%x", stateID), data)
}

func (c *_RepositoryCache) HasState(stateID [32]byte) (bool, error) {
	return c.has("__state__", fmt.Sprintf("%x", stateID))
}

func (c *_RepositoryCache) GetState(stateID [32]byte) ([]byte, error) {
	return c.get("__state__", fmt.Sprintf("%x", stateID))
}

func (c *_RepositoryCache) DelState(stateID [32]byte) error {
	return c.delete("__state__", fmt.Sprintf("%x", stateID))
}

func (c *_RepositoryCache) ListStates() iter.Seq2[[32]byte, error] {
	return func(yield func([32]byte, error) bool) {
		iter := c.db.NewIterator(nil, nil)
		defer iter.Release()

		keyPrefix := "__state__:"
		for iter.Seek([]byte(keyPrefix)); iter.Valid(); iter.Next() {
			if !strings.HasPrefix(string(iter.Key()), keyPrefix) {
				break
			}

			var stateID [32]byte
			_, err := hex.Decode(stateID[:], iter.Key()[len(keyPrefix):])
			if !yield(stateID, err) {
				return
			}
		}
	}
}
