package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"

	"github.com/syndtr/goleveldb/leveldb"
)

type Cache struct {
	db *leveldb.DB
}

func Create(localdir string) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.Create", time.Since(t0))
	}()
	return os.MkdirAll(localdir, 0700)
}

func New(cacheDir string) *Cache {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.New", time.Since(t0))
	}()

	db, err := leveldb.OpenFile(filepath.Join(cacheDir, "cache.db"), nil)
	if err != nil {
		logger.Warn("could not open cache, bypassing: %s", err)
		return nil
	}
	return &Cache{
		db: db,
	}
}

func (cache *Cache) PutSnapshot(RepositoryUuid string, Uuid string, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutSnapshot", time.Since(t0))
	}()
	logger.Trace("cache", "%s: PutSnapshot()", Uuid)

	key := fmt.Sprintf("Snapshot:%s:%s", RepositoryUuid, Uuid)
	return cache.db.Put([]byte(key), data, nil)
}

func (cache *Cache) GetSnapshot(RepositoryUuid string, Uuid string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetSnapshot", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetSnapshot()", Uuid)

	var data []byte
	key := fmt.Sprintf("Snapshot:%s:%s", RepositoryUuid, Uuid)
	data, err := cache.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutBlob(RepositoryUuid string, checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutBlob", time.Since(t0))
	}()

	logger.Trace("cache", "%s: PutBlob(%016x)", RepositoryUuid, checksum)

	key := fmt.Sprintf("Blob:%s:%016x", RepositoryUuid, checksum)
	return cache.db.Put([]byte(key), data, nil)
}

func (cache *Cache) GetBlob(RepositoryUuid string, checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetBlob", time.Since(t0))
	}()
	logger.Trace("cache", "%016x: GetBlob()", checksum)

	var data []byte
	key := fmt.Sprintf("Index:%s:%016x", RepositoryUuid, checksum)
	data, err := cache.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutPath(RepositoryUuid string, checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutPath", time.Since(t0))
	}()
	logger.Trace("cache", "%s: PutPath()", RepositoryUuid)

	key := fmt.Sprintf("Path:%s:%s", RepositoryUuid, checksum)
	return cache.db.Put([]byte(key), data, nil)
}

func (cache *Cache) GetPath(RepositoryUuid string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetPath", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetPath()", RepositoryUuid)

	var data []byte
	key := fmt.Sprintf("Path:%s:%s", RepositoryUuid, checksum)
	data, err := cache.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) Commit() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.Commit", time.Since(t0))
	}()

	logger.Trace("cache", "Commit()")

	cache.db.Close()

	return nil
}
