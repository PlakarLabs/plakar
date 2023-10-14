package cache

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"

	"github.com/syndtr/goleveldb/leveldb"
)

type Cache struct {
	db *leveldb.DB

	mu_metadatas sync.Mutex
	metadatas    map[string][]byte

	mu_blobs sync.Mutex
	blobs    map[string][]byte

	mu_pathnames sync.Mutex
	pathnames    map[string][]byte

	mu_objects sync.Mutex
	objects    map[string][]byte
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

	db, err := leveldb.OpenFile(fmt.Sprintf("%s/cache.db", cacheDir), nil)
	if err != nil {
		logger.Warn("could not open cache, bypassing: %s", err)
		return nil
	}

	cache := &Cache{}
	cache.db = db
	cache.metadatas = make(map[string][]byte)
	cache.blobs = make(map[string][]byte)
	cache.pathnames = make(map[string][]byte)
	cache.objects = make(map[string][]byte)

	return cache
}

func (cache *Cache) PutMetadata(RepositoryUuid string, Uuid string, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutMetadata", time.Since(t0))
	}()

	logger.Trace("cache", "%s: PutMetadata()", Uuid)
	key := fmt.Sprintf("Metadata:%s:%s", RepositoryUuid, Uuid)

	cache.mu_metadatas.Lock()
	cache.metadatas[key] = data
	cache.mu_metadatas.Unlock()

	cache.db.Put([]byte(key), data, nil)

	return nil
}

func (cache *Cache) PutBlob(RepositoryUuid string, checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutBlob", time.Since(t0))
	}()

	logger.Trace("cache", "%s: PutBlob(%016x)", RepositoryUuid, checksum)
	key := fmt.Sprintf("Blob:%s:%016x", RepositoryUuid, checksum)

	cache.mu_blobs.Lock()
	cache.blobs[key] = data
	cache.mu_blobs.Unlock()

	cache.db.Put([]byte(key), data, nil)

	return nil
}

func (cache *Cache) GetMetadata(RepositoryUuid string, Uuid string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetMetadata", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetMetadata()", Uuid)

	key := fmt.Sprintf("Metadata:%s:%s", RepositoryUuid, Uuid)
	cache.mu_metadatas.Lock()
	ret, exists := cache.metadatas[key]
	cache.mu_metadatas.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	data, err := cache.db.Get([]byte(key), nil)

	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) GetBlob(RepositoryUuid string, checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetIndex", time.Since(t0))
	}()
	logger.Trace("cache", "%016x: GetIndex()", checksum)
	key := fmt.Sprintf("Index:%s:%016x", RepositoryUuid, checksum)
	cache.mu_blobs.Lock()
	ret, exists := cache.blobs[key]
	cache.mu_blobs.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
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

	cache.mu_pathnames.Lock()
	cache.pathnames[key] = data
	cache.mu_pathnames.Unlock()

	cache.db.Put([]byte(key), data, nil)

	return nil
}

func (cache *Cache) GetPath(RepositoryUuid string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetPath", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetPath()", RepositoryUuid)

	key := fmt.Sprintf("Path:%s:%s", RepositoryUuid, checksum)
	cache.mu_pathnames.Lock()
	ret, exists := cache.pathnames[key]
	cache.mu_pathnames.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	data, err := cache.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutObject(RepositoryUuid string, checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutObject", time.Since(t0))
	}()
	logger.Trace("cache", "%s: PutObject()", RepositoryUuid)
	key := fmt.Sprintf("Object:%s:%s", RepositoryUuid, checksum)

	cache.mu_objects.Lock()
	cache.objects[key] = data
	cache.mu_objects.Unlock()

	cache.db.Put([]byte(key), data, nil)

	return nil
}

func (cache *Cache) GetObject(RepositoryUuid string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetObject", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetObject()", RepositoryUuid)

	key := fmt.Sprintf("Object:%s:%s", RepositoryUuid, checksum)
	cache.mu_objects.Lock()
	ret, exists := cache.objects[key]
	cache.mu_objects.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
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
