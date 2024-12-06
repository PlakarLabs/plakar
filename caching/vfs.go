package caching

import (
	"fmt"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
)

type _VFSCache struct {
	manager *Manager
	db      *leveldb.DB
}

func newVFSCache(cacheManager *Manager, scheme string, origin string) (*_VFSCache, error) {
	cacheDir := filepath.Join(cacheManager.cacheDir, "vfs", scheme, origin)

	db, err := leveldb.OpenFile(cacheDir, nil)
	if err != nil {
		return nil, err
	}

	return &_VFSCache{
		manager: cacheManager,
		db:      db,
	}, nil
}

func (c *_VFSCache) Close() error {
	return c.db.Close()
}

func (c *_VFSCache) put(prefix string, pathname string, data []byte) error {
	return c.db.Put([]byte(fmt.Sprintf("%s:%s", prefix, pathname)), data, nil)
}

func (c *_VFSCache) get(prefix, pathname string) ([]byte, error) {
	data, err := c.db.Get([]byte(fmt.Sprintf("%s:%s", prefix, pathname)), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func (c *_VFSCache) PutDirectory(pathname string, data []byte) error {
	return c.put("__directory__", pathname, data)
}

func (c *_VFSCache) GetDirectory(pathname string) ([]byte, error) {
	return c.get("__directory__", pathname)
}

func (c *_VFSCache) PutFilename(pathname string, data []byte) error {
	return c.put("__filename__", pathname, data)
}

func (c *_VFSCache) GetFilename(pathname string) ([]byte, error) {
	return c.get("__filename__", pathname)
}

func (c *_VFSCache) PutFileSummary(pathname string, data []byte) error {
	return c.put("__file_summary__", pathname, data)
}

func (c *_VFSCache) GetFileSummary(pathname string) ([]byte, error) {
	return c.get("__file_summary__", pathname)
}

func (c *_VFSCache) PutObject(checksum [32]byte, data []byte) error {
	return c.put("__object__", fmt.Sprintf("%x", checksum), data)
}

func (c *_VFSCache) GetObject(checksum [32]byte) ([]byte, error) {
	return c.get("__object__", fmt.Sprintf("%x", checksum))
}
