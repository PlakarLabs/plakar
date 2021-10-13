package cache

import (
	"fmt"
	"io/ioutil"
	"os"
)

type Cache struct {
	directory string
}

type Chunk struct {
}

type Object struct {
}

type Snapshot struct {
}

func New(cacheDir string) *Cache {
	cache := &Cache{}
	cache.directory = cacheDir

	os.MkdirAll(fmt.Sprintf("%s/pathnames", cache.directory), 0700)
	os.MkdirAll(fmt.Sprintf("%s/snapshots", cache.directory), 0700)

	return cache
}

func (cache *Cache) PathPut(checksum string, data []byte) error {
	pathnamesDir := fmt.Sprintf("%s/pathnames/%s", cache.directory, checksum[0:2])
	os.Mkdir(pathnamesDir, 0700)

	f, err := os.Create(fmt.Sprintf("%s/%s", pathnamesDir, checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	f.Write(data)
	return nil
}

func (cache *Cache) PathGet(checksum string) ([]byte, error) {
	pathnameDir := fmt.Sprintf("%s/pathnames/%s", cache.directory, checksum[0:2])
	return ioutil.ReadFile(fmt.Sprintf("%s/%s", pathnameDir, checksum))
}

func (cache *Cache) SnapshotPut(checksum string, data []byte) error {
	snapshotDir := fmt.Sprintf("%s/snapshots/%s", cache.directory, checksum[0:2])
	os.Mkdir(snapshotDir, 0700)

	f, err := os.Create(fmt.Sprintf("%s/%s", snapshotDir, checksum))
	if err != nil {
		return err
	}
	defer f.Close()

	f.Write(data)
	return nil
}

func (cache *Cache) SnapshotGet(checksum string) ([]byte, error) {
	snapshotDir := fmt.Sprintf("%s/snapshots/%s", cache.directory, checksum[0:2])
	return ioutil.ReadFile(fmt.Sprintf("%s/%s", snapshotDir, checksum))
}
