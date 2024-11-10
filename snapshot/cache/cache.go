package cache

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/vmihailenco/msgpack/v5"
)

type Cache struct {
	db *leveldb.DB
}

func New(cacheDir string) (*Cache, error) {
	db, err := leveldb.OpenFile(cacheDir, nil)
	if err != nil {
		return nil, err
	}

	return &Cache{
		db: db,
	}, nil
}

func (c *Cache) Close() error {
	return c.db.Close()
}

func (c *Cache) LookupFilename(origin string, filename string) (*vfs.FileEntry, [32]byte, uint64, error) {

	encodedOrigin := base64.StdEncoding.EncodeToString([]byte(origin))

	hasher := sha256.New()
	hasher.Write([]byte(filename))
	hashedFilename := hasher.Sum(nil)

	key := []byte(fmt.Sprintf("__filename__:%s:%x", encodedOrigin, hashedFilename))

	data, err := c.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, [32]byte{}, 0, nil
		}
		return nil, [32]byte{}, 0, err
	}

	var fileEntry vfs.FileEntry
	err = msgpack.Unmarshal(data, &fileEntry)
	if err != nil {
		return nil, [32]byte{}, 0, err
	}

	return &fileEntry, sha256.Sum256(data), uint64(len(data)), nil
}

// RecordFilename stores the FileInfo in LevelDB using the pathname as the key.
func (c *Cache) RecordFilename(origin string, filename string, fileEntry *vfs.FileEntry) error {

	encodedOrigin := base64.StdEncoding.EncodeToString([]byte(origin))

	hasher := sha256.New()
	hasher.Write([]byte(filename))
	hashedFilename := hasher.Sum(nil)

	key := []byte(fmt.Sprintf("__filename__:%s:%x", encodedOrigin, hashedFilename))

	// Serialize the FileInfo to JSON
	data, err := msgpack.Marshal(fileEntry)
	if err != nil {
		return err
	}

	// Store the serialized FileInfo under the hashed key in LevelDB
	err = c.db.Put(key, data, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) LookupDirectory(origin string, directory string) (*vfs.DirEntry, [32]byte, uint64, error) {

	encodedOrigin := base64.StdEncoding.EncodeToString([]byte(origin))

	hasher := sha256.New()
	hasher.Write([]byte(directory))
	hashedFilename := hasher.Sum(nil)

	key := []byte(fmt.Sprintf("__directory__:%s:%x", encodedOrigin, hashedFilename))

	data, err := c.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, [32]byte{}, 0, nil
		}
		return nil, [32]byte{}, 0, err
	}

	var dirEntry vfs.DirEntry
	err = msgpack.Unmarshal(data, &dirEntry)
	if err != nil {
		return nil, [32]byte{}, 0, err
	}

	return &dirEntry, sha256.Sum256(data), uint64(len(data)), nil
}

// RecordFilename stores the FileInfo in LevelDB using the pathname as the key.
func (c *Cache) RecordDirectory(origin string, filename string, dirEntry *vfs.DirEntry) error {

	encodedOrigin := base64.StdEncoding.EncodeToString([]byte(origin))

	hasher := sha256.New()
	hasher.Write([]byte(filename))
	hashedFilename := hasher.Sum(nil)

	key := []byte(fmt.Sprintf("__directory__:%s:%x", encodedOrigin, hashedFilename))

	// Serialize the FileInfo to JSON
	data, err := msgpack.Marshal(dirEntry)
	if err != nil {
		return err
	}

	// Store the serialized FileInfo under the hashed key in LevelDB
	err = c.db.Put(key, data, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) LookupObject(checksum [32]byte) (*objects.Object, error) {

	key := []byte(fmt.Sprintf("__object__:%x", checksum))

	data, err := c.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return objects.NewObjectFromBytes(data)
}

func (c *Cache) RecordObject(object *objects.Object) error {

	key := []byte(fmt.Sprintf("__object__:%x", object.Checksum))

	data, err := object.Serialize()
	if err != nil {
		return err
	}

	// Store the serialized FileInfo under the hashed key in LevelDB
	err = c.db.Put(key, data, nil)
	if err != nil {
		return err
	}

	return nil
}
