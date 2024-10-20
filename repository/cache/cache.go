package cache

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

type Cache struct {
	cacheDir string
}

func New(cacheDir string) (*Cache, error) {
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, err
	}
	return &Cache{
		cacheDir: cacheDir,
	}, nil
}

func (c *Cache) Exists(stateID [32]byte) bool {
	_, err := os.Stat(filepath.Join(c.cacheDir, fmt.Sprintf("%x", stateID)))
	return err == nil
}

func (c *Cache) Put(stateID [32]byte, data []byte) error {
	f, err := os.Create(filepath.Join(c.cacheDir, fmt.Sprintf(".%x", stateID)))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return os.Rename(filepath.Join(c.cacheDir, fmt.Sprintf(".%x", stateID)), filepath.Join(c.cacheDir, fmt.Sprintf("%x", stateID)))
}

func (c *Cache) Get(stateID [32]byte) ([]byte, error) {
	return os.ReadFile(filepath.Join(c.cacheDir, fmt.Sprintf("%x", stateID)))
}

func (c *Cache) Delete(stateID [32]byte) error {
	return os.Remove(filepath.Join(c.cacheDir, fmt.Sprintf("%x", stateID)))
}

func (c *Cache) List() <-chan [32]byte {
	ch := make(chan [32]byte)
	go func() {
		defer close(ch)
		filepath.Walk(c.cacheDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || filepath.Ext(path) != "" {
				return nil
			}
			stateID, err := filepath.Rel(c.cacheDir, path)
			if err != nil {
				return err
			}

			var id [32]byte
			decoded, err := hex.DecodeString(stateID)
			if err != nil {
				return err
			}
			if len(decoded) != 32 {
				return nil
			}
			copy(id[:], decoded)
			ch <- id
			return nil
		})
	}()
	return ch
}

func (c *Cache) Close() error {
	return nil
}
