package caching

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
)

type _ScanCache struct {
	snapshotID [32]byte
	manager    *Manager
	db         *leveldb.DB
}

func newScanCache(cacheManager *Manager, snapshotID [32]byte) (*_ScanCache, error) {
	cacheDir := filepath.Join(cacheManager.cacheDir, "scan", fmt.Sprintf("%x", snapshotID))

	db, err := leveldb.OpenFile(cacheDir, nil)
	if err != nil {
		return nil, err
	}

	return &_ScanCache{
		snapshotID: snapshotID,
		manager:    cacheManager,
		db:         db,
	}, nil
}

func (c *_ScanCache) Close() error {
	c.db.Close()
	return os.RemoveAll(filepath.Join(c.manager.cacheDir, "scan", fmt.Sprintf("%x", c.snapshotID)))
}

func (c *_ScanCache) put(prefix string, key string, data []byte) error {
	return c.db.Put([]byte(fmt.Sprintf("%s:%s", prefix, key)), data, nil)
}

func (c *_ScanCache) get(prefix, key string) ([]byte, error) {
	data, err := c.db.Get([]byte(fmt.Sprintf("%s:%s", prefix, key)), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

/// BELOW IS THE OLD CODE FROM BACKUP LAYER, NEEDS TO BE CLEANED UP

/*

func (c *_ScanCache) PutError(pathname string, data []byte) error {
	return c.put("__error__", pathname, data)
}

func (c *_ScanCache) PutPathname(record importer.ScanRecord) error {
	buffer, err := msgpack.Marshal(&record)
	if err != nil {
		return err
	}

	var key string
	if record.FileInfo.Mode().IsDir() {
		if record.Pathname == "/" {
			key = "__pathname__:/"
		} else {
			key = fmt.Sprintf("__pathname__:%s/", record.Pathname)
		}
	} else {
		key = fmt.Sprintf("__pathname__:%s", record.Pathname)
	}

	return c.db.Put([]byte(key), buffer, nil)
}

func (c *_ScanCache) GetPathname(pathname string) (importer.ScanRecord, error) {
	var record importer.ScanRecord

	key := fmt.Sprintf("__pathname__:%s", pathname)
	data, err := c.db.Get([]byte(key), nil)
	if err != nil {
		return record, err
	}

	err = msgpack.Unmarshal(data, &record)
	if err != nil {
		return record, err
	}

	return record, nil
}

func (c *_ScanCache) PutChecksum(pathname string, checksum objects.Checksum) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}
	return c.db.Put([]byte(fmt.Sprintf("__checksum__:%s", pathname)), checksum[:], nil)
}

func (c *_ScanCache) GetChecksum(pathname string) (objects.Checksum, error) {
	data, err := c.db.Get([]byte(fmt.Sprintf("__checksum__:%s", pathname)), nil)
	if err != nil {
		return objects.Checksum{}, err
	}

	if len(data) != 32 {
		return objects.Checksum{}, fmt.Errorf("invalid checksum length: %d", len(data))
	}

	ret := objects.Checksum{}
	copy(ret[:], data)
	return ret, nil
}

func (c *_ScanCache) EnumerateErrorsWithinDirectory(directory string, reverse bool) (<-chan ErrorEntry, error) {
	// Ensure directory ends with a trailing slash for consistency
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	// Create a channel to return the keys
	keyChan := make(chan ErrorEntry)

	// Start a goroutine to perform the iteration
	go func() {
		defer close(keyChan) // Ensure the channel is closed when the function exits

		iter := c.db.NewIterator(nil, nil)
		defer iter.Release()

		// Create the directory prefix to match keys
		directoryKeyPrefix := "__error__:" + directory

		if reverse {
			// Reverse iteration: manually position to the last key within the prefix range
			iter.Seek([]byte(directoryKeyPrefix)) // Start at the prefix
			if iter.Valid() && strings.HasPrefix(string(iter.Key()), directoryKeyPrefix) {
				// Move to the last key in the range
				for iter.Next() && strings.HasPrefix(string(iter.Key()), directoryKeyPrefix) {
				}
				iter.Prev() // Step back to the last valid key
			}
		} else {
			// Forward iteration: start at the beginning of the range
			iter.Seek([]byte(directoryKeyPrefix))
		}

		for iter.Valid() {
			key := string(iter.Key())
			if key == directoryKeyPrefix {
				// Skip the directory key itself
				if reverse {
					iter.Prev()
				} else {
					iter.Next()
				}
				continue
			}

			// Check if the key starts with the directory prefix
			if strings.HasPrefix(key, directoryKeyPrefix) {
				// Remove the prefix and the directory to isolate the remaining part of the path
				remainingPath := key[len(directoryKeyPrefix):]

				// Determine if this is an immediate child
				slashCount := strings.Count(remainingPath, "/")

				// Immediate child should either:
				// - Have no slash (a file)
				// - Have exactly one slash at the end (a directory)
				if slashCount == 0 || (slashCount == 1 && strings.HasSuffix(remainingPath, "/")) {
					// Retrieve the value for the current key
					path := strings.TrimPrefix(key, "__error__:")
					value := iter.Value()
					keyChan <- ErrorEntry{Pathname: path, Error: string(value)}
				}
			} else {
				// Stop if the key is no longer within the expected prefix
				break
			}

			// Advance or reverse the iterator
			if reverse {
				iter.Prev()
			} else {
				iter.Next()
			}
		}
	}()

	// Return the channel for the caller to consume
	return keyChan, nil
}


func (c *_ScanCache) RecordStatistics(pathname string, statistics *vfs.Summary) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	buffer, err := msgpack.Marshal(statistics)
	if err != nil {
		return err
	}
	return c.db.Put([]byte(fmt.Sprintf("__statistics__:%s", pathname)), buffer, nil)
}

func (c *_ScanCache) GetStatistics(pathname string) (*vfs.Summary, error) {
	data, err := c.db.Get([]byte(fmt.Sprintf("__statistics__:%s", pathname)), nil)
	if err != nil {
		return nil, err
	}

	var stats vfs.Summary
	err = msgpack.Unmarshal(data, &stats)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

func (c *_ScanCache) EnumerateKeysWithPrefixReverse(prefix string, isDirectory bool) (<-chan importer.ScanRecord, error) {
	// Create a channel to return the keys
	keyChan := make(chan importer.ScanRecord)

	// Start a goroutine to perform the iteration
	go func() {
		defer close(keyChan) // Ensure the channel is closed when the function exits

		// Use LevelDB's iterator
		iter := c.db.NewIterator(nil, nil)
		defer iter.Release()

		// Move to the last key and iterate backward
		for iter.Last(); iter.Valid(); iter.Prev() {
			key := iter.Key()

			// Check if the key starts with the given prefix
			if !strings.HasPrefix(string(key), prefix) {
				continue
			}

			if isDirectory {
				if !strings.HasSuffix(string(key), "/") {
					continue
				}
			} else {
				if strings.HasSuffix(string(key), "/") {
					continue
				}
			}

			// Retrieve the value for the current key
			value := iter.Value()

			var record importer.ScanRecord
			err := msgpack.Unmarshal(value, &record)
			if err != nil {
				fmt.Printf("Error unmarshaling value: %v\n", err)
				continue
			}

			// Send the record through the channel
			keyChan <- record
		}
	}()

	// Return the channel for the caller to consume
	return keyChan, nil
}

func (c *_ScanCache) EnumerateImmediateChildPathnames2(directory string, reverse bool) (<-chan importer.ScanRecord, error) {
	// Ensure directory ends with a trailing slash for consistency
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	// Create a channel to return the keys
	keyChan := make(chan importer.ScanRecord)

	// Start a goroutine to perform the iteration
	go func() {
		defer close(keyChan) // Ensure the channel is closed when the function exits

		iter := c.db.NewIterator(nil, nil)
		defer iter.Release()

		// Create the directory prefix to match keys
		directoryKeyPrefix := "__pathname__:" + directory

		// Iterate over keys in LevelDB
		for iter.Seek([]byte(directoryKeyPrefix)); iter.Valid(); iter.Next() {
			key := string(iter.Key())
			if key == directoryKeyPrefix {
				continue
			}

			// Check if the key starts with the directory prefix
			if strings.HasPrefix(key, directoryKeyPrefix) {
				// Remove the prefix and the directory to isolate the remaining part of the path
				remainingPath := key[len(directoryKeyPrefix):]

				// Determine if this is an immediate child
				slashCount := strings.Count(remainingPath, "/")

				// Immediate child should either:
				// - Have no slash (a file)
				// - Have exactly one slash at the end (a directory)
				if slashCount == 0 || (slashCount == 1 && strings.HasSuffix(remainingPath, "/")) {
					// Retrieve the value for the current key
					value := iter.Value()

					var record importer.ScanRecord
					err := msgpack.Unmarshal(value, &record)
					if err != nil {
						fmt.Printf("Error unmarshaling value: %v\n", err)
						continue
					}

					// Send the immediate child key through the channel
					keyChan <- record
				}
			} else {
				// Stop if the key is no longer within the expected prefix
				break
			}
		}
	}()

	// Return the channel for the caller to consume
	return keyChan, nil
}

func (c *_ScanCache) EnumerateImmediateChildPathnames(directory string, reverse bool) (<-chan importer.ScanRecord, error) {
	// Ensure directory ends with a trailing slash for consistency
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	// Create a channel to return the keys
	keyChan := make(chan importer.ScanRecord)

	// Start a goroutine to perform the iteration
	go func() {
		defer close(keyChan) // Ensure the channel is closed when the function exits

		iter := c.db.NewIterator(nil, nil)
		defer iter.Release()

		// Create the directory prefix to match keys
		directoryKeyPrefix := "__pathname__:" + directory

		if reverse {
			// Reverse iteration: manually position to the last key within the prefix range
			iter.Seek([]byte(directoryKeyPrefix)) // Start at the prefix
			if iter.Valid() && strings.HasPrefix(string(iter.Key()), directoryKeyPrefix) {
				// Move to the last key in the range
				for iter.Next() && strings.HasPrefix(string(iter.Key()), directoryKeyPrefix) {
				}
				iter.Prev() // Step back to the last valid key
			}
		} else {
			// Forward iteration: start at the beginning of the range
			iter.Seek([]byte(directoryKeyPrefix))
		}

		for iter.Valid() {
			key := string(iter.Key())
			if key == directoryKeyPrefix {
				// Skip the directory key itself
				if reverse {
					iter.Prev()
				} else {
					iter.Next()
				}
				continue
			}

			// Check if the key starts with the directory prefix
			if strings.HasPrefix(key, directoryKeyPrefix) {
				// Remove the prefix and the directory to isolate the remaining part of the path
				remainingPath := key[len(directoryKeyPrefix):]

				// Determine if this is an immediate child
				slashCount := strings.Count(remainingPath, "/")

				// Immediate child should either:
				// - Have no slash (a file)
				// - Have exactly one slash at the end (a directory)
				if slashCount == 0 || (slashCount == 1 && strings.HasSuffix(remainingPath, "/")) {
					// Retrieve the value for the current key
					value := iter.Value()

					var record importer.ScanRecord
					err := msgpack.Unmarshal(value, &record)
					if err != nil {
						fmt.Printf("Error unmarshaling value: %v\n", err)
						if reverse {
							iter.Prev()
						} else {
							iter.Next()
						}
						continue
					}

					// Send the immediate child key through the channel
					keyChan <- record
				}
			} else {
				// Stop if the key is no longer within the expected prefix
				break
			}

			// Advance or reverse the iterator
			if reverse {
				iter.Prev()
			} else {
				iter.Next()
			}
		}
	}()

	// Return the channel for the caller to consume
	return keyChan, nil
}
*/
