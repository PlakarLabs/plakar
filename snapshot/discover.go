package snapshot

import (
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	chunkers "github.com/PlakarLabs/go-cdc-chunkers"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/ultracdc"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/snapshot/importer"
	"github.com/gabriel-vasile/mimetype"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/vmihailenco/msgpack/v5"
)

type NoOpLogger struct{}

func (l *NoOpLogger) Errorf(format string, v ...interface{})   {}
func (l *NoOpLogger) Warningf(format string, v ...interface{}) {}
func (l *NoOpLogger) Infof(format string, v ...interface{})    {}
func (l *NoOpLogger) Debugf(format string, v ...interface{})   {}
func (l *NoOpLogger) Fatalf(format string, v ...interface{})   {}

type scanCache struct {
	db    *leveldb.DB
	dbdir string
}

func newScanCache() (*scanCache, error) {
	tempDir, err := os.MkdirTemp("", "leveldb-temp")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Open an in-memory LevelDB database (replace "/tmp/test" with "" for in-memory)
	db, err := leveldb.OpenFile(tempDir, nil)
	if err != nil {
		return nil, err
	}

	return &scanCache{
		db:    db,
		dbdir: tempDir,
	}, nil
}

func (cache *scanCache) Close() error {
	// Close the LevelDB database
	if err := cache.db.Close(); err != nil {
		return err
	}

	// Remove the temporary directory and its contents
	if err := os.RemoveAll(cache.dbdir); err != nil {
		return fmt.Errorf("failed to remove temp directory: %w", err)
	}

	return nil
}

func (cache *scanCache) RecordPathname(record importer.ScanRecord) error {
	buffer, err := msgpack.Marshal(&record)
	if err != nil {
		return err
	}

	var key string
	if record.Stat.Mode().IsDir() {
		if record.Pathname == "/" {
			key = "__pathname__:/"
		} else {
			key = fmt.Sprintf("__pathname__:%s/", record.Pathname)
		}
	} else {
		key = fmt.Sprintf("__pathname__:%s", record.Pathname)
	}

	// Use LevelDB's Put method to store the key-value pair
	return cache.db.Put([]byte(key), buffer, nil)
}

func (cache *scanCache) RecordChecksum(pathname string, checksum [32]byte) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}
	return cache.db.Put([]byte(fmt.Sprintf("__checksum__:%s", pathname)), checksum[:], nil)
}

func (cache *scanCache) GetChecksum(pathname string) ([32]byte, error) {
	data, err := cache.db.Get([]byte(fmt.Sprintf("__checksum__:%s", pathname)), nil)
	if err != nil {
		return [32]byte{}, err
	}

	if len(data) != 32 {
		return [32]byte{}, fmt.Errorf("invalid checksum length: %d", len(data))
	}

	ret := [32]byte{}
	copy(ret[:], data)
	return ret, nil
}

func (cache *scanCache) EnumerateKeysWithPrefixReverse(prefix string, isDirectory bool) (<-chan importer.ScanRecord, error) {
	// Create a channel to return the keys
	keyChan := make(chan importer.ScanRecord)

	// Start a goroutine to perform the iteration
	go func() {
		defer close(keyChan) // Ensure the channel is closed when the function exits

		// Use LevelDB's iterator
		iter := cache.db.NewIterator(nil, nil)
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

func (cache *scanCache) EnumerateImmediateChildPathnames(directory string) (<-chan string, error) {
	// Ensure directory ends with a trailing slash for consistency
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	// Create a channel to return the keys
	keyChan := make(chan string)

	// Start a goroutine to perform the iteration
	go func() {
		defer close(keyChan) // Ensure the channel is closed when the function exits

		iter := cache.db.NewIterator(nil, nil)
		defer iter.Release()

		// Create the directory prefix to match keys
		directoryKeyPrefix := "__pathname__:" + directory

		// Iterate over keys in LevelDB
		for iter.Seek([]byte(directoryKeyPrefix)); iter.Valid(); iter.Next() {
			key := string(iter.Key())

			// Check if the key starts with the directory prefix
			if strings.HasPrefix(key, directoryKeyPrefix) {
				// Remove the prefix and the directory to isolate the remaining part of the path
				remainingPath := key[len(directoryKeyPrefix):]

				// Skip if remaining path is empty or has multiple levels (i.e., deeper paths)
				if remainingPath == "" || strings.Count(remainingPath, "/") > 1 {
					continue
				}

				// Send the remaining path (immediate child key) through the channel
				keyChan <- remainingPath
			} else {
				// Stop if the key is no longer within the expected prefix
				break
			}
		}
	}()

	// Return the channel for the caller to consume
	return keyChan, nil
}

func (snapshot *Snapshot) Discover(scanDir string, options *PushOptions) error {
	sc, err := newScanCache()
	if err != nil {
		return err
	}
	defer sc.Close()

	imp, err := importer.NewImporter(scanDir)
	if err != nil {
		return err
	}
	defer imp.Close()

	t0 := time.Now()

	scanner, err := imp.Scan()
	if err != nil {
		return err
	}
	snapshot.Header.ScannedDirectories = make([]string, 0)

	if !strings.Contains(scanDir, "://") {
		scanDir, err = filepath.Abs(scanDir)
		if err != nil {
			logger.Warn("%s", err)
			return err
		}
	} else {
		scanDir = imp.Root()
	}
	snapshot.Header.ScannedDirectories = append(snapshot.Header.ScannedDirectories, filepath.ToSlash(scanDir))

	filesCount := 0
	directoriesCount := 0
	symlinksCount := 0
	nregularsCount := 0
	errors := 0

	t0 = time.Now()
	for record := range scanner {
		if snapshot.skipExcludedPathname(options, record) {
			continue
		}

		switch record := record.(type) {
		case importer.ScanError:
			//logger.Warn("%s: %s", record.Pathname, record.Err)
			errors++

		case importer.ScanRecord:
			switch record.Type {
			case importer.RecordTypeFile:
				filesCount++
			case importer.RecordTypeDirectory:
				directoriesCount++
			case importer.RecordTypeSymlink:
				symlinksCount++
			default:
				nregularsCount++
			}
			if err := sc.RecordPathname(record); err != nil {
				return err
			}
		}
	}

	filenames, err := sc.EnumerateKeysWithPrefixReverse("__pathname__", false)
	if err != nil {
		return err
	}
	for record := range filenames {
		data, err := msgpack.Marshal(record)
		if err != nil {
			return err
		}

		var checksum [32]byte
		copy(checksum[:], snapshot.repository.Checksum(data))
		err = sc.RecordChecksum(record.Pathname, checksum)
		if err != nil {
			return err
		}
		//fmt.Println("PutFile", record.Pathname, "checksum", object.Checksum)
	}

	directories, err := sc.EnumerateKeysWithPrefixReverse("__pathname__", true)
	if err != nil {
		return err
	}
	for record := range directories {
		c, err := sc.EnumerateImmediateChildPathnames(record.Pathname)
		if err != nil {
			return err
		}
		for childrecord := range c {
			childrecord := filepath.Join(record.Pathname, childrecord)

			value, err := sc.GetChecksum(childrecord)
			if err != nil {
				return err
			}
			_ = value
		}
		data, err := msgpack.Marshal(record)
		if err != nil {
			return err
		}

		var checksum [32]byte
		copy(checksum[:], snapshot.repository.Checksum(data))
		err = sc.RecordChecksum(record.Pathname, checksum)
		if err != nil {
			return err
		}

	}

	value, err := sc.GetChecksum("/")
	if err != nil {
		return err
	}

	fmt.Println(fmt.Sprintf("%x", value), "took", time.Since(t0), "files:", filesCount, "directories:", directoriesCount, "symlinks:", symlinksCount, "nregulars:", nregularsCount, "errors:", errors)

	return nil
}

/**/
func chunkify2(snapshot *Snapshot, imp *importer.Importer, pathname string, fi objects.FileInfo) (*objects.Object, error) {
	rd, err := imp.NewReader(filepath.FromSlash(pathname))
	if err != nil {
		return nil, err
	}
	defer rd.Close()

	object := &objects.Object{}
	object.ContentType = mime.TypeByExtension(filepath.Ext(pathname))
	objectHasher := snapshot.repository.Hasher()

	if fi.Size() < int64(snapshot.repository.Configuration().ChunkingMin) {
		var t32 [32]byte

		buf, err := io.ReadAll(rd)
		if err != nil {
			return nil, err
		}

		if object.ContentType == "" {
			object.ContentType = mimetype.Detect(buf).String()
		}

		objectHasher.Write(buf)
		copy(t32[:], objectHasher.Sum(nil))
		object.Checksum = t32

		chunk := objects.Chunk{}
		chunk.Checksum = t32
		chunk.Length = uint32(len(buf))
		object.Chunks = append(object.Chunks, chunk.Checksum)

		indexChunk, err := snapshot.Index.LookupChunk(chunk.Checksum)
		if err != nil && err != leveldb.ErrNotFound {
			return nil, err
		}
		if indexChunk == nil {
			exists := snapshot.CheckChunk(chunk.Checksum)
			if !exists {
				err := snapshot.PutChunk(chunk.Checksum, buf)
				if err != nil {
					return nil, err
				}
			}
			snapshot.Index.AddChunk(&chunk)
		}

		return object, nil
	}

	chunkingAlgorithm := snapshot.repository.Configuration().Chunking
	chunkingMinSize := snapshot.repository.Configuration().ChunkingMin
	chunkingNormalSize := snapshot.repository.Configuration().ChunkingNormal
	chunkingMaxSize := snapshot.repository.Configuration().ChunkingMax

	chk, err := chunkers.NewChunker(chunkingAlgorithm, rd, &chunkers.ChunkerOpts{
		MinSize:    chunkingMinSize,
		NormalSize: chunkingNormalSize,
		MaxSize:    chunkingMaxSize,
	})
	if err != nil {
		return nil, err
	}

	chunkHasher := snapshot.repository.Hasher()

	firstChunk := true
	cdcOffset := uint64(0)
	for {
		cdcChunk, err := chk.Next()
		if err != nil && err != io.EOF {
			return nil, err
		}

		if cdcChunk != nil {
			if firstChunk {
				if object.ContentType == "" {
					object.ContentType = mimetype.Detect(cdcChunk).String()
				}
				firstChunk = false
			}

			objectHasher.Write(cdcChunk)

			if !firstChunk {
				chunkHasher.Reset()
			}
			chunkHasher.Write(cdcChunk)

			var t32 [32]byte
			copy(t32[:], chunkHasher.Sum(nil))

			chunk := objects.Chunk{}
			chunk.Checksum = t32
			chunk.Length = uint32(len(cdcChunk))
			object.Chunks = append(object.Chunks, chunk.Checksum)
			cdcOffset += uint64(len(cdcChunk))

			indexChunk, err := snapshot.Index.LookupChunk(chunk.Checksum)
			if err != nil && err != leveldb.ErrNotFound {
				return nil, err
			}
			if indexChunk == nil {
				exists := snapshot.CheckChunk(chunk.Checksum)
				if !exists {
					err := snapshot.PutChunk(chunk.Checksum, cdcChunk)
					if err != nil {
						return nil, err
					}
				}
				snapshot.Index.AddChunk(&chunk)
			}
		}

		if err == io.EOF {
			break
		}

	}
	var t32 [32]byte
	copy(t32[:], objectHasher.Sum(nil))
	object.Checksum = t32

	return object, nil
}
