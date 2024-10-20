package snapshot

import (
	"fmt"
	"io"
	"math"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/snapshot/importer"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
	"github.com/gabriel-vasile/mimetype"
	"github.com/gobwas/glob"
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

func (cache *scanCache) GetPathname(pathname string) (importer.ScanRecord, error) {
	var record importer.ScanRecord

	key := fmt.Sprintf("__pathname__:%s", pathname)
	data, err := cache.db.Get([]byte(key), nil)
	if err != nil {
		return record, err
	}

	err = msgpack.Unmarshal(data, &record)
	if err != nil {
		return record, err
	}

	return record, nil
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

func (cache *scanCache) EnumerateImmediateChildPathnames(directory string) (<-chan importer.ScanRecord, error) {
	// Ensure directory ends with a trailing slash for consistency
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	// Create a channel to return the keys
	keyChan := make(chan importer.ScanRecord)

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

type PushOptions struct {
	MaxConcurrency uint64
	Excludes       []glob.Glob
}

func (snapshot *Snapshot) skipExcludedPathname(options *PushOptions, record importer.ScanResult) bool {
	var pathname string
	switch record := record.(type) {
	case importer.ScanError:
		pathname = record.Pathname
	case importer.ScanRecord:
		pathname = record.Pathname
	}
	doExclude := false
	for _, exclude := range options.Excludes {
		if exclude.Match(pathname) {
			doExclude = true
			break
		}
	}
	return doExclude
}

func (snap *Snapshot) updateImporterStatistics(record importer.ScanResult) {
	atomic.AddUint64(&snap.statistics.ImporterRecords, 1)

	switch record := record.(type) {
	case importer.ScanError:
		atomic.AddUint64(&snap.statistics.ImporterErrors, 1)

	case importer.ScanRecord:
		switch record.Type {
		case importer.RecordTypeFile:
			atomic.AddUint64(&snap.statistics.ImporterFiles, 1)
			if record.Stat.Nlink() > 1 {
				atomic.AddUint64(&snap.statistics.ImporterLinks, 1)
			}
			atomic.AddUint64(&snap.statistics.ImporterSize, uint64(record.Stat.Size()))
		case importer.RecordTypeDirectory:
			atomic.AddUint64(&snap.statistics.ImporterDirectories, 1)
		case importer.RecordTypeSymlink:
			atomic.AddUint64(&snap.statistics.ImporterSymlinks, 1)
		case importer.RecordTypeDevice:
			atomic.AddUint64(&snap.statistics.ImporterDevices, 1)
		case importer.RecordTypePipe:
			atomic.AddUint64(&snap.statistics.ImporterPipes, 1)
		case importer.RecordTypeSocket:
			atomic.AddUint64(&snap.statistics.ImporterSockets, 1)
		default:
			panic("unexpected record type")
		}
	}
}

func (snap *Snapshot) Backup(scanDir string, options *PushOptions) error {

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

	//t0 := time.Now()

	scanner, err := imp.Scan()
	if err != nil {
		return err
	}

	if !strings.Contains(scanDir, "://") {
		scanDir, err = filepath.Abs(scanDir)
		if err != nil {
			logger.Warn("%s", err)
			return err
		}
	} else {
		scanDir = imp.Root()
	}
	snap.Header.ScannedDirectories = append(snap.Header.ScannedDirectories, filepath.ToSlash(scanDir))

	/* importer */
	snap.statistics.ImporterStart = time.Now()
	for record := range scanner {
		if snap.skipExcludedPathname(options, record) {
			continue
		}
		snap.updateImporterStatistics(record)

		switch record := record.(type) {
		case importer.ScanError:
			logger.Warn("%s: %s", record.Pathname, record.Err)

		case importer.ScanRecord:
			if err := sc.RecordPathname(record); err != nil {
				return err
			}

			extension := strings.ToLower(filepath.Ext(record.Pathname))
			if extension == "" {
				extension = "none"
			}
			if _, exists := snap.Header.FileExtension[extension]; !exists {
				snap.Header.FileExtension[extension] = 0
			}
			snap.Header.FileExtension[extension]++
		}
	}
	snap.statistics.ImporterDuration = time.Since(snap.statistics.ImporterStart)

	/* scanner */
	snap.statistics.ScannerStart = time.Now()
	if filenames, err := sc.EnumerateKeysWithPrefixReverse("__pathname__", false); err != nil {
		return err
	} else {
		for record := range filenames {
			fileEntry := vfs.NewFileEntry(filepath.Dir(record.Pathname), &record)

			if record.Stat.Mode().IsRegular() {
				object, err := snap.chunkify(imp, record)
				if err != nil {
					atomic.AddUint64(&snap.statistics.ChunkerErrors, 1)
					return err
				}

				for _, chunk := range object.Chunks {
					fileEntry.AddChunk(chunk)
				}
				fileEntry.AddChecksum(object.Checksum)
				if object.ContentType != "" {
					fileEntry.AddContentType(object.ContentType)
				}

				data, err := msgpack.Marshal(object)
				if err != nil {
					return err
				}

				atomic.AddUint64(&snap.statistics.ObjectsCount, 1)
				atomic.AddUint64(&snap.statistics.ObjectsSize, uint64(len(data)))

				err = snap.PutObject(object.Checksum, data)
				if err != nil {
					return err
				}
				atomic.AddUint64(&snap.statistics.ScannerProcessedSize, uint64(record.Stat.Size()))

				// XXX
				snap.Metadata.AddMetadata(object.ContentType, object.Checksum)
			}

			serialized, err := fileEntry.Serialize()
			if err != nil {
				return err
			}

			checksum := snap.repository.Checksum(serialized)
			err = sc.RecordChecksum(record.Pathname, checksum)
			if err != nil {
				return err
			}

			atomic.AddUint64(&snap.statistics.VFSFilesCount, 1)
			atomic.AddUint64(&snap.statistics.VFSFilesSize, uint64(len(serialized)))

			err = snap.PutFile(checksum, serialized)
			if err != nil {
				return err
			}
		}
	}

	directories, err := sc.EnumerateKeysWithPrefixReverse("__pathname__", true)
	if err != nil {
		return err
	}
	for record := range directories {
		if c, err := sc.EnumerateImmediateChildPathnames(record.Pathname); err != nil {
			return err
		} else {
			dirEntry := vfs.NewDirectoryEntry(filepath.Dir(record.Pathname), &record)

			for childrecord := range c {
				value, err := sc.GetChecksum(childrecord.Pathname)
				if err != nil {
					return err
				}
				dirEntry.AddChild(value, childrecord)
			}

			serialized, err := dirEntry.Serialize()
			if err != nil {
				return err
			}

			checksum := snap.repository.Checksum(serialized)
			err = sc.RecordChecksum(record.Pathname, checksum)
			if err != nil {
				return err
			}

			atomic.AddUint64(&snap.statistics.VFSDirectoriesCount, 1)
			atomic.AddUint64(&snap.statistics.VFSDirectoriesSize, uint64(len(serialized)))

			err = snap.PutDirectory(checksum, serialized)
			if err != nil {
				return err
			}
		}
	}
	snap.statistics.ScannerDuration = time.Since(snap.statistics.ScannerStart)

	// preparing commit
	metadata, err := snap.Metadata.Serialize()
	if err != nil {
		return err
	}
	metadataChecksum := snap.repository.Checksum(metadata)
	err = snap.PutData(metadataChecksum, metadata)
	if err != nil {
		return err
	}

	statistics, err := snap.statistics.Serialize()
	if err != nil {
		return err
	}
	statisticsChecksum := snap.repository.Checksum(statistics)
	err = snap.PutData(statisticsChecksum, statistics)
	if err != nil {
		return err
	}

	value, err := sc.GetChecksum("/")
	if err != nil {
		return err
	}

	snap.Header.Root = value
	snap.Header.Metadata = metadataChecksum
	snap.Header.Statistics = statisticsChecksum
	snap.Header.CreationDuration = time.Since(snap.statistics.ImporterStart)
	snap.Header.DirectoriesCount = snap.statistics.ImporterDirectories
	snap.Header.FilesCount = snap.statistics.ImporterFiles
	snap.Header.ScanSize = snap.statistics.ImporterSize
	snap.Header.ScanProcessedSize = snap.statistics.ScannerProcessedSize

	//
	for _, key := range snap.Metadata.ListKeys() {
		objectType := strings.Split(key, ";")[0]
		objectKind := strings.Split(key, "/")[0]
		if objectType == "" {
			objectType = "unknown"
			objectKind = "unknown"
		}
		if _, exists := snap.Header.FileKind[objectKind]; !exists {
			snap.Header.FileKind[objectKind] = 0
		}
		snap.Header.FileKind[objectKind] += uint64(len(snap.Metadata.ListValues(key)))

		if _, exists := snap.Header.FileType[objectType]; !exists {
			snap.Header.FileType[objectType] = 0
		}
		snap.Header.FileType[objectType] += uint64(len(snap.Metadata.ListValues(key)))
	}

	for key, value := range snap.Header.FileType {
		snap.Header.FilePercentType[key] = math.Round((float64(value)/float64(snap.Header.FilesCount)*100)*100) / 100
	}
	for key, value := range snap.Header.FileKind {
		snap.Header.FilePercentKind[key] = math.Round((float64(value)/float64(snap.Header.FilesCount)*100)*100) / 100
	}
	for key, value := range snap.Header.FileExtension {
		snap.Header.FilePercentExtension[key] = math.Round((float64(value)/float64(snap.Header.FilesCount)*100)*100) / 100
	}
	return snap.Commit()
}

func (snap *Snapshot) chunkify(imp *importer.Importer, record importer.ScanRecord) (*objects.Object, error) {
	atomic.AddUint64(&snap.statistics.ChunkerFiles, 1)

	rd, err := imp.NewReader(record.Pathname)
	if err != nil {
		return nil, err
	}
	defer rd.Close()

	object := objects.NewObject()
	object.ContentType = mime.TypeByExtension(filepath.Ext(record.Pathname))

	objectHasher := snap.repository.Hasher()
	chunkHasher := snap.repository.Hasher()

	var firstChunk = true
	var cdcOffset uint64
	var t32 [32]byte

	// Helper function to process a chunk
	processChunk := func(data []byte) error {
		atomic.AddUint64(&snap.statistics.ChunkerChunks, 1)
		if firstChunk {
			if object.ContentType == "" {
				object.ContentType = mimetype.Detect(data).String()
			}
			firstChunk = false
		}
		objectHasher.Write(data)

		chunkHasher.Reset()
		chunkHasher.Write(data)
		copy(t32[:], chunkHasher.Sum(nil))

		chunk := objects.Chunk{Checksum: t32, Length: uint32(len(data))}
		object.Chunks = append(object.Chunks, chunk)
		cdcOffset += uint64(len(data))

		if !snap.CheckChunk(chunk.Checksum) {
			atomic.AddUint64(&snap.statistics.ChunksCount, 1)
			atomic.AddUint64(&snap.statistics.ChunksSize, uint64(len(data)))
			return snap.PutChunk(chunk.Checksum, data)
		}
		return nil
	}

	// Small file case: read entire file into memory
	if record.Stat.Size() < int64(snap.repository.Configuration().ChunkingMin) {
		buf, err := io.ReadAll(rd)
		if err != nil {
			return nil, err
		}
		if err := processChunk(buf); err != nil {
			return nil, err
		}
	} else {
		// Large file case: chunk file with chunker
		chk, err := snap.repository.Chunker(rd)
		if err != nil {
			return nil, err
		}
		for {
			cdcChunk, err := chk.Next()
			if err != nil && err != io.EOF {
				return nil, err
			}
			if cdcChunk == nil {
				break
			}
			if err := processChunk(cdcChunk); err != nil {
				return nil, err
			}
			if err == io.EOF {
				break
			}
		}
	}
	atomic.AddUint64(&snap.statistics.ChunkerObjects, 1)
	atomic.AddUint64(&snap.statistics.ChunkerSize, uint64(record.Stat.Size()))

	copy(t32[:], objectHasher.Sum(nil))
	object.Checksum = t32
	return object, nil
}
