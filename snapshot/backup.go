package snapshot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/cache"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
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

func (cache *scanCache) RecordAggregates(pathname string, files uint64, dirs uint64, size uint64) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 24))
	binary.Write(buffer, binary.LittleEndian, files)
	binary.Write(buffer, binary.LittleEndian, dirs)
	binary.Write(buffer, binary.LittleEndian, size)
	return cache.db.Put([]byte(fmt.Sprintf("__aggregate__:%s", pathname)), buffer.Bytes(), nil)
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

func (cache *scanCache) GetAggregate(pathname string) (uint64, uint64, uint64, error) {
	data, err := cache.db.Get([]byte(fmt.Sprintf("__aggregate__:%s", pathname)), nil)
	if err != nil {
		return 0, 0, 0, err
	}

	if len(data) != 24 {
		return 0, 0, 0, fmt.Errorf("invalid aggregate length: %d", len(data))
	}

	buffer := bytes.NewReader(data)
	var files uint64
	var dirs uint64
	var size uint64
	binary.Read(buffer, binary.LittleEndian, &files)
	binary.Read(buffer, binary.LittleEndian, &dirs)
	binary.Read(buffer, binary.LittleEndian, &size)
	return files, dirs, size, nil
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

func (snap *Snapshot) importerJob(imp *importer.Importer, sc *scanCache, scanDir string, options *PushOptions, maxConcurrency chan bool) (chan importer.ScanRecord, error) {
	//imp, err := importer.NewImporter(scanDir)
	//if err != nil {
	//		return nil, nil, err
	//	}

	scanner, err := imp.Scan()
	if err != nil {
		return nil, err
	}

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	filesChannel := make(chan importer.ScanRecord, 1000)

	go func() {
		snap.statistics.ImporterStart = time.Now()
		for _record := range scanner {
			if snap.skipExcludedPathname(options, _record) {
				continue
			}

			maxConcurrency <- true
			wg.Add(1)
			go func(record importer.ScanResult) {
				defer func() {
					<-maxConcurrency
					wg.Done()
				}()
				snap.updateImporterStatistics(record)

				switch record := record.(type) {
				case importer.ScanError:
					snap.Event(events.PathErrorEvent(snap.Header.SnapshotID, record.Pathname, record.Err.Error()))

				case importer.ScanRecord:
					snap.Event(events.PathEvent(snap.Header.SnapshotID, record.Pathname))
					if record.Stat.Mode().IsDir() {
						if err := sc.RecordPathname(record); err != nil {
							//return err
							return
						}
					} else {
						filesChannel <- record
					}
					extension := strings.ToLower(filepath.Ext(record.Pathname))
					if extension == "" {
						extension = "none"
					}
					mu.Lock()
					if _, exists := snap.Header.FileExtension[extension]; !exists {
						snap.Header.FileExtension[extension] = 0
					}
					snap.Header.FileExtension[extension]++
					mu.Unlock()
				}
			}(_record)
		}
		wg.Wait()
		close(filesChannel)
		snap.statistics.ImporterDuration = time.Since(snap.statistics.ImporterStart)
	}()

	return filesChannel, nil
}

func (snap *Snapshot) Backup(scanDir string, options *PushOptions) error {
	snap.Event(events.StartEvent())
	defer snap.Event(events.DoneEvent())

	cacheDir := filepath.Join(snap.repository.Context().GetCacheDir(), "fscache")
	cacheInstance, err := cache.New(cacheDir)
	if err != nil {
		return err
	}
	defer cacheInstance.Close()

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

	snap.Header.ImporterOrigin = imp.Origin()
	snap.Header.ImporterType = imp.Type()

	//t0 := time.Now()

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

	maxConcurrency := make(chan bool, options.MaxConcurrency)

	/* importer */
	filesChannel, err := snap.importerJob(imp, sc, scanDir, options, maxConcurrency)
	if err != nil {
		return err
	}

	/* scanner */
	scannerWg := sync.WaitGroup{}
	snap.statistics.ScannerStart = time.Now()
	for _record := range filesChannel {
		maxConcurrency <- true
		scannerWg.Add(1)
		go func(record importer.ScanRecord) {
			defer func() {
				<-maxConcurrency
				scannerWg.Done()
			}()

			snap.Event(events.FileEvent(snap.Header.SnapshotID, _record.Pathname))

			var fileEntry *vfs.FileEntry
			var object *objects.Object

			// Check if the file entry and underlying objects are already in the cache
			cachedFileEntry, cachedFileEntryChecksum, cachedFileEntrySize, err := cacheInstance.LookupFilename(scanDir, record.Pathname)
			if err == nil && cachedFileEntry != nil {
				if cachedFileEntry.ModTime.Equal(record.Stat.ModTime()) && cachedFileEntry.Size == record.Stat.Size() {
					fileEntry = cachedFileEntry
					cachedObject, err := cacheInstance.LookupObject(cachedFileEntry.Checksum)
					if err == nil && cachedObject != nil {
						object = cachedObject
					}
				}
			}

			// Chunkify the file if it is a regular file and we don't have a cached object
			if record.Stat.Mode().IsRegular() {
				if object == nil || !snap.CheckObject(object.Checksum) {
					object, err = snap.chunkify(imp, record)
					if err != nil {
						atomic.AddUint64(&snap.statistics.ChunkerErrors, 1)
						//return err
						return
					}
					if err := cacheInstance.RecordObject(object); err != nil {
						//return err
						return
					}
				}
			}

			if object != nil {
				if !snap.CheckObject(object.Checksum) {
					data, err := object.Serialize()
					if err != nil {
						//return err
						return
					}
					atomic.AddUint64(&snap.statistics.ObjectsCount, 1)
					atomic.AddUint64(&snap.statistics.ObjectsSize, uint64(len(data)))
					err = snap.PutObject(object.Checksum, data)
					if err != nil {
						//return err
						return
					}
				}
				snap.Metadata.AddMetadata(object.ContentType, object.Checksum) // XXX
			}

			var fileEntryChecksum [32]byte
			var fileEntrySize uint64
			if fileEntry != nil && snap.CheckFile(cachedFileEntryChecksum) {
				fileEntryChecksum = cachedFileEntryChecksum
				fileEntrySize = cachedFileEntrySize
			} else {
				fileEntry = vfs.NewFileEntry(filepath.Dir(record.Pathname), &record)
				if object != nil {
					for _, chunk := range object.Chunks {
						fileEntry.AddChunk(chunk)
					}
					fileEntry.AddChecksum(object.Checksum)
					if object.ContentType != "" {
						fileEntry.AddContentType(object.ContentType)
					}
				}

				// Serialize the FileEntry and store it in the repository
				serialized, err := fileEntry.Serialize()
				if err != nil {
					//return err
					return
				}

				fileEntryChecksum = snap.repository.Checksum(serialized)
				fileEntrySize = uint64(len(serialized))
				err = snap.PutFile(fileEntryChecksum, serialized)
				if err != nil {
					return
					//return err
				}

				// Store the newly generated FileEntry in the cache for future runs
				err = cacheInstance.RecordFilename(scanDir, record.Pathname, fileEntry)
				if err != nil {
					return
					//						return err
				}
			}
			atomic.AddUint64(&snap.statistics.VFSFilesCount, 1)
			atomic.AddUint64(&snap.statistics.VFSFilesSize, fileEntrySize)

			// Record the checksum of the FileEntry in the cache
			err = sc.RecordChecksum(record.Pathname, fileEntryChecksum)
			if err != nil {
				// return err
				return
			}
			atomic.AddUint64(&snap.statistics.ScannerProcessedSize, uint64(record.Stat.Size()))
			snap.Event(events.FileOKEvent(snap.Header.SnapshotID, record.Pathname))
		}(_record)
	}
	scannerWg.Wait()

	directories, err := sc.EnumerateKeysWithPrefixReverse("__pathname__", true)
	if err != nil {
		return err
	}
	for record := range directories {
		dirEntry := vfs.NewDirectoryEntry(filepath.Dir(record.Pathname), &record)

		for _, child := range record.Children {
			childpath := filepath.Join(record.Pathname, child.Name())
			value, err := sc.GetChecksum(childpath)
			if err != nil {
				continue
			}

			if child.IsDir() {
				dirEntry.AggregateDirs++

				files, dirs, size, err := sc.GetAggregate(childpath)
				if err != nil {
					continue
				}
				dirEntry.AggregateFiles += files
				dirEntry.AggregateDirs += dirs
				dirEntry.AggregateSize += size
			} else {
				dirEntry.AggregateFiles++
				dirEntry.AggregateSize += uint64(child.Size())
			}
			dirEntry.AddChild(value, child)
		}

		serialized, err := dirEntry.Serialize()
		if err != nil {
			return err
		}
		dirEntryChecksum := snap.repository.Checksum(serialized)
		dirEntrySize := uint64(len(serialized))

		if !snap.CheckDirectory(dirEntryChecksum) {
			err = snap.PutDirectory(dirEntryChecksum, serialized)
			if err != nil {
				return err
			}
		}
		err = sc.RecordChecksum(record.Pathname, dirEntryChecksum)
		if err != nil {
			return err
		}
		err = sc.RecordAggregates(record.Pathname, dirEntry.AggregateFiles, dirEntry.AggregateDirs, dirEntry.AggregateSize)
		if err != nil {
			return err
		}

		atomic.AddUint64(&snap.statistics.VFSDirectoriesCount, 1)
		atomic.AddUint64(&snap.statistics.VFSDirectoriesSize, dirEntrySize)
		snap.Event(events.DirectoryOKEvent(snap.Header.SnapshotID, record.Pathname))
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

	var firstChunk = true
	var cdcOffset uint64
	var object_t32 [32]byte

	// Helper function to process a chunk
	processChunk := func(data []byte) error {
		var chunk_t32 [32]byte
		chunkHasher := snap.repository.Hasher()

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
		copy(chunk_t32[:], chunkHasher.Sum(nil))

		chunk := objects.Chunk{Checksum: chunk_t32, Length: uint32(len(data))}
		object.Chunks = append(object.Chunks, chunk)
		cdcOffset += uint64(len(data))

		if !snap.CheckChunk(chunk.Checksum) {
			atomic.AddUint64(&snap.statistics.ChunksCount, 1)
			atomic.AddUint64(&snap.statistics.ChunksSize, uint64(len(data)))
			return snap.PutChunk(chunk.Checksum, data)
		}
		return nil
	}

	if record.Stat.Size() == 0 {
		// Produce an empty chunk for empty file
		if err := processChunk([]byte{}); err != nil {
			return nil, err
		}
	} else if record.Stat.Size() < int64(snap.repository.Configuration().ChunkingMin) {
		// Small file case: read entire file into memory
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

	copy(object_t32[:], objectHasher.Sum(nil))
	object.Checksum = object_t32
	return object, nil
}
