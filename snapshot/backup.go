package snapshot

import (
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
	"github.com/PlakarKorp/plakar/packfile"
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

type ErrorEntry struct {
	Predecessor objects.Checksum `msgpack:"predecessor"`
	Pathname    string           `msgpack:"pathname"`
	Error       string           `msgpack:"error"`
}

type BackupContext struct {
	aborted        atomic.Bool
	abortedReason  error
	imp            *importer.Importer
	sc             *scanCache
	maxConcurrency chan bool
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

func (cache *scanCache) RecordError(pathname string, err error) error {
	key := fmt.Sprintf("__error__:%s", pathname)
	return cache.db.Put([]byte(key), []byte(err.Error()), nil)
}

func (cache *scanCache) EnumerateErrorsWithinDirectory(directory string, reverse bool) (<-chan ErrorEntry, error) {
	// Ensure directory ends with a trailing slash for consistency
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	// Create a channel to return the keys
	keyChan := make(chan ErrorEntry)

	// Start a goroutine to perform the iteration
	go func() {
		defer close(keyChan) // Ensure the channel is closed when the function exits

		iter := cache.db.NewIterator(nil, nil)
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

func (cache *scanCache) EnumerateErrorsWithinDirectory2(directory string) (<-chan ErrorEntry, error) {
	// Ensure directory ends with a trailing slash for consistency
	if !strings.HasSuffix(directory, "/") {
		directory += "/"
	}

	// Create a channel to return the keys
	keyChan := make(chan ErrorEntry)

	// Start a goroutine to perform the iteration
	go func() {
		defer close(keyChan) // Ensure the channel is closed when the function exits

		iter := cache.db.NewIterator(nil, nil)
		defer iter.Release()

		// Create the directory prefix to match keys
		directoryKeyPrefix := "__error__:" + directory

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
					path := strings.TrimPrefix(key, "__error__:")
					value := iter.Value()
					keyChan <- ErrorEntry{Pathname: path, Error: string(value)}
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

func (cache *scanCache) RecordPathname(record importer.ScanRecord) error {
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

func (cache *scanCache) RecordChecksum(pathname string, checksum objects.Checksum) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}
	return cache.db.Put([]byte(fmt.Sprintf("__checksum__:%s", pathname)), checksum[:], nil)
}

func (cache *scanCache) RecordStatistics(pathname string, statistics *vfs.Summary) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	buffer, err := msgpack.Marshal(statistics)
	if err != nil {
		return err
	}
	return cache.db.Put([]byte(fmt.Sprintf("__statistics__:%s", pathname)), buffer, nil)
}

func (cache *scanCache) GetChecksum(pathname string) (objects.Checksum, error) {
	data, err := cache.db.Get([]byte(fmt.Sprintf("__checksum__:%s", pathname)), nil)
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

func (cache *scanCache) GetStatistics(pathname string) (*vfs.Summary, error) {
	data, err := cache.db.Get([]byte(fmt.Sprintf("__statistics__:%s", pathname)), nil)
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

func (cache *scanCache) EnumerateImmediateChildPathnames2(directory string, reverse bool) (<-chan importer.ScanRecord, error) {
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

func (cache *scanCache) EnumerateImmediateChildPathnames(directory string, reverse bool) (<-chan importer.ScanRecord, error) {
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
			if record.FileInfo.Nlink() > 1 {
				atomic.AddUint64(&snap.statistics.ImporterLinks, 1)
			}
			atomic.AddUint64(&snap.statistics.ImporterSize, uint64(record.FileInfo.Size()))
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

func (snap *Snapshot) importerJob(backupCtx *BackupContext, options *PushOptions) (chan importer.ScanRecord, error) {
	scanner, err := backupCtx.imp.Scan()
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	filesChannel := make(chan importer.ScanRecord, 1000)

	go func() {
		snap.statistics.ImporterStart = time.Now()
		for _record := range scanner {
			if backupCtx.aborted.Load() {
				break
			}
			if snap.skipExcludedPathname(options, _record) {
				continue
			}

			backupCtx.maxConcurrency <- true
			wg.Add(1)
			go func(record importer.ScanResult) {
				defer func() {
					<-backupCtx.maxConcurrency
					wg.Done()
				}()
				snap.updateImporterStatistics(record)

				switch record := record.(type) {
				case importer.ScanError:
					if record.Pathname == backupCtx.imp.Root() || len(record.Pathname) < len(backupCtx.imp.Root()) {
						backupCtx.aborted.Store(true)
						backupCtx.abortedReason = record.Err
						return
					}
					backupCtx.sc.RecordError(record.Pathname, record.Err)
					snap.Event(events.PathErrorEvent(snap.Header.Identifier, record.Pathname, record.Err.Error()))

				case importer.ScanRecord:
					snap.Event(events.PathEvent(snap.Header.Identifier, record.Pathname))
					if err := backupCtx.sc.RecordPathname(record); err != nil {
						backupCtx.sc.RecordError(record.Pathname, err)
						return
					}
					if !record.FileInfo.Mode().IsDir() {
						filesChannel <- record
					}
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

	snap.Header.Importer.Origin = imp.Origin()
	snap.Header.Importer.Type = imp.Type()

	if !strings.Contains(scanDir, "://") {
		scanDir, err = filepath.Abs(scanDir)
		if err != nil {
			logger.Warn("%s", err)
			return err
		}
	} else {
		scanDir = imp.Root()
	}
	snap.Header.Importer.Directory = filepath.ToSlash(scanDir)

	backupCtx := &BackupContext{
		imp:            imp,
		sc:             sc,
		maxConcurrency: make(chan bool, options.MaxConcurrency),
	}

	/* importer */
	filesChannel, err := snap.importerJob(backupCtx, options)
	if err != nil {
		return err
	}

	/* scanner */
	scannerWg := sync.WaitGroup{}
	snap.statistics.ScannerStart = time.Now()
	for _record := range filesChannel {
		backupCtx.maxConcurrency <- true
		scannerWg.Add(1)
		go func(record importer.ScanRecord) {
			defer func() {
				<-backupCtx.maxConcurrency
				scannerWg.Done()
			}()

			snap.Event(events.FileEvent(snap.Header.Identifier, _record.Pathname))

			var fileEntry *vfs.FileEntry
			var object *objects.Object

			// Check if the file entry and underlying objects are already in the cache
			cachedFileEntry, cachedFileEntryChecksum, cachedFileEntrySize, err := cacheInstance.LookupFilename(imp.Origin(), record.Pathname)
			if err == nil && cachedFileEntry != nil {
				if cachedFileEntry.Stat().ModTime().Equal(record.FileInfo.ModTime()) && cachedFileEntry.Stat().Size() == record.FileInfo.Size() {
					fileEntry = cachedFileEntry
					if fileEntry.Type == importer.RecordTypeFile {
						cachedObject, err := cacheInstance.LookupObject(cachedFileEntry.Object.Checksum)
						if err == nil && cachedObject != nil {
							object = cachedObject
						}
					}
				}
			}

			// Chunkify the file if it is a regular file and we don't have a cached object
			if record.FileInfo.Mode().IsRegular() {
				if object == nil || !snap.BlobExists(packfile.TYPE_OBJECT, object.Checksum) {
					object, err = snap.chunkify(imp, record)
					if err != nil {
						atomic.AddUint64(&snap.statistics.ChunkerErrors, 1)
						sc.RecordError(record.Pathname, err)
						return
					}
					if err := cacheInstance.RecordObject(object); err != nil {
						sc.RecordError(record.Pathname, err)
						return
					}
				}
			}

			if object != nil {
				if !snap.BlobExists(packfile.TYPE_OBJECT, object.Checksum) {
					data, err := object.Serialize()
					if err != nil {
						sc.RecordError(record.Pathname, err)
						return
					}
					atomic.AddUint64(&snap.statistics.ObjectsCount, 1)
					atomic.AddUint64(&snap.statistics.ObjectsSize, uint64(len(data)))
					err = snap.PutBlob(packfile.TYPE_OBJECT, object.Checksum, data)
					if err != nil {
						sc.RecordError(record.Pathname, err)
						return
					}
				}
			}

			var fileEntryChecksum [32]byte
			var fileEntrySize uint64
			if fileEntry != nil && snap.BlobExists(packfile.TYPE_FILE, cachedFileEntryChecksum) {
				fileEntryChecksum = cachedFileEntryChecksum
				fileEntrySize = cachedFileEntrySize
			} else {
				fileEntry = vfs.NewFileEntry(filepath.Dir(record.Pathname), &record)
				if object != nil {
					fileEntry.Object = object
				}

				// Serialize the FileEntry and store it in the repository
				serialized, err := fileEntry.Serialize()
				if err != nil {
					sc.RecordError(record.Pathname, err)
					return
				}

				fileEntryChecksum = snap.repository.Checksum(serialized)
				fileEntrySize = uint64(len(serialized))
				err = snap.PutBlob(packfile.TYPE_FILE, fileEntryChecksum, serialized)
				if err != nil {
					sc.RecordError(record.Pathname, err)
					return
				}

				// Store the newly generated FileEntry in the cache for future runs
				err = cacheInstance.RecordFilename(imp.Origin(), record.Pathname, fileEntry)
				if err != nil {
					sc.RecordError(record.Pathname, err)
					return
				}

				fileSummary := &vfs.FileSummary{
					Type:    record.Type,
					Size:    uint64(record.FileInfo.Size()),
					Mode:    record.FileInfo.Mode(),
					ModTime: record.FileInfo.ModTime().Unix(),
				}
				if object != nil {
					fileSummary.Objects++
					fileSummary.Chunks += uint64(len(object.Chunks))
					fileSummary.ContentType = object.ContentType
					fileSummary.Entropy = object.Entropy
				}

				err = cacheInstance.RecordFileSummary(imp.Origin(), record.Pathname, fileSummary)
				if err != nil {
					sc.RecordError(record.Pathname, err)
					return
				}
			}
			atomic.AddUint64(&snap.statistics.VFSFilesCount, 1)
			atomic.AddUint64(&snap.statistics.VFSFilesSize, fileEntrySize)

			// Record the checksum of the FileEntry in the cache
			err = sc.RecordChecksum(record.Pathname, fileEntryChecksum)
			if err != nil {
				sc.RecordError(record.Pathname, err)
				return
			}
			atomic.AddUint64(&snap.statistics.ScannerProcessedSize, uint64(record.FileInfo.Size()))
			snap.Event(events.FileOKEvent(snap.Header.Identifier, record.Pathname))
		}(_record)
	}
	scannerWg.Wait()

	var rootSummary *vfs.Summary

	directories, err := sc.EnumerateKeysWithPrefixReverse("__pathname__", true)
	if err != nil {
		return err
	}
	for record := range directories {
		dirEntry := vfs.NewDirectoryEntry(filepath.Dir(record.Pathname), &record)

		childrenChan, err := sc.EnumerateImmediateChildPathnames(record.Pathname, true)
		if err != nil {
			return err
		}

		/* children */
		var lastChecksum *objects.Checksum
		for child := range childrenChan {
			childChecksum, err := sc.GetChecksum(child.Pathname)
			if err != nil {
				continue
			}
			childEntry := &vfs.ChildEntry{
				Lchecksum: childChecksum,
				LfileInfo: child.FileInfo,
			}
			if child.FileInfo.Mode().IsDir() {
				childSummary, err := sc.GetStatistics(child.Pathname)
				if err != nil {
					continue
				}
				dirEntry.Summary.UpdateBelow(childSummary)
				childEntry.Lsummary = childSummary
			} else {
				fileSummary, _, _, err := cacheInstance.LookupFileSummary(imp.Origin(), child.Pathname)
				if err != nil {
					continue
				}
				dirEntry.Summary.UpdateWithFileSummary(fileSummary)
			}

			if lastChecksum != nil {
				childEntry.Successor = lastChecksum
			}
			childEntrySerialized, err := childEntry.ToBytes()
			if err != nil {
				continue
			}
			childEntryChecksum := snap.repository.Checksum(childEntrySerialized)
			lastChecksum = &childEntryChecksum

			if !snap.BlobExists(packfile.TYPE_CHILD, childEntryChecksum) {
				if err := snap.PutBlob(packfile.TYPE_CHILD, childEntryChecksum, childEntrySerialized); err != nil {
					continue
				}
			}
			dirEntry.Children.Count++
		}
		dirEntry.Children.Head = lastChecksum
		dirEntry.Summary.UpdateAverages()

		/* errors */
		lastChecksum = nil
		if errc, err := sc.EnumerateErrorsWithinDirectory(record.Pathname, true); err == nil {
			for entry := range errc {
				errorEntry := &vfs.ErrorEntry{Name: filepath.Base(entry.Pathname), Error: entry.Error}

				if lastChecksum != nil {
					errorEntry.Successor = lastChecksum
				}
				errorEntrySerialized, err := errorEntry.ToBytes()
				if err != nil {
					continue
				}
				errorEntryChecksum := snap.repository.Checksum(errorEntrySerialized)
				lastChecksum = &errorEntryChecksum
				if !snap.BlobExists(packfile.TYPE_ERROR, errorEntryChecksum) {
					if err := snap.PutBlob(packfile.TYPE_ERROR, errorEntryChecksum, errorEntrySerialized); err != nil {
						continue
					}
				}
				dirEntry.Summary.Directory.Errors++
			}
			// Set first and last error checksums in the directory entry
			dirEntry.Errors.Count = dirEntry.Summary.Directory.Errors
			dirEntry.Errors.Head = lastChecksum
		}

		serialized, err := dirEntry.Serialize()
		if err != nil {
			return err
		}
		dirEntryChecksum := snap.repository.Checksum(serialized)
		dirEntrySize := uint64(len(serialized))

		if !snap.BlobExists(packfile.TYPE_DIRECTORY, dirEntryChecksum) {
			err = snap.PutBlob(packfile.TYPE_DIRECTORY, dirEntryChecksum, serialized)
			if err != nil {
				sc.RecordError(record.Pathname, err)
				return err
			}
		}
		err = sc.RecordChecksum(record.Pathname, dirEntryChecksum)
		if err != nil {
			sc.RecordError(record.Pathname, err)
			return err
		}
		err = sc.RecordStatistics(record.Pathname, &dirEntry.Summary)
		if err != nil {
			sc.RecordError(record.Pathname, err)
			return err
		}

		atomic.AddUint64(&snap.statistics.VFSDirectoriesCount, 1)
		atomic.AddUint64(&snap.statistics.VFSDirectoriesSize, dirEntrySize)
		snap.Event(events.DirectoryOKEvent(snap.Header.Identifier, record.Pathname))
		if record.Pathname == "/" {
			rootSummary = &dirEntry.Summary
		}
	}

	if backupCtx.aborted.Load() {
		return backupCtx.abortedReason
	}

	snap.statistics.ScannerDuration = time.Since(snap.statistics.ScannerStart)

	statistics, err := snap.statistics.Serialize()
	if err != nil {
		return err
	}
	statisticsChecksum := snap.repository.Checksum(statistics)
	err = snap.PutBlob(packfile.TYPE_DATA, statisticsChecksum, statistics)
	if err != nil {
		return err
	}

	value, err := sc.GetChecksum("/")
	if err != nil {
		return err
	}

	snap.Header.Root = value
	//snap.Header.Metadata = metadataChecksum
	snap.Header.Statistics = statisticsChecksum
	snap.Header.Duration = time.Since(snap.statistics.ImporterStart)
	snap.Header.Summary = *rootSummary

	/*
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
	*/
	return snap.Commit()
}

func entropy(data []byte) float64 {
	if len(data) == 0 {
		return 0.0
	}

	// Count the frequency of each byte value
	var freq [256]float64
	for _, b := range data {
		freq[b]++
	}

	// Calculate the entropy
	entropy := 0.0
	dataSize := float64(len(data))
	for _, f := range freq {
		if f > 0 {
			p := f / dataSize
			entropy -= p * math.Log2(p)
		}
	}
	return entropy
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

	var totalEntropy float64
	var totalDataSize uint64

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

		chunk := objects.Chunk{Checksum: chunk_t32, Length: uint32(len(data)), Entropy: entropy(data)}
		object.Chunks = append(object.Chunks, chunk)
		cdcOffset += uint64(len(data))

		totalEntropy += chunk.Entropy * float64(len(data))
		totalDataSize += uint64(len(data))

		if !snap.BlobExists(packfile.TYPE_CHUNK, chunk.Checksum) {
			atomic.AddUint64(&snap.statistics.ChunksCount, 1)
			atomic.AddUint64(&snap.statistics.ChunksSize, uint64(len(data)))
			return snap.PutBlob(packfile.TYPE_CHUNK, chunk.Checksum, data)
		}
		return nil
	}

	if record.FileInfo.Size() == 0 {
		// Produce an empty chunk for empty file
		if err := processChunk([]byte{}); err != nil {
			return nil, err
		}
	} else if record.FileInfo.Size() < int64(snap.repository.Configuration().Chunking.MinSize) {
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
	atomic.AddUint64(&snap.statistics.ChunkerSize, uint64(record.FileInfo.Size()))

	if totalDataSize > 0 {
		object.Entropy = totalEntropy / float64(totalDataSize)
	} else {
		object.Entropy = 0.0
	}

	copy(object_t32[:], objectHasher.Sum(nil))
	object.Checksum = object_t32
	return object, nil
}
