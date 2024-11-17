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

type FileSummary struct {
	Type        importer.RecordType `msgpack:"type"`
	Size        uint64              `msgpack:"size"`
	Objects     uint64              `msgpack:"objects"`
	Chunks      uint64              `msgpack:"chunks"`
	ModTime     int64               `msgpack:"modTime"`
	ContentType string              `msgpack:"contentType"`
	Entropy     float64             `msgpack:"entropy"`
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

func (cache *scanCache) RecordChecksum(pathname string, checksum [32]byte) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}
	return cache.db.Put([]byte(fmt.Sprintf("__checksum__:%s", pathname)), checksum[:], nil)
}

func (cache *scanCache) RecordStatistics(pathname string, statistics *vfs.Statistics) error {
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

func (cache *scanCache) RecordFileSummary(pathname string, summary *FileSummary) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	buffer, err := msgpack.Marshal(summary)
	if err != nil {
		return err
	}
	return cache.db.Put([]byte(fmt.Sprintf("__file_summary__:%s", pathname)), buffer, nil)
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

func (cache *scanCache) GetStatistics(pathname string) (*vfs.Statistics, error) {
	data, err := cache.db.Get([]byte(fmt.Sprintf("__statistics__:%s", pathname)), nil)
	if err != nil {
		return nil, err
	}

	var stats vfs.Statistics
	err = msgpack.Unmarshal(data, &stats)
	if err != nil {
		return nil, err
	}
	return &stats, nil
}

func (cache *scanCache) GetFileSummary(pathname string) (*FileSummary, error) {
	data, err := cache.db.Get([]byte(fmt.Sprintf("__file_summary__:%s", pathname)), nil)
	if err != nil {
		return nil, err
	}

	var stats FileSummary
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

func (snap *Snapshot) importerJob(imp *importer.Importer, sc *scanCache, options *PushOptions, maxConcurrency chan bool) (chan importer.ScanRecord, error) {
	//imp, err := importer.NewImporter(scanDir)
	//if err != nil {
	//		return nil, nil, err
	//	}

	scanner, err := imp.Scan()
	if err != nil {
		return nil, err
	}

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
					if record.FileInfo.Mode().IsDir() {
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
					//					mu.Lock()
					//					if _, exists := snap.Header.FileExtension[extension]; !exists {
					//						snap.Header.FileExtension[extension] = 0
					//					}
					//					snap.Header.FileExtension[extension]++
					//					mu.Unlock()
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

	snap.Header.Origin = imp.Origin()
	snap.Header.Type = imp.Type()

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
	snap.Header.ScannedDirectory = filepath.ToSlash(scanDir)

	maxConcurrency := make(chan bool, options.MaxConcurrency)

	/* importer */
	filesChannel, err := snap.importerJob(imp, sc, options, maxConcurrency)
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
					fileEntry.Object = object
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

				fileSummary := &FileSummary{
					Type:    importer.RecordTypeFile,
					Size:    uint64(record.FileInfo.Size()),
					ModTime: record.FileInfo.ModTime().Unix(),
				}
				if object != nil {
					fileSummary.Objects++
					fileSummary.Chunks += uint64(len(object.Chunks))
					fileSummary.ContentType = object.ContentType
					fileSummary.Entropy = object.Entropy
				}

				err = sc.RecordFileSummary(record.Pathname, fileSummary)
				if err != nil {
					return
					//return err
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
			atomic.AddUint64(&snap.statistics.ScannerProcessedSize, uint64(record.FileInfo.Size()))
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
		dirEntry.NumChildren = uint64(len(record.Children))

		dirEntropy := 0.0
		dirSize := uint64(0)
		nFiles := 0
		for _, child := range record.Children {
			childpath := filepath.Join(record.Pathname, child.Name())
			value, err := sc.GetChecksum(childpath)
			if err != nil {
				continue
			}

			if child.IsDir() {
				childStatistics := &vfs.Statistics{}
				dirEntry.Statistics.Directory.Directories++
				dirEntry.Statistics.Below.Directories++

				childStatistics, err := sc.GetStatistics(childpath)
				if err != nil {
					continue
				}

				dirEntry.Statistics.Below.Files += childStatistics.Below.Files + childStatistics.Directory.Files
				dirEntry.Statistics.Below.Directories += childStatistics.Below.Directories + childStatistics.Directory.Directories
				dirEntry.Statistics.Below.Symlinks += childStatistics.Below.Symlinks + childStatistics.Directory.Symlinks
				dirEntry.Statistics.Below.Devices += childStatistics.Below.Devices + childStatistics.Directory.Devices
				dirEntry.Statistics.Below.Pipes += childStatistics.Below.Pipes + childStatistics.Directory.Pipes
				dirEntry.Statistics.Below.Sockets += childStatistics.Below.Sockets + childStatistics.Directory.Sockets
				dirEntry.Statistics.Below.Objects += childStatistics.Below.Objects + childStatistics.Directory.Objects
				dirEntry.Statistics.Below.Chunks += childStatistics.Below.Chunks + childStatistics.Directory.Chunks

				if dirEntry.Statistics.Below.MinSize == 0 || childStatistics.Below.MinSize < dirEntry.Statistics.Below.MinSize {
					dirEntry.Statistics.Below.MinSize = childStatistics.Below.MinSize
				}
				if dirEntry.Statistics.Below.MinSize == 0 || childStatistics.Directory.MinSize < dirEntry.Statistics.Below.MinSize {
					dirEntry.Statistics.Below.MinSize = childStatistics.Directory.MinSize
				}
				if dirEntry.Statistics.Below.MaxSize == 0 || childStatistics.Below.MaxSize > dirEntry.Statistics.Below.MaxSize {
					dirEntry.Statistics.Below.MaxSize = childStatistics.Below.MaxSize
				}
				if dirEntry.Statistics.Below.MaxSize == 0 || childStatistics.Directory.MaxSize > dirEntry.Statistics.Below.MaxSize {
					dirEntry.Statistics.Below.MaxSize = childStatistics.Directory.MaxSize
				}
				dirEntry.Statistics.Below.Size += childStatistics.Below.Size + childStatistics.Directory.Size

				if dirEntry.Statistics.Below.MinModTime == 0 || childStatistics.Below.MinModTime < dirEntry.Statistics.Below.MinModTime {
					dirEntry.Statistics.Below.MinModTime = childStatistics.Below.MinModTime
				}
				if dirEntry.Statistics.Below.MinModTime == 0 || childStatistics.Directory.MinModTime < dirEntry.Statistics.Below.MinModTime {
					dirEntry.Statistics.Below.MinModTime = childStatistics.Directory.MinModTime
				}
				if dirEntry.Statistics.Below.MaxModTime == 0 || childStatistics.Below.MaxModTime > dirEntry.Statistics.Below.MaxModTime {
					dirEntry.Statistics.Below.MaxModTime = childStatistics.Below.MaxModTime
				}
				if dirEntry.Statistics.Below.MaxModTime == 0 || childStatistics.Directory.MaxModTime > dirEntry.Statistics.Below.MaxModTime {
					dirEntry.Statistics.Below.MaxModTime = childStatistics.Directory.MaxModTime
				}

				if dirEntry.Statistics.Below.MinEntropy == 0 || childStatistics.Below.MinEntropy < dirEntry.Statistics.Below.MinEntropy {
					dirEntry.Statistics.Below.MinEntropy = childStatistics.Below.MinEntropy
				}
				if dirEntry.Statistics.Below.MinEntropy == 0 || childStatistics.Directory.MinEntropy < dirEntry.Statistics.Below.MinEntropy {
					dirEntry.Statistics.Below.MinEntropy = childStatistics.Directory.MinEntropy
				}
				if dirEntry.Statistics.Below.MaxEntropy == 0 || childStatistics.Below.MaxEntropy > dirEntry.Statistics.Below.MaxEntropy {
					dirEntry.Statistics.Below.MaxEntropy = childStatistics.Below.MaxEntropy
				}
				if dirEntry.Statistics.Below.MaxEntropy == 0 || childStatistics.Directory.MaxEntropy > dirEntry.Statistics.Below.MaxEntropy {
					dirEntry.Statistics.Below.MaxEntropy = childStatistics.Directory.MaxEntropy
				}
				dirEntry.Statistics.Below.HiEntropy += childStatistics.Below.HiEntropy + childStatistics.Directory.HiEntropy
				dirEntry.Statistics.Below.LoEntropy += childStatistics.Below.LoEntropy + childStatistics.Directory.LoEntropy

				dirEntry.Statistics.Below.MIMEAudio += childStatistics.Directory.MIMEAudio + childStatistics.Below.MIMEAudio
				dirEntry.Statistics.Below.MIMEVideo += childStatistics.Directory.MIMEVideo + childStatistics.Below.MIMEVideo
				dirEntry.Statistics.Below.MIMEImage += childStatistics.Directory.MIMEImage + childStatistics.Below.MIMEImage
				dirEntry.Statistics.Below.MIMEText += childStatistics.Directory.MIMEText + childStatistics.Below.MIMEText
				dirEntry.Statistics.Below.MIMEApplication += childStatistics.Directory.MIMEApplication + childStatistics.Below.MIMEApplication
				dirEntry.Statistics.Below.MIMEOther += childStatistics.Directory.MIMEOther + childStatistics.Below.MIMEOther

				dirEntry.AddDirectoryChild(value, child, childStatistics)

			} else {
				fileSummary, err := sc.GetFileSummary(childpath)
				if err != nil {
					continue
				}

				switch fileSummary.Type {
				case importer.RecordTypeFile:
					dirEntry.Statistics.Below.Files++
					dirEntry.Statistics.Directory.Files++
				case importer.RecordTypeDirectory:
					dirEntry.Statistics.Below.Directories++
					dirEntry.Statistics.Directory.Directories++
				case importer.RecordTypeSymlink:
					dirEntry.Statistics.Below.Symlinks++
					dirEntry.Statistics.Directory.Symlinks++
				case importer.RecordTypeDevice:
					dirEntry.Statistics.Below.Devices++
					dirEntry.Statistics.Directory.Devices++
				case importer.RecordTypePipe:
					dirEntry.Statistics.Below.Pipes++
					dirEntry.Statistics.Directory.Pipes++
				case importer.RecordTypeSocket:
					dirEntry.Statistics.Below.Sockets++
					dirEntry.Statistics.Directory.Sockets++
				default:
					panic("unexpected record type")
				}

				if fileSummary.Objects > 0 {
					dirEntry.Statistics.Below.Objects += fileSummary.Objects
					dirEntry.Statistics.Below.Chunks += fileSummary.Chunks
					dirEntry.Statistics.Directory.Objects += fileSummary.Objects
					dirEntry.Statistics.Directory.Chunks += fileSummary.Chunks
				}

				if fileSummary.ModTime < dirEntry.Statistics.Below.MinModTime || dirEntry.Statistics.Below.MinModTime == 0 {
					dirEntry.Statistics.Below.MinModTime = fileSummary.ModTime
				}
				if fileSummary.ModTime > dirEntry.Statistics.Below.MaxModTime || dirEntry.Statistics.Below.MaxModTime == 0 {
					dirEntry.Statistics.Below.MaxModTime = fileSummary.ModTime
				}

				if fileSummary.ModTime < dirEntry.Statistics.Directory.MinModTime || dirEntry.Statistics.Directory.MinModTime == 0 {
					dirEntry.Statistics.Directory.MinModTime = fileSummary.ModTime
				}
				if fileSummary.ModTime > dirEntry.Statistics.Directory.MaxModTime || dirEntry.Statistics.Directory.MaxModTime == 0 {
					dirEntry.Statistics.Directory.MaxModTime = fileSummary.ModTime
				}

				if fileSummary.Size < dirEntry.Statistics.Below.MinSize || dirEntry.Statistics.Below.MinSize == 0 {
					dirEntry.Statistics.Below.MinSize = fileSummary.Size
				}
				if fileSummary.Size > dirEntry.Statistics.Below.MaxSize || dirEntry.Statistics.Below.MaxSize == 0 {
					dirEntry.Statistics.Below.MaxSize = fileSummary.Size
				}

				if fileSummary.Size < dirEntry.Statistics.Directory.MinSize || dirEntry.Statistics.Directory.MinSize == 0 {
					dirEntry.Statistics.Directory.MinSize = fileSummary.Size
				}
				if fileSummary.Size > dirEntry.Statistics.Directory.MaxSize || dirEntry.Statistics.Directory.MaxSize == 0 {
					dirEntry.Statistics.Directory.MaxSize = fileSummary.Size
				}

				if fileSummary.Entropy < dirEntry.Statistics.Below.MinEntropy || dirEntry.Statistics.Below.MinEntropy == 0 {
					dirEntry.Statistics.Below.MinEntropy = fileSummary.Entropy
				}
				if fileSummary.Entropy > dirEntry.Statistics.Below.MaxEntropy || dirEntry.Statistics.Below.MaxEntropy == 0 {
					dirEntry.Statistics.Below.MaxEntropy = fileSummary.Entropy
				}

				if fileSummary.Entropy < dirEntry.Statistics.Directory.MinEntropy || dirEntry.Statistics.Directory.MinEntropy == 0 {
					dirEntry.Statistics.Directory.MinEntropy = fileSummary.Entropy
				}
				if fileSummary.Entropy > dirEntry.Statistics.Directory.MaxEntropy || dirEntry.Statistics.Directory.MaxEntropy == 0 {
					dirEntry.Statistics.Directory.MaxEntropy = fileSummary.Entropy
				}

				if fileSummary.Entropy <= 2.0 {
					dirEntry.Statistics.Below.LoEntropy++
					dirEntry.Statistics.Directory.LoEntropy++
				} else if fileSummary.Entropy >= 7.0 {
					dirEntry.Statistics.Below.HiEntropy++
					dirEntry.Statistics.Directory.HiEntropy++
				}
				dirEntropy += fileSummary.Entropy
				nFiles++

				if fileSummary.ContentType != "" {
					if strings.HasPrefix(fileSummary.ContentType, "text/") {
						dirEntry.Statistics.Directory.MIMEText++
					} else if strings.HasPrefix(fileSummary.ContentType, "image/") {
						dirEntry.Statistics.Directory.MIMEImage++
					} else if strings.HasPrefix(fileSummary.ContentType, "audio/") {
						dirEntry.Statistics.Directory.MIMEAudio++
					} else if strings.HasPrefix(fileSummary.ContentType, "video/") {
						dirEntry.Statistics.Directory.MIMEVideo++
					} else if strings.HasPrefix(fileSummary.ContentType, "application/") {
						dirEntry.Statistics.Directory.MIMEApplication++
					} else {
						dirEntry.Statistics.Directory.MIMEOther++
					}
				}

				dirEntry.Statistics.Directory.Size += fileSummary.Size
				dirEntry.Statistics.Below.Size += uint64(child.Size())
				dirEntry.AddFileChild(value, child)
			}
		}
		if nFiles > 0 {
			dirEntry.Statistics.Directory.AvgEntropy = dirEntropy / float64(nFiles)
			dirEntry.Statistics.Directory.AvgSize = dirSize / uint64(nFiles)
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
		err = sc.RecordStatistics(record.Pathname, &dirEntry.Statistics)
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

		if !snap.CheckChunk(chunk.Checksum) {
			atomic.AddUint64(&snap.statistics.ChunksCount, 1)
			atomic.AddUint64(&snap.statistics.ChunksSize, uint64(len(data)))
			return snap.PutChunk(chunk.Checksum, data)
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
