package snapshot

import (
	"io"
	"math"
	"mime"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chunkers "github.com/PlakarLabs/go-cdc-chunkers"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/ultracdc"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/progress"
	"github.com/PlakarLabs/plakar/vfs"
	"github.com/gabriel-vasile/mimetype"
)

func pathnameCached(snapshot *Snapshot, fi vfs.FileInfo, pathname string) (*objects.Object, error) {
	cache := snapshot.repository.GetCache()

	if cache == nil {
		return nil, nil
	}

	cachedObject, err := snapshot.GetCachedObject(pathname)
	if err != nil {
		return nil, nil
	}

	if cachedObject.Info.Mode() != fi.Mode() || cachedObject.Info.Dev() != fi.Dev() || cachedObject.Info.Size() != fi.Size() || cachedObject.Info.ModTime() != fi.ModTime() {
		return nil, nil
	}

	object := objects.Object{}
	object.Checksum = cachedObject.Checksum
	object.Chunks = make([][32]byte, 0)
	for _, chunk := range cachedObject.Chunks {
		object.Chunks = append(object.Chunks, chunk.Checksum)
	}
	object.ContentType = cachedObject.ContentType

	for offset, _ := range object.Chunks {
		//		chunk := cachedObject.Chunks[offset]
		//		exists, err := snapshot.CheckChunk(chunk.Checksum)
		//		if err != nil {
		//			return nil, err
		//		}
		//		if !exists {
		//			return nil, nil
		//		}
		snapshot.Index.AddChunk(cachedObject.Chunks[offset])
	}
	return &object, nil
}

func chunkify(snapshot *Snapshot, pathname string) (*objects.Object, error) {
	rd, err := snapshot.Filesystem.ImporterOpen(pathname)
	if err != nil {
		return nil, err
	}
	defer rd.Close()

	object := &objects.Object{}
	object.ContentType = mime.TypeByExtension(filepath.Ext(pathname))
	objectHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)

	chk, err := chunkers.NewChunker("ultracdc", rd, &chunkers.ChunkerOpts{
		MinSize:    256 << 10,
		NormalSize: (256 << 10) + (8 << 10),
		MaxSize:    1024 << 10,
	})
	if err != nil {
		return nil, err
	}

	chunkHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)

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
			chunk.Start = cdcOffset
			chunk.Length = uint(len(cdcChunk))
			object.Chunks = append(object.Chunks, chunk.Checksum)
			cdcOffset += uint64(len(cdcChunk))

			indexChunk := snapshot.Index.LookupChunk(chunk.Checksum)
			if indexChunk == nil {
				exists, err := snapshot.CheckChunk(chunk.Checksum)
				if err != nil {
					return nil, err
				}
				if !exists {
					nbytes, err := snapshot.PutChunk(chunk.Checksum, cdcChunk)
					if err != nil {
						return nil, err
					}
					atomic.AddUint64(&snapshot.Metadata.ChunksTransferCount, uint64(1))
					atomic.AddUint64(&snapshot.Metadata.ChunksTransferSize, uint64(nbytes))

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

func (snapshot *Snapshot) Push(scanDir string, showProgress bool) error {

	maxConcurrency := make(chan bool, runtime.NumCPU()*8+1)
	wg := sync.WaitGroup{}

	t0 := time.Now()

	cache := snapshot.repository.Cache

	var c chan int64
	if showProgress {
		c = progress.NewProgress("push", "scanning filesystem")
	} else {
		c = make(chan int64)
		go func() {
			for _ = range c {
			}
		}()
	}

	snapshot.Metadata.ScannedDirectories = make([]string, 0)

	fs, err := vfs.NewFilesystemFromScan(snapshot.repository.Location, scanDir)
	if err != nil {
		logger.Warn("%s", err)
	}
	snapshot.Filesystem = fs

	if !strings.Contains(scanDir, "://") {
		scanDir, err = filepath.Abs(scanDir)
		if err != nil {
			logger.Warn("%s", err)
			return err
		}
	}
	snapshot.Metadata.ScannedDirectories = append(snapshot.Metadata.ScannedDirectories, scanDir)

	close(c)

	if showProgress {
		c = progress.NewProgressBytes("push", "pushing snapshot", int64(snapshot.Filesystem.Size()))
	} else {
		c = make(chan int64)
		go func() {
			for _ = range c {
			}
		}()
	}
	for _, filename := range snapshot.Filesystem.ListFiles() {
		maxConcurrency <- true
		wg.Add(1)
		go func(_filename string) {
			defer func() { wg.Done() }()
			defer func() { <-maxConcurrency }()

			fileinfo, exists := snapshot.Filesystem.LookupInodeForFile(_filename)
			if !exists {
				logger.Warn("%s: failed to find file informations", _filename)
				return
			}
			c <- fileinfo.Size()
			atomic.AddUint64(&snapshot.Metadata.ScanSize, uint64(fileinfo.Size()))

			var object *objects.Object
			object, err := pathnameCached(snapshot, *fileinfo, _filename)
			if err != nil {
				// something went wrong with the cache
				// errchan <- err
			}

			exists = false
			if object != nil {
				if snapshot.Index.LookupObject(object.Checksum) != nil {
					exists = true
				} else {
					exists, err = snapshot.CheckObject(object.Checksum)
					if err != nil {
						logger.Warn("%s: failed to check object existence: %s", _filename, err)
						return
					}
				}
			}

			// can't reuse object from cache, chunkify
			if object == nil || !exists {
				object, err = chunkify(snapshot, _filename)
				if err != nil {
					logger.Warn("%s: could not chunkify: %s", _filename, err)
					return
				}
				if cache != nil {
					snapshot.PutCachedObject(_filename, *object, *fileinfo)
				}

				if snapshot.Index.LookupObject(object.Checksum) == nil {
					exists, err = snapshot.CheckObject(object.Checksum)
					if err != nil {
						logger.Warn("%s: failed to check object existence: %s", _filename, err)
						return
					}
					if !exists {
						nbytes, err := snapshot.PutObject(object)
						if err != nil {
							logger.Warn("%s: failed to store object: %s", _filename, err)
							return
						}
						atomic.AddUint64(&snapshot.Metadata.ObjectsTransferCount, uint64(1))
						atomic.AddUint64(&snapshot.Metadata.ObjectsTransferSize, uint64(nbytes))
					}
				}
			}
			snapshot.Index.AddObject(object)

			hasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
			hasher.Write([]byte(_filename))
			pathnameChecksum := hasher.Sum(nil)
			pathnameID := snapshot.Filesystem.GetPathnameID(_filename)

			key := [32]byte{}
			copy(key[:], pathnameChecksum)

			snapshot.Index.RecordPathnameChecksum(key, pathnameID)
			snapshot.Index.LinkPathnameToObject(pathnameID, object)
			atomic.AddUint64(&snapshot.Metadata.ScanProcessedSize, uint64(fileinfo.Size()))
		}(filename)
	}
	wg.Wait()
	close(c)
	snapshot.Filesystem.ImporterEnd()

	snapshot.Metadata.ChunksCount = uint64(len(snapshot.Index.ListChunks()))
	snapshot.Metadata.ObjectsCount = uint64(len(snapshot.Index.ListObjects()))
	snapshot.Metadata.FilesCount = uint64(len(snapshot.Filesystem.ListFiles()))
	snapshot.Metadata.DirectoriesCount = uint64(len(snapshot.Filesystem.ListDirectories()))

	for _, chunk := range snapshot.Index.Chunks {
		atomic.AddUint64(&snapshot.Metadata.ChunksSize, uint64(chunk.Length))
	}

	for _, key := range snapshot.Index.ListContentTypes() {
		objectType := strings.Split(key, ";")[0]
		objectKind := strings.Split(key, "/")[0]
		if objectType == "" {
			objectType = "unknown"
			objectKind = "unknown"
		}
		if _, exists := snapshot.Metadata.FileKind[objectKind]; !exists {
			snapshot.Metadata.FileKind[objectKind] = 0
		}
		snapshot.Metadata.FileKind[objectKind] += uint64(len(snapshot.Index.LookupObjectsForContentType(key)))

		if _, exists := snapshot.Metadata.FileType[objectType]; !exists {
			snapshot.Metadata.FileType[objectType] = 0
		}
		snapshot.Metadata.FileType[objectType] += uint64(len(snapshot.Index.LookupObjectsForContentType(key)))
	}

	for _, key := range snapshot.Filesystem.ListStat() {
		extension := strings.ToLower(filepath.Ext(key))
		if extension == "" {
			extension = "none"
		}
		if _, exists := snapshot.Metadata.FileExtension[extension]; !exists {
			snapshot.Metadata.FileExtension[extension] = 0
		}
		snapshot.Metadata.FileExtension[extension]++
	}

	for key, value := range snapshot.Metadata.FileType {
		snapshot.Metadata.FilePercentType[key] = math.Round((float64(value)/float64(snapshot.Metadata.FilesCount)*100)*100) / 100
	}
	for key, value := range snapshot.Metadata.FileKind {
		snapshot.Metadata.FilePercentKind[key] = math.Round((float64(value)/float64(snapshot.Metadata.FilesCount)*100)*100) / 100
	}
	for key, value := range snapshot.Metadata.FileExtension {
		snapshot.Metadata.FilePercentExtension[key] = math.Round((float64(value)/float64(snapshot.Metadata.FilesCount)*100)*100) / 100
	}

	snapshot.Metadata.NonRegularCount = uint64(len(snapshot.Filesystem.ListNonRegular()))
	snapshot.Metadata.PathnamesCount = uint64(len(snapshot.Filesystem.ListStat()))

	snapshot.Metadata.CreationDuration = time.Since(t0)

	err = snapshot.Commit()
	if err != nil {
		logger.Warn("could not commit snapshot: %s", err)
	}

	return err
}
