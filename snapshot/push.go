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

func chunkify(snapshot *Snapshot, pathname string, fi *vfs.FileInfo) (*objects.Object, error) {
	rd, err := snapshot.Filesystem.ImporterOpen(filepath.FromSlash(pathname))
	if err != nil {
		return nil, err
	}
	defer rd.Close()

	atomic.AddInt64(&snapshot.concurrentObjects, 1)
	defer atomic.AddInt64(&snapshot.concurrentObjects, -1)

	object := &objects.Object{}
	object.ContentType = mime.TypeByExtension(filepath.Ext(pathname))
	objectHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)

	if fi.Size() < 256<<10 {
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

		indexChunk := snapshot.Index.LookupChunk(chunk.Checksum)
		if indexChunk == nil {
			exists, err := snapshot.CheckChunk(chunk.Checksum)
			if err != nil {
				atomic.AddInt64(&snapshot.concurrentObjects, -1)
				return nil, err
			}
			if !exists {
				_, err := snapshot.PutChunk(chunk.Checksum, buf)
				if err != nil {
					atomic.AddInt64(&snapshot.concurrentObjects, -1)
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

	chunkHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)

	firstChunk := true
	cdcOffset := uint64(0)
	for {
		cdcChunk, err := chk.Next()
		if err != nil && err != io.EOF {
			return nil, err
		}

		if cdcChunk != nil {
			atomic.AddInt64(&snapshot.concurrentChunks, 1)
			atomic.AddInt64(&snapshot.concurrentChunksSize, int64(len(cdcChunk)))

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

			indexChunk := snapshot.Index.LookupChunk(chunk.Checksum)
			if indexChunk == nil {
				exists, err := snapshot.CheckChunk(chunk.Checksum)
				if err != nil {
					atomic.AddInt64(&snapshot.concurrentObjects, -1)
					return nil, err
				}
				if !exists {
					nbytes, err := snapshot.PutChunk(chunk.Checksum, cdcChunk)
					if err != nil {
						atomic.AddInt64(&snapshot.concurrentObjects, -1)
						return nil, err
					}
					atomic.AddUint64(&snapshot.Header.ChunksTransferCount, uint64(1))
					atomic.AddUint64(&snapshot.Header.ChunksTransferSize, uint64(nbytes))

				}
				snapshot.Index.AddChunk(&chunk)
			}
			atomic.AddInt64(&snapshot.concurrentChunksSize, -int64(len(cdcChunk)))
			atomic.AddInt64(&snapshot.concurrentChunks, -1)
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

func (snapshot *Snapshot) Push(scanDir string) error {

	maxConcurrency := make(chan bool, runtime.NumCPU()*8+1)
	wg := sync.WaitGroup{}

	t0 := time.Now()

	cache := snapshot.repository.Cache

	snapshot.Header.ScannedDirectories = make([]string, 0)

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
	snapshot.Header.ScannedDirectories = append(snapshot.Header.ScannedDirectories, filepath.ToSlash(scanDir))

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
			atomic.AddUint64(&snapshot.Header.ScanSize, uint64(fileinfo.Size()))

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
				object, err = chunkify(snapshot, _filename, fileinfo)
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
						err := snapshot.PutObject(object)
						if err != nil {
							logger.Warn("%s: failed to store object: %s", _filename, err)
							return
						}
						atomic.AddUint64(&snapshot.Header.ObjectsTransferCount, uint64(1))
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
			atomic.AddUint64(&snapshot.Header.ScanProcessedSize, uint64(fileinfo.Size()))
		}(filename)
	}
	wg.Wait()
	snapshot.Filesystem.ImporterEnd()

	snapshot.Header.ChunksCount = uint64(len(snapshot.Index.ListChunks()))
	snapshot.Header.ObjectsCount = uint64(len(snapshot.Index.ListObjects()))
	snapshot.Header.FilesCount = uint64(len(snapshot.Filesystem.ListFiles()))
	snapshot.Header.DirectoriesCount = uint64(len(snapshot.Filesystem.ListDirectories()))

	for _, chunkLength := range snapshot.Index.Chunks {
		atomic.AddUint64(&snapshot.Header.ChunksSize, uint64(chunkLength))
	}

	for _, key := range snapshot.Index.ListContentTypes() {
		objectType := strings.Split(key, ";")[0]
		objectKind := strings.Split(key, "/")[0]
		if objectType == "" {
			objectType = "unknown"
			objectKind = "unknown"
		}
		if _, exists := snapshot.Header.FileKind[objectKind]; !exists {
			snapshot.Header.FileKind[objectKind] = 0
		}
		snapshot.Header.FileKind[objectKind] += uint64(len(snapshot.Index.LookupObjectsForContentType(key)))

		if _, exists := snapshot.Header.FileType[objectType]; !exists {
			snapshot.Header.FileType[objectType] = 0
		}
		snapshot.Header.FileType[objectType] += uint64(len(snapshot.Index.LookupObjectsForContentType(key)))
	}

	for _, key := range snapshot.Filesystem.ListStat() {
		extension := strings.ToLower(filepath.Ext(key))
		if extension == "" {
			extension = "none"
		}
		if _, exists := snapshot.Header.FileExtension[extension]; !exists {
			snapshot.Header.FileExtension[extension] = 0
		}
		snapshot.Header.FileExtension[extension]++
	}

	for key, value := range snapshot.Header.FileType {
		snapshot.Header.FilePercentType[key] = math.Round((float64(value)/float64(snapshot.Header.FilesCount)*100)*100) / 100
	}
	for key, value := range snapshot.Header.FileKind {
		snapshot.Header.FilePercentKind[key] = math.Round((float64(value)/float64(snapshot.Header.FilesCount)*100)*100) / 100
	}
	for key, value := range snapshot.Header.FileExtension {
		snapshot.Header.FilePercentExtension[key] = math.Round((float64(value)/float64(snapshot.Header.FilesCount)*100)*100) / 100
	}

	snapshot.Header.NonRegularCount = uint64(len(snapshot.Filesystem.ListNonRegular()))
	snapshot.Header.PathnamesCount = uint64(len(snapshot.Filesystem.ListStat()))

	snapshot.Header.CreationDuration = time.Since(t0)

	err = snapshot.Commit()
	if err != nil {
		logger.Warn("could not commit snapshot: %s", err)
	}
	return err
}
