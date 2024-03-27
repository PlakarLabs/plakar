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

	chunkers "github.com/PlakarLabs/go-cdc-chunkers"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/fastcdc"
	_ "github.com/PlakarLabs/go-cdc-chunkers/chunkers/ultracdc"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/vfs"
	"github.com/gabriel-vasile/mimetype"
	"github.com/gobwas/glob"
)

type PushOptions struct {
	MaxConcurrency uint64
	Excludes       []glob.Glob
}

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

	for offset := range object.Chunks {
		chunk := cachedObject.Chunks[offset]
		exists := snapshot.CheckChunk(chunk.Checksum)
		if !exists {
			return nil, nil
		}
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

	object := &objects.Object{}
	object.ContentType = mime.TypeByExtension(filepath.Ext(pathname))
	objectHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)

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

		indexChunk := snapshot.Index.LookupChunk(chunk.Checksum)
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
			chunk.Length = uint32(len(cdcChunk))
			object.Chunks = append(object.Chunks, chunk.Checksum)
			cdcOffset += uint64(len(cdcChunk))

			indexChunk := snapshot.Index.LookupChunk(chunk.Checksum)
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

func (snapshot *Snapshot) Push(scanDir string, options *PushOptions) error {
	if err := snapshot.Lock(); err != nil {
		return err
	}
	defer snapshot.Unlock()

	locksID, err := snapshot.repository.GetLocks()
	if err != nil {
		return err
	}
	for _, lockID := range locksID {
		if lockID == snapshot.Header.IndexID {
			continue
		}
		if lock, err := GetLock(snapshot.repository, lockID); err != nil {
			if os.IsNotExist(err) {
				// was removed since we got the list
				continue
			}
			return err
		} else {
			if lock.Exclusive && !lock.Expired(time.Minute*15) {
				return fmt.Errorf("can't push: %s is exclusively locked", snapshot.repository.Location)
			}
		}
	}

	lockDone := make(chan bool)
	defer close(lockDone)
	go func() {
		for {
			select {
			case <-lockDone:
				return
			case <-time.After(5 * time.Minute):
				snapshot.Lock()
			}
		}
	}()

	maxConcurrency := make(chan struct{}, options.MaxConcurrency)

	wg := sync.WaitGroup{}

	t0 := time.Now()

	cache := snapshot.repository.Cache

	snapshot.Header.ScannedDirectories = make([]string, 0)

	fs, err := vfs.NewFilesystemFromScan(snapshot.repository.Location, scanDir, options.Excludes)
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
		maxConcurrency <- struct{}{}
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
				exists = snapshot.CheckObject(object.Checksum)
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

				if !snapshot.Index.ObjectExists(object.Checksum) {
					exists = snapshot.CheckObject(object.Checksum)
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
			snapshot.Metadata.AddMetadata(object.ContentType, object.Checksum)

			hasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
			hasher.Write([]byte(_filename))
			pathnameChecksum := hasher.Sum(nil)
			key := [32]byte{}
			copy(key[:], pathnameChecksum)

			snapshot.Index.LinkPathnameToObject(key, object)
			atomic.AddUint64(&snapshot.Header.ScanProcessedSize, uint64(fileinfo.Size()))
		}(filename)
	}
	wg.Wait()
	snapshot.Filesystem.ImporterEnd()

	snapshot.Header.ChunksCount = uint64(len(snapshot.Index.ListChunks()))
	snapshot.Header.ObjectsCount = uint64(len(snapshot.Index.ListObjects()))
	snapshot.Header.FilesCount = uint64(len(snapshot.Filesystem.ListFiles()))
	snapshot.Header.DirectoriesCount = uint64(len(snapshot.Filesystem.ListDirectories()))

	for _, chunk := range snapshot.Index.ListChunks() {
		chunkLength, exists := snapshot.Index.GetChunkLength(chunk)
		if !exists {
			panic("ListChunks: corrupted index")
		}
		atomic.AddUint64(&snapshot.Header.ChunksSize, uint64(chunkLength))
	}

	for _, key := range snapshot.Metadata.ListKeys() {
		objectType := strings.Split(key, ";")[0]
		objectKind := strings.Split(key, "/")[0]
		if objectType == "" {
			objectType = "unknown"
			objectKind = "unknown"
		}
		if _, exists := snapshot.Header.FileKind[objectKind]; !exists {
			snapshot.Header.FileKind[objectKind] = 0
		}
		snapshot.Header.FileKind[objectKind] += uint64(len(snapshot.Metadata.ListValues(key)))

		if _, exists := snapshot.Header.FileType[objectType]; !exists {
			snapshot.Header.FileType[objectType] = 0
		}
		snapshot.Header.FileType[objectType] += uint64(len(snapshot.Metadata.ListValues(key)))
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
