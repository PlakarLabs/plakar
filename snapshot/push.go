package snapshot

import (
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"mime"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"github.com/poolpOrg/go-fastcdc"
	"github.com/poolpOrg/plakar/logger"
)

type objectMsg struct {
	Object *Object
	Data   []byte
}

func pathnameCached(snapshot *Snapshot, fi Fileinfo, pathname string) (*Object, error) {
	cache := snapshot.repository.GetCache()

	if cache == nil {
		return nil, nil
	}

	cachedObject, err := snapshot.GetCachedObject(pathname)
	if err != nil {
		return nil, nil
	}

	if cachedObject.Info.Mode != fi.Mode || cachedObject.Info.Dev != fi.Dev || cachedObject.Info.Size != fi.Size || cachedObject.Info.ModTime != fi.ModTime {
		return nil, nil
	}

	object := Object{}
	object.Checksum = cachedObject.Checksum
	object.Chunks = make([][32]byte, 0)
	for _, chunk := range cachedObject.Chunks {
		object.Chunks = append(object.Chunks, chunk.Checksum)
	}
	object.ContentType = cachedObject.ContentType

	for offset, _ := range object.Chunks {
		chunk := cachedObject.Chunks[offset]
		exists, err := snapshot.CheckChunk(chunk.Checksum)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}
		snapshot.Index.AddChunk(cachedObject.Chunks[offset])
	}
	return &object, nil
}

func chunkify(chunkerOptions *fastcdc.ChunkerOpts, snapshot *Snapshot, pathname string) (*Object, error) {
	rd, err := os.Open(pathname)
	if err != nil {
		return nil, err
	}
	defer rd.Close()

	object := &Object{}
	object.ContentType = mime.TypeByExtension(filepath.Ext(pathname))
	objectHash := sha256.New()

	chk, err := fastcdc.NewChunker(rd, chunkerOptions)
	if err != nil {
		return nil, err
	}

	firstChunk := true
	for {
		cdcChunk, err := chk.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if firstChunk {
			if object.ContentType == "" {
				object.ContentType = mimetype.Detect(cdcChunk.Data).String()
			}
			firstChunk = false
		}

		objectHash.Write(cdcChunk.Data)

		chunkHash := sha256.New()
		chunkHash.Write(cdcChunk.Data)

		var t32 [32]byte
		copy(t32[:], chunkHash.Sum(nil))

		chunk := Chunk{}
		chunk.Checksum = t32
		chunk.Start = uint(cdcChunk.Offset)
		chunk.Length = uint(cdcChunk.Size)
		object.Chunks = append(object.Chunks, chunk.Checksum)

		indexChunk := snapshot.Index.LookupChunk(chunk.Checksum)
		if indexChunk == nil {
			exists, err := snapshot.CheckChunk(chunk.Checksum)
			if err != nil {
				return nil, err
			}

			if !exists {
				nbytes, err := snapshot.PutChunk(chunk.Checksum, cdcChunk.Data)
				if err != nil {
					return nil, err
				}
				fmt.Println("### chunksize: ", nbytes)
			}
			snapshot.Index.AddChunk(&chunk)
		}
	}
	var t32 [32]byte
	copy(t32[:], objectHash.Sum(nil))
	object.Checksum = t32

	return object, nil
}

func (snapshot *Snapshot) Push(scanDirs []string) error {
	chunkerOptions := fastcdc.NewChunkerOptions()

	maxConcurrency := make(chan bool, runtime.NumCPU()*8+1)
	wg := sync.WaitGroup{}

	t0 := time.Now()

	cache := snapshot.repository.Cache

	snapshot.Metadata.ScannedDirectories = make([]string, 0)
	for _, scanDir := range scanDirs {
		scanDir, err := filepath.Abs(scanDir)
		if err != nil {
			logger.Warn("%s", err)
			return err
		}
		snapshot.Metadata.ScannedDirectories = append(snapshot.Metadata.ScannedDirectories, scanDir)
		err = snapshot.Filesystem.Scan(scanDir, snapshot.SkipDirs)
		if err != nil {
			logger.Warn("%s", err)
		}
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
			atomic.AddUint64(&snapshot.Metadata.ScanSize, uint64(fileinfo.Size))

			var object *Object
			object, err := pathnameCached(snapshot, *fileinfo, _filename)
			if err != nil {
				// something went wrong with the cache
				// errchan <- err
			}

			if object != nil {
				exists, err = snapshot.CheckObject(object.Checksum)
				if err != nil {
					logger.Warn("%s: failed to check object existence: %s", _filename, err)
					return
				}
			}

			// can't reuse object from cache, chunkify
			if object == nil || !exists {
				object, err = chunkify(chunkerOptions, snapshot, _filename)
				if err != nil {
					logger.Warn("%s: could not chunkify: %s", _filename, err)
					return
				}
				if cache != nil {
					snapshot.PutCachedObject(_filename, *object, *fileinfo)
				}

				exists, err = snapshot.CheckObject(object.Checksum)
				if err != nil {
					logger.Warn("%s: failed to check object existence: %s", _filename, err)
					return
				}
				if !exists {
					_, err = snapshot.PutObject(object)
					if err != nil {
						logger.Warn("%s: failed to store object: %s", _filename, err)
						return
					}
				}
			}

			snapshot.Index.AddObject(object)
			snapshot.Index.LinkPathnameToObject(_filename, object)
			atomic.AddUint64(&snapshot.Metadata.ScanProcessedSize, uint64(fileinfo.Size))
		}(filename)
	}
	wg.Wait()

	snapshot.Metadata.ChunksCount = uint64(len(snapshot.Index.ListChunks()))
	snapshot.Metadata.ObjectsCount = uint64(len(snapshot.Index.ListObjects()))
	snapshot.Metadata.FilesCount = uint64(len(snapshot.Filesystem.ListFiles()))
	snapshot.Metadata.DirectoriesCount = uint64(len(snapshot.Filesystem.ListDirectories()))

	for _, key := range snapshot.Index.ListContentTypes() {
		objectType := strings.Split(key, ";")[0]
		objectKind := strings.Split(key, "/")[0]
		if objectType == "" {
			objectType = "unknown"
			objectKind = "unknown"
		}
		if _, exists := snapshot.Metadata.Statistics.Kind[objectKind]; !exists {
			snapshot.Metadata.Statistics.Kind[objectKind] = 0
		}
		snapshot.Metadata.Statistics.Kind[objectKind] += uint64(len(snapshot.Index.LookupObjectsForContentType(key)))

		if _, exists := snapshot.Metadata.Statistics.Type[objectType]; !exists {
			snapshot.Metadata.Statistics.Type[objectType] = 0
		}
		snapshot.Metadata.Statistics.Type[objectType] += uint64(len(snapshot.Index.LookupObjectsForContentType(key)))
	}

	for _, key := range snapshot.Index.ListPathnames() {
		extension := strings.ToLower(filepath.Ext(key))
		if extension == "" {
			extension = "none"
		}
		if _, exists := snapshot.Metadata.Statistics.Extension[extension]; !exists {
			snapshot.Metadata.Statistics.Extension[extension] = 0
		}
		snapshot.Metadata.Statistics.Extension[extension]++
	}

	for key, value := range snapshot.Metadata.Statistics.Type {
		snapshot.Metadata.Statistics.PercentType[key] = math.Round((float64(value)/float64(snapshot.Metadata.FilesCount)*100)*100) / 100
	}
	for key, value := range snapshot.Metadata.Statistics.Kind {
		snapshot.Metadata.Statistics.PercentKind[key] = math.Round((float64(value)/float64(snapshot.Metadata.FilesCount)*100)*100) / 100
	}
	for key, value := range snapshot.Metadata.Statistics.Extension {
		snapshot.Metadata.Statistics.PercentExtension[key] = math.Round((float64(value)/float64(snapshot.Metadata.FilesCount)*100)*100) / 100
	}

	snapshot.Metadata.NonRegularCount = uint64(len(snapshot.Filesystem.ListNonRegular()))
	snapshot.Metadata.PathnamesCount = uint64(len(snapshot.Index.ListPathnames()))

	snapshot.Metadata.CreationDuration = time.Since(t0)

	err := snapshot.Commit()
	if err != nil {
		logger.Warn("could not commit snapshot: %s", err)
	}

	return err
}
