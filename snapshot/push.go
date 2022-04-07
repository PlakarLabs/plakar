package snapshot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gabriel-vasile/mimetype"
	"github.com/poolpOrg/go-fastcdc"
	"github.com/poolpOrg/plakar/filesystem"
	"github.com/poolpOrg/plakar/logger"
)

type objectMsg struct {
	Object *Object
	Data   []byte
}

func pushObjectWriterChannelHandler(snapshot *Snapshot) (chan objectMsg, func()) {
	maxGoroutines := make(chan bool, 1024)

	c := make(chan objectMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			maxGoroutines <- true
			wg.Add(1)
			go func(object *Object, data []byte) {
				err := snapshot.PutObject(object.Checksum, data)
				if err != nil {
					//errchan <- err
				}
				wg.Done()
				<-maxGoroutines
			}(msg.Object, msg.Data)
		}
		wg.Wait()
		done <- true
	}()

	return c, func() {
		close(c)
		<-done
	}
}

func pushObjectChannelHandler(snapshot *Snapshot, chanObjectWriter chan objectMsg) (chan objectMsg, func()) {
	maxGoroutines := make(chan bool, 1024)

	c := make(chan objectMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			maxGoroutines <- true
			wg.Add(1)
			go func(object *Object, data []byte) {
				for _, chunkChecksum := range object.Chunks {
					snapshot.StateSetChunkToObject(chunkChecksum, object.Checksum)
				}
				if len(data) != 0 {
					chanObjectWriter <- objectMsg{object, data}
				}
				wg.Done()
				<-maxGoroutines
			}(msg.Object, msg.Data)
		}
		wg.Wait()
		done <- true
	}()

	return c, func() {
		close(c)
		<-done
	}
}

func pushObjectsProcessorChannelHandler(snapshot *Snapshot) (chan map[string]*Object, func()) {
	chanObjectWriter, chanObjectWriterDone := pushObjectWriterChannelHandler(snapshot)
	chanObject, chanObjectDone := pushObjectChannelHandler(snapshot, chanObjectWriter)

	maxGoroutines := make(chan bool, 1024)

	c := make(chan map[string]*Object)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			maxGoroutines <- true
			wg.Add(1)
			go func(objects map[string]*Object) {
				checkPathnames := make([]string, 0)
				for checksum := range objects {
					checkPathnames = append(checkPathnames, checksum)
				}

				res, err := snapshot.ReferenceObjects(checkPathnames)
				if err != nil {
					logger.Warn("%s", err)
				}
				for i, exists := range res {
					object := objects[checkPathnames[i]]
					if exists {
						chanObject <- struct {
							Object *Object
							Data   []byte
						}{object, []byte("")}
					} else {
						objectData, err := json.Marshal(object)
						if err != nil {
							logger.Warn("%s", err)
							break
						}

						chanObject <- objectMsg{object, objectData}
					}
				}
				wg.Done()
				<-maxGoroutines
			}(msg)
		}
		wg.Wait()
		done <- true
	}()

	return c, func() {
		close(c)
		<-done
		chanObjectDone()
		chanObjectWriterDone()
	}
}

func pathnameCached(snapshot *Snapshot, fi filesystem.Fileinfo, pathname string) (*Object, error) {
	cache := snapshot.store.GetCache()

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

	chunks := make([]string, 0)
	for _, chunk := range cachedObject.Chunks {
		chunks = append(chunks, chunk.Checksum)
	}

	res, err := snapshot.ReferenceChunks(chunks)
	if err != nil {
		return nil, err
	}

	notExistsCount := 0
	for _, exists := range res {
		if !exists {
			notExistsCount++
			return nil, nil
		}
	}

	object := Object{}
	object.Checksum = cachedObject.Checksum
	object.Chunks = make([]string, 0)
	for _, chunk := range cachedObject.Chunks {
		object.Chunks = append(object.Chunks, chunk.Checksum)
	}
	object.ContentType = cachedObject.ContentType

	for offset, chunkChecksum := range object.Chunks {
		snapshot.Index.muChunks.Lock()
		snapshot.Index.Chunks[chunkChecksum] = cachedObject.Chunks[offset]
		snapshot.Index.muChunks.Unlock()
	}

	return &object, nil
}

func chunkify(chunkerOptions *fastcdc.ChunkerOpts, snapshot *Snapshot, pathname string) (*Object, error) {
	rd, err := os.Open(pathname)
	if err != nil {
		logger.Warn("%s", err)
		return nil, err
	}
	defer rd.Close()

	object := &Object{}
	objectHash := sha256.New()

	chk, err := fastcdc.NewChunker(rd, chunkerOptions)
	if err != nil {
		logger.Warn("%s", err)
		return nil, err
	}

	firstChunk := true
	for {
		cdcChunk, err := chk.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Warn("%s", err)
			return nil, err
		}
		if firstChunk {
			object.ContentType = mime.TypeByExtension(filepath.Ext(pathname))
			if object.ContentType == "" {
				object.ContentType = mimetype.Detect(cdcChunk.Data).String()
			}
			firstChunk = false
		}

		objectHash.Write(cdcChunk.Data)

		chunkHash := sha256.New()
		chunkHash.Write(cdcChunk.Data)

		chunk := Chunk{}
		chunk.Checksum = fmt.Sprintf("%032x", chunkHash.Sum(nil))
		chunk.Start = uint(cdcChunk.Offset)
		chunk.Length = uint(cdcChunk.Size)
		object.Chunks = append(object.Chunks, chunk.Checksum)

		chunks := make([]string, 0)
		chunks = append(chunks, chunk.Checksum)

		// XXX - we can reduce the number of ReferenceChunks calls
		// by grouping chunks but let's do that later when everything
		// is already working

		res, err := snapshot.ReferenceChunks(chunks)
		if err != nil {
			return nil, err
		}
		if !res[0] {
			snapshot.Index.muChunks.Lock()
			if _, ok := snapshot.Index.Chunks[chunk.Checksum]; !ok {
				err = snapshot.PutChunk(chunk.Checksum, cdcChunk.Data)
				if err == nil {
					snapshot.Index.Chunks[chunk.Checksum] = &chunk
				}
			}
			snapshot.Index.muChunks.Unlock()
			if err != nil {
				return nil, err
			}
		} else {
			snapshot.Index.muChunks.Lock()
			snapshot.Index.Chunks[chunk.Checksum] = &chunk
			snapshot.Index.muChunks.Unlock()
		}
	}
	object.Checksum = fmt.Sprintf("%032x", objectHash.Sum(nil))
	return object, nil
}

func (snapshot *Snapshot) Push(scanDirs []string) error {
	cache := snapshot.store.Cache

	for _, scanDir := range scanDirs {
		scanDir, err := filepath.Abs(scanDir)
		if err != nil {
			return err
		}
		err = snapshot.Index.Filesystem.Scan(scanDir, snapshot.SkipDirs)
		if err != nil {
			//errchan<-err
		}
	}

	chanObjectsProcessor, chanObjectsProcessorDone := pushObjectsProcessorChannelHandler(snapshot)

	chunkerOptions := fastcdc.NewChunkerOptions()

	//bufPool := &sync.Pool{
	//	New: func() interface{} {
	//		b := make([]byte, chunkerOptions.MaxSize)
	//		return &b
	//	},
	//}
	//chunkerOptions.BufferAllocate = func() *[]byte {
	//	return bufPool.Get().(*[]byte)
	//}
	//chunkerOptions.BufferRelease = func(buffer *[]byte) {
	//	bufPool.Put(buffer)
	//}

	maxConcurrency := make(chan bool, 1024)
	wg := sync.WaitGroup{}
	for _, pathname := range snapshot.Index.Filesystem.ListFiles() {
		fileinfo, _ := snapshot.Index.Filesystem.LookupInodeForFile(pathname)
		maxConcurrency <- true
		wg.Add(1)
		go func(pathname string, fileinfo *filesystem.Fileinfo) {
			defer wg.Done()
			defer func() { <-maxConcurrency }()

			var object *Object

			// XXX - later optim: if fileinfo.Dev && fileinfo.Ino already exist in this snapshot
			// lookup object from snapshot and bypass scanning

			object, err := pathnameCached(snapshot, *fileinfo, pathname)
			if err != nil {
				// something went wrong with the cache
				// errchan <- err
			}

			// can't reuse object from cache, chunkify
			if object == nil {
				object, err = chunkify(chunkerOptions, snapshot, pathname)
				if err != nil {
					// something went wrong, skip this file
					// errchan <- err
					return
				}
				if cache != nil {
					snapshot.PutCachedObject(pathname, *object, *fileinfo)
				}
			}

			snapshot.Index.muPathnames.Lock()
			snapshot.Index.Pathnames[pathname] = object.Checksum
			snapshot.Index.muPathnames.Unlock()

			snapshot.Index.muObjects.Lock()
			snapshot.Index.Objects[object.Checksum] = object
			snapshot.Index.muObjects.Unlock()

			snapshot.Index.muObjectToPathnames.Lock()
			snapshot.Index.ObjectToPathnames[object.Checksum] = append(snapshot.Index.ObjectToPathnames[object.Checksum], pathname)
			snapshot.Index.muObjectToPathnames.Unlock()

			snapshot.Index.muContentTypeToObjects.Lock()
			snapshot.Index.ContentTypeToObjects[object.ContentType] = append(snapshot.Index.ContentTypeToObjects[object.ContentType], object.Checksum)
			snapshot.Index.muContentTypeToObjects.Unlock()

			atomic.AddUint64(&snapshot.Metadata.Size, uint64(fileinfo.Size))

		}(pathname, fileinfo)
	}
	wg.Wait()

	chanObjectsProcessor <- snapshot.Index.Objects
	chanObjectsProcessorDone()

	// compute some more metadata
	snapshot.Metadata.Statistics.Chunks = uint64(len(snapshot.Index.Chunks))
	snapshot.Metadata.Statistics.Objects = uint64(len(snapshot.Index.Objects))
	snapshot.Metadata.Statistics.Files = uint64(len(snapshot.Index.Filesystem.Files))
	snapshot.Metadata.Statistics.Directories = uint64(len(snapshot.Index.Filesystem.Directories))

	for key, value := range snapshot.Index.ContentTypeToObjects {
		objectType := strings.Split(key, ";")[0]
		objectKind := strings.Split(key, "/")[0]
		if objectType == "" {
			objectType = "unknown"
			objectKind = "unknown"
		}
		for _ = range value {
			if _, exists := snapshot.Metadata.Statistics.Kind[objectKind]; !exists {
				snapshot.Metadata.Statistics.Kind[objectKind] = 0
			}
			snapshot.Metadata.Statistics.Kind[objectKind]++

			if _, exists := snapshot.Metadata.Statistics.Type[objectType]; !exists {
				snapshot.Metadata.Statistics.Type[objectType] = 0
			}
			snapshot.Metadata.Statistics.Type[objectType]++
		}
	}

	for key := range snapshot.Index.Pathnames {
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
		snapshot.Metadata.Statistics.PercentType[key] = math.Round((float64(value)/float64(snapshot.Metadata.Statistics.Files)*100)*100) / 100
	}
	for key, value := range snapshot.Metadata.Statistics.Kind {
		snapshot.Metadata.Statistics.PercentKind[key] = math.Round((float64(value)/float64(snapshot.Metadata.Statistics.Files)*100)*100) / 100
	}
	for key, value := range snapshot.Metadata.Statistics.Extension {
		snapshot.Metadata.Statistics.PercentExtension[key] = math.Round((float64(value)/float64(snapshot.Metadata.Statistics.Files)*100)*100) / 100
	}

	snapshot.Metadata.ScannedDirectories = snapshot.Index.Filesystem.ScannedDirectories

	return snapshot.Commit()
}
