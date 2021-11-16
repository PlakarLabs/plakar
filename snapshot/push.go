package snapshot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/gabriel-vasile/mimetype"
	"github.com/poolpOrg/plakar/logger"
	"github.com/restic/chunker"
)

type chunkMsg struct {
	Chunk *Chunk
	Data  []byte
}

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

func pushChunkWriterChannelHandler(snapshot *Snapshot) (chan chunkMsg, func()) {
	maxGoroutines := make(chan bool, 1024)

	c := make(chan chunkMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			maxGoroutines <- true
			wg.Add(1)
			go func(chunk *Chunk, data []byte) {
				err := snapshot.PutChunk(chunk.Checksum, data)
				if err != nil {
					// errchan <- err
				}
				wg.Done()
				<-maxGoroutines
			}(msg.Chunk, msg.Data)
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
				for _, chunk := range object.Chunks {
					snapshot.StateSetChunkToObject(chunk.Checksum, object.Checksum)
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

func pushChunkChannelHandler(snapshot *Snapshot, chanChunkWriter chan chunkMsg) (chan chunkMsg, func()) {
	maxGoroutines := make(chan bool, 1024)

	c := make(chan chunkMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			maxGoroutines <- true
			wg.Add(1)
			go func(chunk *Chunk, data []byte) {
				var ok bool
				if _, ok = snapshot.StateGetChunk(chunk.Checksum); !ok {
					snapshot.StateSetChunk(chunk.Checksum, chunk)
				}
				if !ok {
					if len(data) != 0 {
						chanChunkWriter <- chunkMsg{chunk, data}
					}
				}
				wg.Done()
				<-maxGoroutines
			}(msg.Chunk, msg.Data)
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

func pushChunksProcessorChannelHandler(snapshot *Snapshot) (chan *Object, func()) {
	chanChunkWriter, chanChunkWriterDone := pushChunkWriterChannelHandler(snapshot)
	chanChunk, chanChunkDone := pushChunkChannelHandler(snapshot, chanChunkWriter)

	maxGoroutines := make(chan bool, 1024)

	c := make(chan *Object)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			maxGoroutines <- true
			wg.Add(1)
			go func(object *Object) {
				chunks := make([]string, 0)
				for _, chunk := range object.Chunks {
					chunks = append(chunks, chunk.Checksum)
				}

				res, err := snapshot.ReferenceChunks(chunks)
				if err != nil {
					//					errchan <- err
					return
				}
				for i, exists := range res {
					chunk := object.Chunks[i]
					if exists {
						chanChunk <- chunkMsg{chunk, []byte("")}
					} else {
						object.fp.Seek(int64(chunk.Start), 0)

						chunkData := make([]byte, chunk.Length)
						n, err := object.fp.Read(chunkData)
						if err != nil || n != int(chunk.Length) {
							if err != nil {
								//errchan <- err
							}
							break
						}
						chanChunk <- chunkMsg{chunk, chunkData}
					}
					atomic.AddUint64(&snapshot.Size, uint64(chunk.Length))
				}
				object.fp.Close()
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
		chanChunkDone()
		chanChunkWriterDone()
	}
}

func pathnameCached(snapshot *Snapshot, fi Fileinfo, pathname string) (*Object, error) {
	cache := snapshot.store.GetCache()
	cache = nil
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
	object.path = pathname
	object.Checksum = cachedObject.Checksum
	object.Chunks = cachedObject.Chunks
	object.ContentType = cachedObject.ContentType
	return &object, nil
}

func chunkify(bufPool sync.Pool, pathname string) (*Object, error) {
	rd, err := os.Open(pathname)
	if err != nil {
		logger.Warn("%s", err)
		return nil, err
	}
	//defer rd.Close()

	object := &Object{}
	object.fp = rd
	objectHash := sha256.New()

	chk := chunker.New(rd, 0x3dea92648f6e83)
	buf := bufPool.Get().(*[]byte)
	firstChunk := true
	for {
		cdcChunk, err := chk.Next(*buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Warn("%s", err)
			return nil, err
		}
		if firstChunk {
			object.ContentType = mimetype.Detect(cdcChunk.Data).String()
			firstChunk = false
		}

		objectHash.Write(cdcChunk.Data)

		chunkHash := sha256.New()
		chunkHash.Write(cdcChunk.Data)

		chunk := Chunk{}
		chunk.Checksum = fmt.Sprintf("%032x", chunkHash.Sum(nil))
		chunk.Start = cdcChunk.Start
		chunk.Length = cdcChunk.Length
		object.Chunks = append(object.Chunks, &chunk)
	}
	bufPool.Put(buf)
	object.Checksum = fmt.Sprintf("%032x", objectHash.Sum(nil))
	return object, nil
}

func (snapshot *Snapshot) Push(root string) error {
	//cache := snapshot.store.Cache

	root, err := filepath.Abs(root)
	if err != nil {
		log.Fatal(err)
	}

	snapshot.Filesystem.Scan(root, snapshot.SkipDirs)

	chanObjectsProcessor, chanObjectsProcessorDone := pushObjectsProcessorChannelHandler(snapshot)
	chanChunksProcessor, chanChunksProcessorDone := pushChunksProcessorChannelHandler(snapshot)

	bufPool := sync.Pool{
		New: func() interface{} {
			b := make([]byte, 16*1024*1024)
			return &b
		},
	}
	maxConcurrency := make(chan bool, 8192)
	wg := sync.WaitGroup{}
	for pathname, fileinfo := range snapshot.Filesystem.Files {
		maxConcurrency <- true
		wg.Add(1)
		go func(pathname string, fileinfo *Fileinfo) {
			defer wg.Done()
			defer func() { <-maxConcurrency }()

			var object *Object

			//object, err = pathnameCached(snapshot, *fileinfo, pathname)
			//if err != nil {
			//	// something went wrong with the cache
			//}

			//DEBUG: for now, pretend there's no cache
			object = nil

			// can't reuse object from cache, chunkify
			if object == nil {
				object, err = chunkify(bufPool, pathname)
				if err != nil {
					// something went wrong, skip this file
					// errchan <- err
					return
				}
			}
			//if cache != nil {
			//	snapshot.PutCachedObject(pathname, *object, *fileinfo)
			//}

			snapshot.muFilenames.Lock()
			snapshot.Filenames[pathname] = object.Checksum
			snapshot.muFilenames.Unlock()

			snapshot.muObjects.Lock()
			snapshot.Objects[object.Checksum] = object
			snapshot.muObjects.Unlock()

			snapshot.muObjectToPathnames.Lock()
			snapshot.ObjectToPathnames[object.Checksum] = append(snapshot.ObjectToPathnames[object.Checksum], pathname)
			snapshot.muObjectToPathnames.Unlock()

			snapshot.muContentTypeToObjects.Lock()
			snapshot.ContentTypeToObjects[object.ContentType] = append(snapshot.ContentTypeToObjects[object.ContentType], object.Checksum)
			snapshot.muContentTypeToObjects.Unlock()

			chanChunksProcessor <- object

		}(pathname, fileinfo)
	}
	wg.Wait()
	chanChunksProcessorDone()

	chanObjectsProcessor <- snapshot.Objects
	chanObjectsProcessorDone()

	return nil
}
