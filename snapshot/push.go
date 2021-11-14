package snapshot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gabriel-vasile/mimetype"
	"github.com/iafan/cwalk"
	"github.com/poolpOrg/plakar/logger"
	"github.com/restic/chunker"
)

type inodeMsg struct {
	Pathname string
	Fileinfo *Fileinfo
}

type pathMsg struct {
	Pathname string
	Checksum string
}

type chunkMsg struct {
	Chunk *Chunk
	Data  []byte
}

type objectMsg struct {
	Object *Object
	Data   []byte
}

type objectPathMsg struct {
	Object   *Object
	Data     []byte
	Pathname string
}

func (snapshot *Snapshot) inodeChannelHandler() (chan inodeMsg, func()) {
	c := make(chan inodeMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			snapshot.maxConcurrentGoroutines <- true
			wg.Add(1)
			go func(pathname string, fileinfo *Fileinfo) {
				snapshot.SetInode(pathname, fileinfo)
				wg.Done()
				<-snapshot.maxConcurrentGoroutines
			}(msg.Pathname, msg.Fileinfo)
		}
		wg.Wait()
		done <- true
	}()

	return c, func() {
		close(c)
		<-done
	}
}

func (snapshot *Snapshot) pathChannelHandler() (chan pathMsg, func()) {
	c := make(chan pathMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			snapshot.maxConcurrentGoroutines <- true
			wg.Add(1)
			go func(pathname string, checksum string) {
				if _, ok := snapshot.StateGetPathname(pathname); !ok {
					snapshot.StateSetPathname(pathname, checksum)
				}
				wg.Done()
				<-snapshot.maxConcurrentGoroutines
			}(msg.Pathname, msg.Checksum)
		}
		wg.Wait()
		done <- true
	}()

	return c, func() {
		close(c)
		<-done
	}
}

func (snapshot *Snapshot) objectWriterChannelHandler() (chan objectMsg, func()) {
	c := make(chan objectMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			snapshot.maxConcurrentGoroutines <- true
			wg.Add(1)
			go func(object *Object, data []byte) {
				var ok bool
				if _, ok := snapshot.StateGetWrittenObject(object.Checksum); !ok {
					snapshot.StateSetWrittenObject(object.Checksum, false)
					snapshot.StateSetInflightObject(object.Checksum, object)
				}
				if !ok {
					err := snapshot.PutObject(object.Checksum, data)

					snapshot.StateDeleteInflightObject(object.Checksum)
					if err != nil {
						//errchan <- err
					} else {
						snapshot.StateSetWrittenObject(object.Checksum, true)
					}
				}
				wg.Done()
				<-snapshot.maxConcurrentGoroutines
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

func (snapshot *Snapshot) chunkWriterChannelHandler() (chan chunkMsg, func()) {
	c := make(chan chunkMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			snapshot.maxConcurrentGoroutines <- true
			wg.Add(1)
			go func(chunk *Chunk, data []byte) {
				var ok bool
				if _, ok := snapshot.StateGetWrittenChunk(chunk.Checksum); !ok {
					snapshot.StateSetWrittenChunk(chunk.Checksum, false)
					snapshot.StateSetInflightChunk(chunk.Checksum, chunk)
				}
				if !ok {
					err := snapshot.PutChunk(chunk.Checksum, data)

					snapshot.StateDeleteInflightChunk(chunk.Checksum)
					if err != nil {
						//						errchan <- err
					} else {
						snapshot.StateSetWrittenChunk(chunk.Checksum, true)
					}
				}
				wg.Done()
				<-snapshot.maxConcurrentGoroutines
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

func (snapshot *Snapshot) objectChannelHandler(chanObjectWriter chan objectMsg) (chan objectMsg, func()) {
	c := make(chan objectMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			snapshot.maxConcurrentGoroutines <- true
			wg.Add(1)
			go func(object *Object, data []byte) {
				var ok bool
				if _, ok = snapshot.StateGetObject(object.Checksum); !ok {
					snapshot.StateSetObject(object.Checksum, object)
					for _, chunk := range object.Chunks {
						snapshot.StateSetChunkToObject(chunk.Checksum, object.Checksum)
					}
					snapshot.StateSetObjectToPathname(object.Checksum, object.path)
					snapshot.StateSetContentTypeToObjects(object.ContentType, object.Checksum)
				}
				if !ok {
					if len(data) != 0 {
						chanObjectWriter <- objectMsg{object, data}
					}
				}
				wg.Done()
				<-snapshot.maxConcurrentGoroutines
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

func (snapshot *Snapshot) chunkChannelHandler(chanChunkWriter chan chunkMsg) (chan chunkMsg, func()) {
	c := make(chan chunkMsg)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			snapshot.maxConcurrentGoroutines <- true
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
				<-snapshot.maxConcurrentGoroutines
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

func (snapshot *Snapshot) objectsProcessorChannelHandler() (chan map[string]*Object, func()) {
	chanObjectWriter, chanObjectWriterDone := snapshot.objectWriterChannelHandler()
	chanObject, chanObjectDone := snapshot.objectChannelHandler(chanObjectWriter)

	c := make(chan map[string]*Object)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			snapshot.maxConcurrentGoroutines <- true
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
				<-snapshot.maxConcurrentGoroutines
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

func (snapshot *Snapshot) chunksProcessorChannelHandler() (chan *Object, func()) {
	chanChunkWriter, chanChunkWriterDone := snapshot.chunkWriterChannelHandler()
	chanChunk, chanChunkDone := snapshot.chunkChannelHandler(chanChunkWriter)

	c := make(chan *Object)
	done := make(chan bool)
	var wg sync.WaitGroup

	go func() {
		for msg := range c {
			snapshot.maxConcurrentGoroutines <- true
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
				<-snapshot.maxConcurrentGoroutines
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

func (snapshot *Snapshot) Push(root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		log.Fatal(err)
	}

	snapshot.StateAddRoot(root)

	cache := snapshot.store.GetCache()

	chanInode, chanInodeDone := snapshot.inodeChannelHandler()
	chanPath, chanPathDone := snapshot.pathChannelHandler()
	chanObjectsProcessor, chanObjectsProcessorDone := snapshot.objectsProcessorChannelHandler()
	chanChunksProcessor, chanChunksProcessorDone := snapshot.chunksProcessorChannelHandler()

	objectsMutex := sync.Mutex{}
	objects := make(map[string]*Object)

	atoms := strings.Split(root, "/")
	for i := len(atoms) - 1; i != 0; i-- {
		path := filepath.Clean(fmt.Sprintf("/%s", strings.Join(atoms[0:i], "/")))
		f, err := os.Stat(path)
		if err != nil {
			return err
		}

		fi := FileinfoFromStat(f)
		chanInode <- inodeMsg{path, &fi}
	}

	err = cwalk.Walk(root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			logger.Warn("%s", err)
			return nil
		}

		for _, skipPath := range snapshot.SkipDirs {
			if strings.HasPrefix(fmt.Sprintf("%s/%s", root, path), skipPath) {
				return nil
			}
		}

		fi := FileinfoFromStat(f)

		pathname := filepath.Clean(fmt.Sprintf("%s/%s", root, path))

		if f.Mode().IsRegular() {

			if cache != nil {
				cachedObject, err := snapshot.GetCachedObject(pathname)
				if err == nil {
					if cachedObject.Info.Mode == fi.Mode && cachedObject.Info.Dev == fi.Dev && cachedObject.Info.Size == fi.Size && cachedObject.Info.ModTime == fi.ModTime {
						chunks := make([]string, 0)
						for _, chunk := range cachedObject.Chunks {
							chunks = append(chunks, chunk.Checksum)
						}

						res, err := snapshot.ReferenceChunks(chunks)
						if err != nil {
							logger.Warn("%s", err)
						} else {
							notExistsCount := 0
							for _, exists := range res {
								if !exists {
									notExistsCount++
									break
								}
							}

							if notExistsCount == 0 {
								object := Object{}
								object.path = pathname
								object.Checksum = cachedObject.Checksum
								object.Chunks = cachedObject.Chunks
								object.ContentType = cachedObject.ContentType

								chanInode <- inodeMsg{pathname, &cachedObject.Info}

								chanChunksProcessor <- &object

								objectsMutex.Lock()
								objects[object.Checksum] = &object
								objectsMutex.Unlock()

								chanPath <- pathMsg{object.path, object.Checksum}

								return nil
							}
						}
					}
				}
			}

			rd, err := os.Open(pathname)
			if err != nil {
				logger.Warn("%s", err)
				return nil
			}

			object := Object{}
			object.fp = rd
			objectHash := sha256.New()

			chk := chunker.New(rd, 0x3dea92648f6e83)
			buf := make([]byte, 16*1024*1024)
			firstChunk := true
			for {
				cdcChunk, err := chk.Next(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					logger.Warn("%s", err)
					return nil
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

			object.Checksum = fmt.Sprintf("%032x", objectHash.Sum(nil))

			chanChunksProcessor <- &object

			objectsMutex.Lock()
			objects[object.Checksum] = &object
			objectsMutex.Unlock()

			chanPath <- pathMsg{pathname, object.Checksum}

			if cache != nil {
				snapshot.PutCachedObject(pathname, object, fi)
			}
		}
		chanInode <- inodeMsg{pathname, &fi}
		return nil
	})
	if err != nil {
		logger.Warn("%s", err)
	}

	// no more inodes to discover
	chanInodeDone()

	// no more chunks to discover
	chanChunksProcessorDone()

	// process objects
	chanObjectsProcessor <- objects

	// no more objects to discover
	chanObjectsProcessorDone()

	// no more paths to discover
	chanPathDone()

	// ... and we're done
	return nil
}
