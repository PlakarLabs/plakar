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
	"syscall"

	"github.com/gabriel-vasile/mimetype"
	"github.com/iafan/cwalk"
	"github.com/poolpOrg/plakar/logger"
	"github.com/restic/chunker"
)

func (snapshot *Snapshot) Push(root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		log.Fatal(err)
	}

	snapshot.StateAddRoot(root)

	cache := snapshot.store.GetCache()

	chanChunksProcessorMax := make(chan int, 64)
	chanChunksProcessor := make(chan *Object)
	chanChunksProcessorDone := make(chan bool)

	chanObjectsProcessorMax := make(chan int, 64)
	chanObjectsProcessor := make(chan map[string]*Object)
	chanObjectsProcessorDone := make(chan bool)

	chanObjectWriterMax := make(chan int, 64)
	chanObjectWriter := make(chan struct {
		Object *Object
		Data   []byte
	})
	chanObjectWriterDone := make(chan bool)

	chanChunkWriterMax := make(chan int, 64)
	chanChunkWriter := make(chan struct {
		Chunk *Chunk
		Data  []byte
	})
	chanChunkWriterDone := make(chan bool)

	chanInodeMax := make(chan int, 64)
	chanInode := make(chan *FileInfo)
	chanInodeDone := make(chan bool)

	chanPathMax := make(chan int, 64)
	chanPath := make(chan struct {
		Pathname string
		Checksum string
	})
	chanPathDone := make(chan bool)

	chanObjectMax := make(chan int, 64)
	chanObject := make(chan struct {
		Object *Object
		Data   []byte
	})
	chanObjectDone := make(chan bool)

	chanChunkMax := make(chan int, 64)
	chanChunk := make(chan struct {
		Chunk *Chunk
		Data  []byte
	})
	chanChunkDone := make(chan bool)

	chanSizeMax := make(chan int, 64)
	chanSize := make(chan uint64)
	chanSizeDone := make(chan bool)

	go func() {
		var wg sync.WaitGroup
		for msg := range chanInode {
			chanInodeMax <- 1
			wg.Add(1)
			go func(msg *FileInfo) {
				snapshot.StateSetTree(msg.path, msg)
				if msg.Mode.IsDir() {
					snapshot.StateSetDirectory(msg.path)
				} else if msg.Mode.IsRegular() {
					snapshot.StateSetFile(msg.path)
				} else {
					snapshot.StateSetNonRegular(msg.path)
				}
				wg.Done()
				<-chanInodeMax
			}(msg)
		}
		wg.Wait()
		chanInodeDone <- true
	}()

	go func() {
		var wg sync.WaitGroup
		for msg := range chanPath {
			chanPathMax <- 1
			wg.Add(1)
			go func(msg struct {
				Pathname string
				Checksum string
			}) {
				if _, ok := snapshot.StateGetPathname(msg.Pathname); !ok {
					snapshot.StateSetPathname(msg.Pathname, msg.Checksum)
				}
				wg.Done()
				<-chanPathMax
			}(msg)
		}
		wg.Wait()
		chanPathDone <- true
	}()

	go func() {
		var wg sync.WaitGroup
		for msg := range chanObject {
			chanObjectMax <- 1
			wg.Add(1)
			go func(msg struct {
				Object *Object
				Data   []byte
			}) {
				var ok bool
				if _, ok = snapshot.StateGetObject(msg.Object.Checksum); !ok {
					snapshot.StateSetObject(msg.Object.Checksum, msg.Object)
					for _, chunk := range msg.Object.Chunks {
						snapshot.StateSetChunkToObject(chunk.Checksum, msg.Object.Checksum)
					}
					snapshot.StateSetObjectToPathname(msg.Object.Checksum, msg.Object.path)
					snapshot.StateSetContentTypeToObjects(msg.Object.ContentType, msg.Object.Checksum)
				}
				if !ok {
					if len(msg.Data) != 0 {
						chanObjectWriter <- msg
					}
				}
				wg.Done()
				<-chanObjectMax
			}(msg)
		}
		wg.Wait()
		chanObjectDone <- true
	}()

	go func() {
		var wg sync.WaitGroup
		for msg := range chanChunk {
			chanChunkMax <- 1
			wg.Add(1)
			go func(msg struct {
				Chunk *Chunk
				Data  []byte
			}) {
				var ok bool
				if _, ok = snapshot.StateGetChunk(msg.Chunk.Checksum); !ok {
					snapshot.StateSetChunk(msg.Chunk.Checksum, msg.Chunk)
				}
				if !ok {
					if len(msg.Data) != 0 {
						chanChunkWriter <- msg
					}
				}
				wg.Done()
				<-chanChunkMax
			}(msg)
		}
		wg.Wait()
		chanChunkDone <- true
	}()

	go func() {
		var wg sync.WaitGroup
		for msg := range chanSize {
			chanSizeMax <- 1
			wg.Add(1)
			go func(msg uint64) {
				atomic.AddUint64(&snapshot.Size, msg)
				wg.Done()
				<-chanSizeMax
			}(msg)
		}
		wg.Wait()
		chanSizeDone <- true
	}()

	// this goroutine is in charge of all chunks writes to the store
	go func() {
		var wg sync.WaitGroup
		for msg := range chanChunkWriter {
			chanChunkWriterMax <- 1
			wg.Add(1)
			go func(msg struct {
				Chunk *Chunk
				Data  []byte
			}) {
				var ok bool
				if _, ok := snapshot.StateGetWrittenChunk(msg.Chunk.Checksum); !ok {
					snapshot.StateSetWrittenChunk(msg.Chunk.Checksum, false)
					snapshot.StateSetInflightChunk(msg.Chunk.Checksum, msg.Chunk)
				}
				if !ok {
					err := snapshot.PutChunk(msg.Chunk.Checksum, msg.Data)

					snapshot.StateDeleteInflightChunk(msg.Chunk.Checksum)
					if err != nil {
						//						errchan <- err
					} else {
						snapshot.StateSetWrittenChunk(msg.Chunk.Checksum, true)
					}
				}
				wg.Done()
				<-chanChunkWriterMax
			}(msg)
		}
		wg.Wait()
		chanChunkWriterDone <- true
	}()

	// this goroutine is in charge of all objects writes to the store
	go func() {
		var wg sync.WaitGroup
		for msg := range chanObjectWriter {
			chanObjectWriterMax <- 1
			wg.Add(1)
			go func(msg struct {
				Object *Object
				Data   []byte
			}) {
				var ok bool
				if _, ok := snapshot.StateGetWrittenObject(msg.Object.Checksum); !ok {
					snapshot.StateSetWrittenObject(msg.Object.Checksum, false)
					snapshot.StateSetInflightObject(msg.Object.Checksum, msg.Object)
				}
				if !ok {
					err := snapshot.PutObject(msg.Object.Checksum, msg.Data)

					snapshot.StateDeleteInflightObject(msg.Object.Checksum)
					if err != nil {
						//errchan <- err
					} else {
						snapshot.StateSetWrittenObject(msg.Object.Checksum, true)
					}
				}
				wg.Done()
				<-chanObjectWriterMax
			}(msg)
		}
		wg.Wait()
		chanObjectWriterDone <- true
	}()

	// this goroutine is in charge of processing all chunks
	go func() {
		var wg sync.WaitGroup
		for msg := range chanChunksProcessor {
			chanChunksProcessorMax <- 1
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
						chanChunk <- struct {
							Chunk *Chunk
							Data  []byte
						}{chunk, []byte("")}
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

						chanChunk <- struct {
							Chunk *Chunk
							Data  []byte
						}{chunk, chunkData}
					}
					chanSize <- uint64(chunk.Length)
				}
				object.fp.Close()
				wg.Done()
				<-chanChunksProcessorMax
			}(msg)
		}
		wg.Wait()
		chanChunksProcessorDone <- true
	}()

	// this goroutine is in charge of processing all objects
	go func() {
		var wg sync.WaitGroup
		for msg := range chanObjectsProcessor {
			chanObjectsProcessorMax <- 1
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

						chanObject <- struct {
							Object *Object
							Data   []byte
						}{object, objectData}
					}
				}
				wg.Done()
				<-chanObjectsProcessorMax
			}(msg)
		}
		wg.Wait()
		chanObjectsProcessorDone <- true
	}()

	objectsMutex := sync.Mutex{}
	objects := make(map[string]*Object)

	atoms := strings.Split(root, "/")
	for i := len(atoms) - 1; i != 0; i-- {
		path := filepath.Clean(fmt.Sprintf("/%s", strings.Join(atoms[0:i], "/")))
		f, err := os.Stat(path)
		if err != nil {
			return err
		}

		fi := FileInfo{
			Name:    f.Name(),
			Size:    f.Size(),
			Mode:    f.Mode(),
			ModTime: f.ModTime(),
			Dev:     uint64(f.Sys().(*syscall.Stat_t).Dev),
			Ino:     uint64(f.Sys().(*syscall.Stat_t).Ino),
			Uid:     uint64(f.Sys().(*syscall.Stat_t).Uid),
			Gid:     uint64(f.Sys().(*syscall.Stat_t).Gid),
			path:    path,
		}

		chanInode <- &fi
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

		fi := FileInfo{
			Name:    f.Name(),
			Size:    f.Size(),
			Mode:    f.Mode(),
			ModTime: f.ModTime(),
			Dev:     uint64(f.Sys().(*syscall.Stat_t).Dev),
			Ino:     uint64(f.Sys().(*syscall.Stat_t).Ino),
			Uid:     uint64(f.Sys().(*syscall.Stat_t).Uid),
			Gid:     uint64(f.Sys().(*syscall.Stat_t).Gid),
			path:    filepath.Clean(fmt.Sprintf("%s/%s", root, path)),
		}

		if f.Mode().IsRegular() {

			if cache != nil {
				cachedObject, err := snapshot.GetCachedObject(fi.path)
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
								object.path = fi.path
								object.Checksum = cachedObject.Checksum
								object.Chunks = cachedObject.Chunks
								object.ContentType = cachedObject.ContentType

								chanInode <- &cachedObject.Info

								chanChunksProcessor <- &object

								objectsMutex.Lock()
								objects[object.Checksum] = &object
								objectsMutex.Unlock()

								chanPath <- struct {
									Pathname string
									Checksum string
								}{object.path, object.Checksum}

								return nil
							}
						}
					}
				}
			}

			rd, err := os.Open(fi.path)
			if err != nil {
				logger.Warn("%s", err)
				return nil
			}

			object := Object{}
			object.fp = rd
			object.path = fi.path
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

			chanPath <- struct {
				Pathname string
				Checksum string
			}{object.path, object.Checksum}

			if cache != nil {
				snapshot.PutCachedObject(object, fi)
			}
		}
		chanInode <- &fi
		return nil
	})
	if err != nil {
		logger.Warn("%s", err)
	}

	// no more inode to discover, close and wait
	close(chanInode)
	<-chanInodeDone

	// no more chunks will be processed,
	// close channel and wait for all chunks to be processed
	close(chanChunksProcessor)
	<-chanChunksProcessorDone

	close(chanSize)
	<-chanSizeDone

	close(chanChunk)
	<-chanChunkDone

	// once all chunks are processed we won't be generating new writes,
	// close channel and wait for all chunks to be written
	close(chanChunkWriter)
	<-chanChunkWriterDone

	// no more objects will be added,
	// send objects for processing, close channel and wait for all objects to be processed
	chanObjectsProcessor <- objects
	close(chanObjectsProcessor)
	<-chanObjectsProcessorDone

	close(chanObject)
	<-chanObjectDone

	// all objects are processed and we won't be generating new writes,
	// close channel and wait for all objects to be written
	close(chanObjectWriter)
	<-chanObjectWriterDone

	// no more paths to discover, close and wait
	close(chanPath)
	<-chanPathDone

	// ... and we're done
	return nil
}
