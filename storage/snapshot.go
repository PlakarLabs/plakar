/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package storage

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"

	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/encryption"

	"github.com/gabriel-vasile/mimetype"
	"github.com/iafan/cwalk"
	"github.com/restic/chunker"
)

func SnapshotToSummary(snapshot *Snapshot) *SnapshotSummary {
	ss := &SnapshotSummary{}
	ss.Uuid = snapshot.Uuid
	ss.CreationTime = snapshot.CreationTime
	ss.Version = snapshot.Version
	ss.Hostname = snapshot.Hostname
	ss.Username = snapshot.Username
	ss.Directories = uint64(len(snapshot.Directories))
	ss.Files = uint64(len(snapshot.Files))
	ss.NonRegular = uint64(len(snapshot.NonRegular))
	ss.Sums = uint64(len(snapshot.Sums))
	ss.Objects = uint64(len(snapshot.Objects))
	ss.Chunks = uint64(len(snapshot.Chunks))
	ss.Size = snapshot.Size
	return ss
}

func (snapshot *Snapshot) FromBuffer(store Store, data []byte) (*Snapshot, error) {
	if store.Configuration().Encrypted != "" {
		keypair := store.Context().Keypair
		tmp, err := encryption.Decrypt(keypair.MasterKey, data)
		if err != nil {
			return nil, err
		}
		data = tmp
	}

	data, err := compression.Inflate(data)
	if err != nil {
		return nil, err
	}
	var snapshotStorage SnapshotStorage

	if err := json.Unmarshal(data, &snapshotStorage); err != nil {
		return nil, err
	}

	snapshot.Uuid = snapshotStorage.Uuid
	snapshot.CreationTime = snapshotStorage.CreationTime
	snapshot.Version = snapshotStorage.Version
	snapshot.Hostname = snapshotStorage.Hostname
	snapshot.Username = snapshotStorage.Username
	snapshot.Directories = snapshotStorage.Directories
	snapshot.Files = snapshotStorage.Files
	snapshot.NonRegular = snapshotStorage.NonRegular
	snapshot.Sums = snapshotStorage.Sums
	snapshot.Objects = snapshotStorage.Objects
	snapshot.Chunks = snapshotStorage.Chunks
	snapshot.Size = snapshotStorage.Size
	snapshot.BackingStore = store
	return snapshot, nil
}

func (snapshot *Snapshot) Pull(root string, pattern string) {
	keypair := snapshot.BackingStore.Context().Keypair

	errchan := snapshot.BackingStore.Context().StderrChannel

	var dest string

	dpattern := path.Clean(pattern)
	fpattern := path.Clean(pattern)

	/* if at root, pretend there's no pattern */
	if dpattern == "/" || dpattern == "." {
		dpattern = ""
		fpattern = ""
	}

	/* if pattern is a file, we rebase dpattern to parent */
	if _, ok := snapshot.Files[fpattern]; ok {
		tmp := strings.Split(dpattern, "/")
		if len(tmp) > 1 {
			dpattern = strings.Join(tmp[:len(tmp)-1], "/")
		}
	}

	for directory, fi := range snapshot.Directories {
		if directory != dpattern &&
			!strings.HasPrefix(directory, fmt.Sprintf("%s/", dpattern)) {
			continue
		}
		dest = fmt.Sprintf("%s/%s", root, directory)
		os.MkdirAll(dest, 0700)
		os.Chmod(dest, fi.Mode)
		os.Chown(dest, int(fi.Uid), int(fi.Gid))
	}

	for file, fi := range snapshot.Files {
		if file != fpattern &&
			!strings.HasPrefix(file, fmt.Sprintf("%s/", fpattern)) {
			continue
		}

		dest = fmt.Sprintf("%s/%s", root, file)

		checksum := snapshot.Sums[file]

		f, err := os.Create(dest)
		if err != nil {
			errchan <- err.Error()
			continue
		}

		data, err := snapshot.BackingStore.ObjectGet(checksum)
		if err != nil {
			errchan <- err.Error()
			continue
		}

		if snapshot.BackingStore.Configuration().Encrypted != "" {
			tmp, err := encryption.Decrypt(keypair.MasterKey, data)
			if err != nil {
				errchan <- err.Error()
				continue
			}
			data = tmp
		}

		data, err = compression.Inflate(data)
		if err != nil {
			errchan <- err.Error()
			continue
		}

		object := Object{}
		err = json.Unmarshal(data, &object)
		if err != nil {
			errchan <- err.Error()
			f.Close()
			continue
		}

		objectHash := sha256.New()
		for _, chunk := range object.Chunks {
			data, err := snapshot.ChunkGet(chunk.Checksum)
			if err != nil {
				errchan <- err.Error()
				continue
			}

			if len(data) != int(chunk.Length) {
				errchan <- errors.New("chunk length mismatches with record")
				continue
			} else {
				chunkHash := sha256.New()
				chunkHash.Write(data)
				if chunk.Checksum != fmt.Sprintf("%032x", chunkHash.Sum(nil)) {
					errchan <- errors.New("chunk checksum mismatches with record")
					continue
				}
			}

			objectHash.Write(data)
			f.Write(data)
		}
		if object.Checksum != fmt.Sprintf("%032x", objectHash.Sum(nil)) {
			errchan <- errors.New("object checksum mismatches with record")
		}

		f.Close()
		os.Chmod(dest, fi.Mode)
		os.Chown(dest, int(fi.Uid), int(fi.Gid))
	}
}

func (snapshot *Snapshot) Push(root string) {
	cache := snapshot.BackingStore.Context().Cache
	outchan := snapshot.BackingStore.Context().StdoutChannel
	errchan := snapshot.BackingStore.Context().StderrChannel

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
		mu := sync.Mutex{}
		var wg sync.WaitGroup
		for msg := range chanInode {
			chanInodeMax <- 1
			wg.Add(1)
			go func(msg *FileInfo) {
				if msg.Mode.IsDir() {
					mu.Lock()
					snapshot.Directories[msg.path] = msg
					mu.Unlock()
				} else if msg.Mode.IsRegular() {
					mu.Lock()
					snapshot.Files[msg.path] = msg
					mu.Unlock()
				} else {
					mu.Lock()
					snapshot.NonRegular[msg.path] = msg
					mu.Unlock()
				}
				wg.Done()
				<-chanInodeMax
			}(msg)
		}
		wg.Wait()
		chanInodeDone <- true
	}()

	go func() {
		mu := sync.Mutex{}
		var wg sync.WaitGroup
		for msg := range chanPath {
			chanPathMax <- 1
			wg.Add(1)
			go func(msg struct {
				Pathname string
				Checksum string
			}) {
				mu.Lock()
				if _, ok := snapshot.Sums[msg.Pathname]; !ok {
					snapshot.Sums[msg.Pathname] = msg.Checksum
				}
				mu.Unlock()
				wg.Done()
				<-chanPathMax
			}(msg)
		}
		wg.Wait()
		chanPathDone <- true
	}()

	go func() {
		mu := sync.Mutex{}
		var wg sync.WaitGroup
		for msg := range chanObject {
			chanObjectMax <- 1
			wg.Add(1)
			go func(msg struct {
				Object *Object
				Data   []byte
			}) {
				var ok bool
				mu.Lock()
				if _, ok = snapshot.Objects[msg.Object.Checksum]; !ok {
					snapshot.Objects[msg.Object.Checksum] = msg.Object
				}
				mu.Unlock()
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
		mu := sync.Mutex{}
		var wg sync.WaitGroup
		for msg := range chanChunk {
			chanChunkMax <- 1
			wg.Add(1)
			go func(msg struct {
				Chunk *Chunk
				Data  []byte
			}) {
				var ok bool
				mu.Lock()
				if _, ok := snapshot.Chunks[msg.Chunk.Checksum]; !ok {
					snapshot.Chunks[msg.Chunk.Checksum] = msg.Chunk
				}
				mu.Unlock()
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
		mu := sync.Mutex{}
		var wg sync.WaitGroup
		for msg := range chanSize {
			chanSizeMax <- 1
			wg.Add(1)
			go func(msg uint64) {
				mu.Lock()
				snapshot.Size += msg
				mu.Unlock()
				wg.Done()
				<-chanSizeMax
			}(msg)
		}
		wg.Wait()
		chanSizeDone <- true
	}()

	/*
		case msg := <-chanSnapshotCachedChunk:
			if _, ok := snapshot.Chunks[msg.Checksum]; !ok {
				outchan <- fmt.Sprintf("chunk\tlink %s (cached)", msg.Checksum)
				snapshot.Chunks[msg.Checksum] = msg
			} else {
				outchan <- fmt.Sprintf("chunk\tskip %s (cached)", msg.Checksum)
			}
	*/

	// this goroutine is in charge of all chunks writes to the store
	go func() {
		mu := sync.Mutex{}
		var wg sync.WaitGroup
		for msg := range chanChunkWriter {
			chanChunkWriterMax <- 1
			wg.Add(1)
			go func(msg struct {
				Chunk *Chunk
				Data  []byte
			}) {
				var ok bool
				mu.Lock()
				if _, ok := snapshot.WrittenChunks[msg.Chunk.Checksum]; !ok {
					snapshot.WrittenChunks[msg.Chunk.Checksum] = false
					snapshot.InflightChunks[msg.Chunk.Checksum] = msg.Chunk
				}
				mu.Unlock()
				if !ok {
					outchan <- fmt.Sprintf("chunk\tpush %s", msg.Chunk.Checksum)
					err := snapshot.ChunkPut(msg.Chunk.Checksum, msg.Data)

					mu.Lock()
					delete(snapshot.InflightChunks, msg.Chunk.Checksum)
					if err != nil {
						errchan <- err
					} else {
						snapshot.WrittenChunks[msg.Chunk.Checksum] = true
					}
					mu.Unlock()
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
		mu := sync.Mutex{}
		var wg sync.WaitGroup
		for msg := range chanObjectWriter {
			chanObjectWriterMax <- 1
			wg.Add(1)
			go func(msg struct {
				Object *Object
				Data   []byte
			}) {
				var ok bool
				mu.Lock()
				if _, ok := snapshot.WrittenObjects[msg.Object.Checksum]; !ok {
					snapshot.WrittenObjects[msg.Object.Checksum] = false
					snapshot.InflightObjects[msg.Object.Checksum] = msg.Object
				}
				mu.Unlock()
				if !ok {
					outchan <- fmt.Sprintf("object\tpush %s", msg.Object.Checksum)
					err := snapshot.ObjectPut(msg.Object.Checksum, msg.Data)

					mu.Lock()
					delete(snapshot.InflightObjects, msg.Object.Checksum)
					if err != nil {
						errchan <- err
					} else {
						snapshot.WrittenObjects[msg.Object.Checksum] = true
					}
					mu.Unlock()
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
				res := snapshot.BackingTransaction.ChunksMark(chunks)
				for i, exists := range res {
					chunk := object.Chunks[i]
					if !exists {
						object.fp.Seek(int64(chunk.Start), 0)

						chunkData := make([]byte, chunk.Length)
						n, err := object.fp.Read(chunkData)
						if err != nil || n != int(chunk.Length) {
							if err != nil {
								errchan <- err
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
				checksums := make([]string, 0)
				for checksum := range objects {
					checksums = append(checksums, checksum)
				}

				res := snapshot.BackingTransaction.ObjectsMark(checksums)
				for i, exists := range res {
					object := objects[checksums[i]]
					if !exists {
						objectData, err := json.Marshal(object)
						if err != nil {
							errchan <- err
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

	cwalk.Walk(root, func(path string, f os.FileInfo, err error) error {

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
			path:    fmt.Sprintf("%s/%s", root, path),
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

						res := snapshot.BackingTransaction.ChunksMark(chunks)
						notExistsCount := 0
						for _, exists := range res {
							if !exists {
								notExistsCount++
								break
							}
						}

						if notExistsCount == 0 {
							exists := snapshot.BackingTransaction.ObjectMark(cachedObject.Checksum)
							if exists {
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
								//
								//chanObject <- struct {
								//	Object *Object
								//	Data   []byte
								//}{&object, []byte("")}

								for _, chunk := range object.Chunks {
									chanChunk <- struct {
										Chunk *Chunk
										Data  []byte
									}{chunk, []byte("")}
									chanSize <- uint64(chunk.Length)
								}

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
				errchan <- err
				return nil
			}

			object := Object{}
			object.fp = rd
			object.path = fi.path
			objectHash := sha256.New()

			chk := chunker.New(rd, 0x3dea92648f6e83)
			buf := make([]byte, 16*256*256)
			firstChunk := true
			for {
				cdcChunk, err := chk.Next(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					errchan <- err
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
	// no more inode to discover, close and wait
	close(chanInode)
	<-chanInodeDone

	// no more chunks will be processed,
	// close channel and wait for all chunks to be processed
	close(chanChunksProcessor)
	<-chanChunksProcessorDone
	outchan <- "+ done processing chunks"

	close(chanSize)
	<-chanSizeDone

	close(chanChunk)
	<-chanChunkDone

	// once all chunks are processed we won't be generating new writes,
	// close channel and wait for all chunks to be written
	close(chanChunkWriter)
	<-chanChunkWriterDone
	outchan <- "+ done writing chunks"

	// no more objects will be added,
	// send objects for processing, close channel and wait for all objects to be processed
	chanObjectsProcessor <- objects
	close(chanObjectsProcessor)
	<-chanObjectsProcessorDone
	outchan <- "+ done processing objects"

	close(chanObject)
	<-chanObjectDone

	// all objects are processed and we won't be generating new writes,
	// close channel and wait for all objects to be written
	close(chanObjectWriter)
	<-chanObjectWriterDone
	outchan <- "+ done writing objects"

	// no more paths to discover, close and wait
	close(chanPath)
	<-chanPathDone

	// ... and we're done
}

func (snapshot *Snapshot) Commit() error {
	keypair := snapshot.BackingStore.Context().Keypair
	errchan := snapshot.BackingStore.Context().StderrChannel

	snapshotStorage := SnapshotStorage{}
	snapshotStorage.Uuid = snapshot.Uuid
	snapshotStorage.CreationTime = snapshot.CreationTime
	snapshotStorage.Version = snapshot.Version
	snapshotStorage.Hostname = snapshot.Hostname
	snapshotStorage.Username = snapshot.Username
	snapshotStorage.Directories = snapshot.Directories
	snapshotStorage.Files = snapshot.Files
	snapshotStorage.NonRegular = snapshot.NonRegular
	snapshotStorage.Sums = snapshot.Sums
	snapshotStorage.Objects = snapshot.Objects
	snapshotStorage.Chunks = snapshot.Chunks
	snapshotStorage.Size = snapshot.Size

	// commit index to transaction
	jsnapshot, err := json.Marshal(snapshotStorage)
	if err != nil {
		errchan <- err.Error()
		return err
	}

	jsnapshot = compression.Deflate(jsnapshot)
	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Encrypt(keypair.MasterKey, jsnapshot)
		if err != nil {
			errchan <- err.Error()
			return err
		}
		jsnapshot = tmp
	}

	snapshot.BackingTransaction.IndexPut(string(jsnapshot))
	if snapshot.BackingStore.Context().Cache != nil {
		snapshot.BackingStore.Context().Cache.SnapshotPut(snapshot.Uuid, jsnapshot)
	}
	// commit transaction to store
	_, err = snapshot.BackingTransaction.Commit(snapshot)
	if err != nil {
		errchan <- err.Error()
		return err
	}
	return nil
}

func (snapshot *Snapshot) Purge() error {
	return snapshot.BackingStore.Purge(snapshot.Uuid)
}

func (snapshot *Snapshot) IndexGet() (*Object, error) {
	keypair := snapshot.BackingStore.Context().Keypair
	outchan := snapshot.BackingStore.Context().StdoutChannel

	outchan <- fmt.Sprintf("get index %s", snapshot.Uuid)
	data, err := snapshot.BackingStore.IndexGet(snapshot.Uuid)
	if err != nil {
		return nil, err
	}

	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Decrypt(keypair.MasterKey, data)
		if err != nil {
			return nil, err
		}
		data = tmp
	}

	data, err = compression.Inflate(data)
	if err != nil {
		return nil, err
	}

	object := &Object{}
	err = json.Unmarshal(data, &object)
	return object, err
}

func (snapshot *Snapshot) ObjectPut(checksum string, buf []byte) error {
	keypair := snapshot.BackingStore.Context().Keypair

	buf = compression.Deflate(buf)

	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buf)
		if err != nil {
			return nil
		}
		buf = tmp
	}
	return snapshot.BackingTransaction.ObjectPut(checksum, string(buf))
}

func (snapshot *Snapshot) ObjectGet(checksum string) (*Object, error) {
	keypair := snapshot.BackingStore.Context().Keypair

	data, err := snapshot.BackingStore.ObjectGet(checksum)
	if err != nil {
		return nil, err
	}

	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Decrypt(keypair.MasterKey, data)
		if err != nil {
			return nil, err
		}
		data = tmp
	}

	data, err = compression.Inflate(data)
	if err != nil {
		return nil, err
	}

	object := &Object{}
	err = json.Unmarshal(data, &object)
	return object, err
}

func (snapshot *Snapshot) ChunkPut(checksum string, buf []byte) error {
	keypair := snapshot.BackingStore.Context().Keypair

	buf = compression.Deflate(buf)

	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buf)
		if err != nil {
			return nil
		}
		buf = tmp
	}
	return snapshot.BackingTransaction.ChunkPut(checksum, string(buf))
}

func (snapshot *Snapshot) ChunkGet(checksum string) ([]byte, error) {
	keypair := snapshot.BackingStore.Context().Keypair

	data, err := snapshot.BackingStore.ChunkGet(checksum)
	if err != nil {
		return nil, err
	}

	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Decrypt(keypair.MasterKey, data)
		if err != nil {
			return nil, err
		}
		data = tmp
	}

	data, err = compression.Inflate(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (snapshot *Snapshot) GetCachedObject(pathname string) (*CachedObject, error) {
	keypair := snapshot.BackingStore.Context().Keypair
	cache := snapshot.BackingStore.Context().Cache
	errchan := snapshot.BackingStore.Context().StderrChannel

	pathHash := sha256.New()
	pathHash.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHash.Sum(nil))

	data, err := cache.PathGet(hashedPath)
	if err != nil {
		return nil, err
	}

	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Decrypt(keypair.MasterKey, data)
		if err != nil {
			errchan <- err.Error()
			return nil, err
		}
		data = tmp
	}

	data, err = compression.Inflate(data)
	if err != nil {
		errchan <- err.Error()
		return nil, err
	}

	cacheObject := CachedObject{}
	err = json.Unmarshal(data, &cacheObject)
	if err != nil {
		errchan <- err.Error()
		return nil, err
	}
	cacheObject.Info.path = pathname
	return &cacheObject, nil
}

func (snapshot *Snapshot) PutCachedObject(object Object, fi FileInfo) error {
	keypair := snapshot.BackingStore.Context().Keypair
	cache := snapshot.BackingStore.Context().Cache
	errchan := snapshot.BackingStore.Context().StderrChannel

	pathHash := sha256.New()
	pathHash.Write([]byte(object.path))
	hashedPath := fmt.Sprintf("%032x", pathHash.Sum(nil))

	cacheObject := CachedObject{}
	cacheObject.Checksum = object.Checksum
	cacheObject.Chunks = object.Chunks
	cacheObject.ContentType = object.ContentType
	cacheObject.Info = fi

	jobject, err := json.Marshal(cacheObject)
	if err != nil {
		errchan <- err
		return err
	}

	jobject = compression.Deflate(jobject)
	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Encrypt(keypair.MasterKey, jobject)
		if err != nil {
			errchan <- err
			return err
		}
		jobject = tmp
	}

	cache.PathPut(hashedPath, jobject)
	return nil
}
