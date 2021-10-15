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

	outchan := snapshot.BackingStore.Context().StdoutChannel
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
	outchan <- fmt.Sprintf("pull %s: OK", snapshot.Uuid)
}

func (snapshot *Snapshot) Push(root string) {
	outchan := snapshot.BackingStore.Context().StdoutChannel
	errchan := snapshot.BackingStore.Context().StderrChannel

	chanObjectWriter := make(chan struct {
		Object *Object
		Data   []byte
	})
	chanChunkWriter := make(chan struct {
		Chunk *Chunk
		Data  []byte
	})

	chanInode := make(chan *FileInfo)
	chanObject := make(chan struct {
		Object *Object
		Data   []byte
	})

	chanSnapshotChunk := make(chan struct {
		Chunk *Chunk
		Data  []byte
	})
	chanSnapshotObject := make(chan *Object)
	chanSnapshotSize := make(chan uint64)

	// this goroutine is in charge of all writes to the store
	go func() {
		for {
			select {
			case msg := <-chanChunkWriter:
				if _, ok := snapshot.WrittenChunks[msg.Chunk.Checksum]; !ok {
					snapshot.WrittenChunks[msg.Chunk.Checksum] = false
					snapshot.InflightChunks[msg.Chunk.Checksum] = msg.Chunk
					err := snapshot.ChunkPut(msg.Chunk.Checksum, msg.Data)
					delete(snapshot.InflightChunks, msg.Chunk.Checksum)
					if err != nil {
						errchan <- err
					} else {
						snapshot.WrittenChunks[msg.Chunk.Checksum] = true
					}
				}
			case msg := <-chanObjectWriter:
				if _, ok := snapshot.WrittenObjects[msg.Object.Checksum]; !ok {
					snapshot.WrittenObjects[msg.Object.Checksum] = false
					snapshot.InflightObjects[msg.Object.Checksum] = msg.Object
					err := snapshot.ObjectPut(msg.Object.Checksum, msg.Data)
					delete(snapshot.InflightObjects, msg.Object.Checksum)
					if err != nil {
						errchan <- err
					} else {
						snapshot.WrittenObjects[msg.Object.Checksum] = true
					}
				}
			}
		}
	}()

	// this goroutine is in charge of maintaining snapshot state
	go func() {
		for {
			select {
			case msg := <-chanInode:
				if msg.Mode.IsDir() {
					snapshot.Directories[msg.path] = msg
				} else if msg.Mode.IsRegular() {
					snapshot.Files[msg.path] = msg
				} else {
					snapshot.NonRegular[msg.path] = msg
				}

			case msg := <-chanSnapshotObject:
				snapshot.Objects[msg.Checksum] = msg
				snapshot.Sums[msg.path] = msg.Checksum

			case msg := <-chanSnapshotChunk:
				if _, ok := snapshot.Chunks[msg.Chunk.Checksum]; !ok {
					snapshot.Chunks[msg.Chunk.Checksum] = msg.Chunk
					chanChunkWriter <- msg
				}

			case msg := <-chanSnapshotSize:
				snapshot.Size += msg
			}
		}
	}()

	// this goroutine is in charge of deciding what needs to be written to the store
	go func() {
		for msg := range chanObject {
			go func(object *Object, objectData []byte) {
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

						chanSnapshotChunk <- struct {
							Chunk *Chunk
							Data  []byte
						}{chunk, chunkData}
					}
					chanSnapshotSize <- uint64(chunk.Length)
				}

				exists := snapshot.BackingTransaction.ObjectMark(object.Checksum)
				if !exists {
					chanObjectWriter <- struct {
						Object *Object
						Data   []byte
					}{object, objectData}
				}

				object.fp.Close()
			}(msg.Object, msg.Data)
		}
	}()

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
			buf := make([]byte, 16*1024*1024)
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
			jobject, err := json.Marshal(object)
			if err != nil {
				errchan <- err
				return nil
			}

			chanObject <- struct {
				Object *Object
				Data   []byte
			}{&object, jobject}
			chanSnapshotObject <- &object
		}
		chanInode <- &fi
		return nil
	})
	outchan <- fmt.Sprintf("push %s: OK", snapshot.Uuid)
}

func (snapshot *Snapshot) Commit() error {
	keypair := snapshot.BackingStore.Context().Keypair
	outchan := snapshot.BackingStore.Context().StdoutChannel
	errchan := snapshot.BackingStore.Context().StderrChannel

	outchan <- fmt.Sprintf("commit %s: in progress", snapshot.Uuid)
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

	// commit transaction to store
	_, err = snapshot.BackingTransaction.Commit(snapshot)
	if err != nil {
		errchan <- err.Error()
		return err
	}
	outchan <- fmt.Sprintf("commit %s: OK", snapshot.Uuid)
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
	outchan := snapshot.BackingStore.Context().StdoutChannel

	buf = compression.Deflate(buf)

	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buf)
		if err != nil {
			return nil
		}
		buf = tmp
	}
	outchan <- fmt.Sprintf("put object %s", checksum)
	return snapshot.BackingTransaction.ObjectPut(checksum, string(buf))
}

func (snapshot *Snapshot) ObjectGet(checksum string) (*Object, error) {
	keypair := snapshot.BackingStore.Context().Keypair
	outchan := snapshot.BackingStore.Context().StdoutChannel

	outchan <- fmt.Sprintf("get object %s", checksum)
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
	outchan := snapshot.BackingStore.Context().StdoutChannel

	buf = compression.Deflate(buf)

	if snapshot.BackingStore.Configuration().Encrypted != "" {
		tmp, err := encryption.Encrypt(keypair.MasterKey, buf)
		if err != nil {
			return nil
		}
		buf = tmp
	}
	outchan <- fmt.Sprintf("put chunk %s", checksum)
	return snapshot.BackingTransaction.ChunkPut(checksum, string(buf))
}

func (snapshot *Snapshot) ChunkGet(checksum string) ([]byte, error) {
	keypair := snapshot.BackingStore.Context().Keypair
	outchan := snapshot.BackingStore.Context().StdoutChannel

	outchan <- fmt.Sprintf("get chunk %s", checksum)
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
