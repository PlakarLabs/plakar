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
	ss.RealSize = snapshot.RealSize
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
	snapshot.RealSize = snapshotStorage.RealSize
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
	keypair := snapshot.BackingStore.Context().Keypair
	outchan := snapshot.BackingStore.Context().StdoutChannel
	errchan := snapshot.BackingStore.Context().StderrChannel

	chanInode := make(chan *FileInfo)
	chanError := make(chan error)
	chanChunk := make(chan struct {
		*Chunk
		string
	})
	chanObject := make(chan *Object)

	go func() {
		for {
			select {
			case fi := <-chanInode:
				if fi.Mode.IsDir() {
					snapshot.Directories[fi.path] = fi
				} else if fi.Mode.IsRegular() {
					snapshot.Files[fi.path] = fi
				} else {
					snapshot.NonRegular[fi.path] = fi
				}

			case err := <-chanError:
				errchan <- err.Error()

			case chunk := <-chanChunk:
				if _, ok := snapshot.Chunks[chunk.Checksum]; !ok {
					snapshot.Chunks[chunk.Checksum] = chunk.Chunk
					snapshot.RealSize += uint64(chunk.Length)
				}
				snapshot.Size += uint64(chunk.Length)

			case object := <-chanObject:
				checksums := make([]string, 0)
				chunks := make(map[string]*Chunk)
				for _, chunk := range object.Chunks {
					checksums = append(checksums, chunk.Checksum)
					chunks[chunk.Checksum] = chunk
				}

				res := snapshot.BackingTransaction.ChunksMark(checksums)
				for checksum, exists := range res {
					chunk := chunks[checksum]
					if exists {
						outchan <- fmt.Sprintf("skip chunk %s", checksum)
					} else {
						object.fp.Seek(int64(chunk.Start), 0)

						buf := make([]byte, chunk.Length)
						_, err := object.fp.Read(buf)
						if err != nil {
							continue
						}

						err = snapshot.ChunkPut(checksum, buf)
						if err != nil {
							continue
						}
					}
				}

				exists := snapshot.BackingTransaction.ObjectMark(object.Checksum)
				if exists {
					outchan <- fmt.Sprintf("skip object %s", object.Checksum)
				} else {
					jobject, err := json.Marshal(object)
					if err != nil {
						chanError <- err
						return
					}

					jobject = compression.Deflate(jobject)
					if snapshot.BackingStore.Configuration().Encrypted != "" {
						tmp, err := encryption.Encrypt(keypair.MasterKey, jobject)
						if err != nil {
							chanError <- err
						}
						jobject = tmp
					}

					err = snapshot.BackingTransaction.ObjectPut(object.Checksum, string(jobject))
					if err != nil {
						chanError <- err
						return
					}
				}

				snapshot.Objects[object.Checksum] = object
				snapshot.Sums[object.path] = object.Checksum
				object.fp.Close()
			}
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
				chanError <- err
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
					chanError <- err
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

				chanChunk <- struct {
					*Chunk
					string
				}{&chunk, string(cdcChunk.Data)}
			}

			object.Checksum = fmt.Sprintf("%032x", objectHash.Sum(nil))
			chanObject <- &object
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
	snapshotStorage.RealSize = snapshot.RealSize

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
