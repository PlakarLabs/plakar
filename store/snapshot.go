package store

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/iafan/cwalk"
	"github.com/restic/chunker"
)

func SnapshotToSummary(snapshot *Snapshot) *SnapshotSummary {
	ss := &SnapshotSummary{}
	ss.Uuid = snapshot.Uuid
	ss.CreationTime = snapshot.CreationTime
	ss.Version = snapshot.Version
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

func (self *Snapshot) Pull(root string, pattern string) {
	var dest string

	dpattern := path.Clean(pattern)
	fpattern := path.Clean(pattern)

	/* if at root, pretend there's no pattern */
	if dpattern == "/" || dpattern == "." {
		dpattern = ""
		fpattern = ""
	}

	/* if pattern is a file, we rebase dpattern to parent */
	if _, ok := self.Files[fpattern]; ok {
		tmp := strings.Split(dpattern, "/")
		if len(tmp) > 1 {
			dpattern = strings.Join(tmp[:len(tmp)-1], "/")
		}
	}

	for directory, fi := range self.Directories {
		if directory != dpattern &&
			!strings.HasPrefix(directory, fmt.Sprintf("%s/", dpattern)) {
			continue
		}
		dest = fmt.Sprintf("%s/%s", root, directory)
		os.MkdirAll(dest, 0700)
		os.Chmod(dest, fi.Mode)
		os.Chown(dest, int(fi.Uid), int(fi.Gid))
	}

	for file, fi := range self.Files {
		if file != fpattern &&
			!strings.HasPrefix(file, fmt.Sprintf("%s/", fpattern)) {
			continue
		}

		dest = fmt.Sprintf("%s/%s", root, file)

		checksum := self.Sums[file]

		f, err := os.Create(dest)
		if err != nil {
			continue
		}

		data, err := self.store.ObjectGet(checksum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: missing object %s\n", file, checksum)
			continue
		}

		data, err = Inflate(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: corrupt object %s\n", file, checksum)
			continue
		}

		object := Object{}
		err = json.Unmarshal(data, &object)
		if err != nil {
			f.Close()
			continue
		}

		objectHash := sha256.New()
		for _, chunk := range object.Chunks {
			data, err := self.store.ChunkGet(chunk.Checksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: missing chunk %s\n", file, chunk.Checksum)
				continue
			}
			data, err = Inflate(data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: corrupt chunk %s\n", file, chunk.Checksum)
				continue
			}

			if len(data) != int(chunk.Length) {
				fmt.Fprintf(os.Stderr, "%s: corrupt chunk %s: invalid length (%d should be %d)\n",
					file, chunk.Checksum, len(data), chunk.Length)
				continue
			} else {
				chunkHash := sha256.New()
				chunkHash.Write(data)
				if chunk.Checksum != fmt.Sprintf("%032x", chunkHash.Sum(nil)) {
					fmt.Fprintf(os.Stderr, "%s: corrupt chunk %s: checksum mismatch\n", file, chunk.Checksum, chunkHash.Sum(nil))
					continue
				}
			}

			objectHash.Write(data)
			f.Write(data)
		}
		if object.Checksum != fmt.Sprintf("%032x", objectHash.Sum(nil)) {
			fmt.Fprintf(os.Stderr, "%s: corrupt file: checksum mismatch\n", file)
		}

		f.Close()
		os.Chmod(dest, fi.Mode)
		os.Chown(dest, int(fi.Uid), int(fi.Gid))
	}
}

func (self *Snapshot) Push(root string) {

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
					self.Directories[fi.path] = fi
				} else if fi.Mode.IsRegular() {
					self.Files[fi.path] = fi
				} else {
					self.NonRegular[fi.path] = fi
				}

			case err := <-chanError:
				fmt.Fprintf(os.Stderr, "%s\n", err)

			case chunk := <-chanChunk:
				if _, ok := self.Chunks[chunk.Checksum]; !ok {
					self.Chunks[chunk.Checksum] = chunk.Chunk
					self.RealSize += uint64(chunk.Length)
				}
				self.Size += uint64(chunk.Length)

			case object := <-chanObject:
				checksums := make([]string, 0)
				chunks := make(map[string]*Chunk)
				for _, chunk := range object.Chunks {
					checksums = append(checksums, chunk.Checksum)
					chunks[chunk.Checksum] = chunk
				}

				res := self.transaction.ChunksMark(checksums)
				for checksum, exists := range res {
					chunk := chunks[checksum]
					if exists {
						//fmt.Printf("\rskip: %s %s [%d:%d]", checksum, object.path, chunk.Start, chunk.Length)
					} else {
						//fmt.Printf("\rpush: %s %s [%d:%d]", checksum, object.path, chunk.Start, chunk.Length)
						object.fp.Seek(int64(chunk.Start), 0)

						buf := make([]byte, chunk.Length)
						_, err := object.fp.Read(buf)
						if err != nil {
						}
						tmp := make(map[string]string)
						tmp[checksum] = string(Deflate(buf))
						self.transaction.ChunksPut(tmp)
					}
				}

				exists := self.transaction.ObjectMark(object.Checksum)
				if !exists {
					jobject, err := json.Marshal(object)
					if err != nil {
						chanError <- err
						return
					}

					jobject = Deflate(jobject)
					err = self.transaction.ObjectPut(object.Checksum, string(jobject))
					if err != nil {
						chanError <- err
						return
					}
				}

				self.Objects[object.Checksum] = object
				self.Sums[object.path] = object.Checksum
				object.fp.Close()
			}
		}
	}()

	cwalk.Walk(root, func(path string, f os.FileInfo, err error) error {

		for _, skipPath := range self.skipDirs {
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
			for {
				cdcChunk, err := chk.Next(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					chanError <- err
					return nil
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
	fmt.Println(self.Uuid)
}

func (self *Snapshot) Commit() error {
	// commit index to transaction
	jsnapshot, _ := json.Marshal(self)
	jsnapshot = Deflate(jsnapshot)

	self.transaction.IndexPut(string(jsnapshot))

	// commit transaction to store
	self.transaction.Commit(self)

	return nil
}

func (self *Snapshot) Purge() error {
	return self.store.Purge(self.Uuid)
}

func (self *Snapshot) ObjectGet(checksum string) (*Object, error) {
	data, err := self.store.ObjectGet(checksum)
	if err != nil {
		return nil, err
	}

	data, err = Inflate(data)
	if err != nil {
		// prepare for when inflate can fail
		return nil, err
	}

	object := &Object{}
	err = json.Unmarshal(data, &object)
	return object, err
}

func (self *Snapshot) ChunkGet(checksum string) ([]byte, error) {
	data, err := self.store.ChunkGet(checksum)
	if err != nil {
		return nil, err
	}

	data, err = Inflate(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
