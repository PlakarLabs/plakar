package repository

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

func (snapshot *Snapshot) Pull(root string, pattern string) {
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
			continue
		}

		data, err := snapshot.BackingStore.ObjectGet(checksum)
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
			data, err := snapshot.BackingStore.ChunkGet(chunk.Checksum)
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
					fmt.Fprintf(os.Stderr, "%s: corrupt chunk %s: checksum mismatch\n", file, chunk.Checksum)
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

func (snapshot *Snapshot) Push(root string) {

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
				fmt.Fprintf(os.Stderr, "%s\n", err)

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
						//fmt.Printf("\rskip: %s %s [%d:%d]", checksum, object.path, chunk.Start, chunk.Length)
					} else {
						//fmt.Printf("\rpush: %s %s [%d:%d]", checksum, object.path, chunk.Start, chunk.Length)
						object.fp.Seek(int64(chunk.Start), 0)

						buf := make([]byte, chunk.Length)
						_, err := object.fp.Read(buf)
						if err != nil {
							continue
						}
						tmp := make(map[string]string)
						tmp[checksum] = string(Deflate(buf))
						snapshot.BackingTransaction.ChunksPut(tmp)
					}
				}

				exists := snapshot.BackingTransaction.ObjectMark(object.Checksum)
				if !exists {
					jobject, err := json.Marshal(object)
					if err != nil {
						chanError <- err
						return
					}

					jobject = Deflate(jobject)
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
	fmt.Println(snapshot.Uuid)
}

func (snapshot *Snapshot) Commit() error {
	// commit index to transaction
	jsnapshot, _ := json.Marshal(snapshot)
	jsnapshot = Deflate(jsnapshot)

	snapshot.BackingTransaction.IndexPut(string(jsnapshot))

	// commit transaction to store
	snapshot.BackingTransaction.Commit(snapshot)

	return nil
}

func (snapshot *Snapshot) Purge() error {
	return snapshot.BackingStore.Purge(snapshot.Uuid)
}

func (snapshot *Snapshot) ObjectGet(checksum string) (*Object, error) {
	data, err := snapshot.BackingStore.ObjectGet(checksum)
	if err != nil {
		return nil, err
	}

	data, err = Inflate(data)
	if err != nil {
		return nil, err
	}

	object := &Object{}
	err = json.Unmarshal(data, &object)
	return object, err
}

func (snapshot *Snapshot) ChunkGet(checksum string) ([]byte, error) {
	data, err := snapshot.BackingStore.ChunkGet(checksum)
	if err != nil {
		return nil, err
	}

	data, err = Inflate(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
