package snapshot

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/vfs/exporter"
)

func (snapshot *Snapshot) Pull(exp *exporter.Exporter, rebase bool, pattern string) {
	var wg sync.WaitGroup
	maxDirectoriesConcurrency := make(chan bool, runtime.NumCPU()*8+1)
	maxFilesConcurrency := make(chan bool, runtime.NumCPU()*8+1)

	dpattern := path.Clean(pattern)
	fpattern := path.Clean(pattern)

	/* if at root, pretend there's no pattern */
	if dpattern == "/" || dpattern == "." {
		dpattern = ""
		fpattern = ""
	}

	/* if pattern is a file, we rebase dpattern to parent */
	//patternIsFile := false
	if _, ok := snapshot.Filesystem.LookupInodeForFile(fpattern); ok {
		//patternIsFile = true
		tmp := strings.Split(dpattern, "/")
		if len(tmp) > 1 {
			dpattern = strings.Join(tmp[:len(tmp)-1], "/")
		}
	}

	directoriesCount := 0
	for _, directory := range snapshot.Filesystem.ListDirectories() {
		if dpattern != "" {
			if directory != dpattern &&
				(!strings.HasPrefix(directory, fmt.Sprintf("%s/", dpattern)) ||
					len(directory) > len(dpattern)) {
				continue
			}
		}
		maxDirectoriesConcurrency <- true
		wg.Add(1)
		go func(directory string) {
			defer wg.Done()
			defer func() { <-maxDirectoriesConcurrency }()

			var dest string

			fi, _ := snapshot.Filesystem.LookupInodeForDirectory(directory)
			rel := path.Clean(filepath.Join(".", directory))
			if rebase && strings.HasPrefix(directory, dpattern) {
				dest = filepath.Join(exp.Root(), directory[len(dpattern):])
			} else {
				dest = filepath.Join(exp.Root(), directory)
			}

			logger.Trace("snapshot", "snapshot %s: mkdir %s, mode=%s, uid=%d, gid=%d", snapshot.Header.GetIndexShortID(), rel, fi.Mode().String(), fi.Uid, fi.Gid)

			dest = filepath.FromSlash(dest)
			if err := exp.CreateDirectory(dest, fi); err != nil {
				logger.Warn("failed to create restored directory %s: %s", dest, err)
			}
			directoriesCount++
		}(directory)
	}
	wg.Wait()

	filesCount := 0
	var filesSize uint64 = 0
	for _, filename := range snapshot.Filesystem.ListFiles() {
		if fpattern != "" {
			if filename != fpattern &&
				!strings.HasPrefix(filename, fmt.Sprintf("%s/", fpattern)) {
				continue
			}
		}
		maxFilesConcurrency <- true
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			defer func() { <-maxFilesConcurrency }()

			var dest string

			fi, _ := snapshot.Filesystem.LookupInodeForFile(file)
			rel := path.Clean(filepath.Join(".", file))
			if rebase && strings.HasPrefix(file, dpattern) {
				dest = filepath.Join(exp.Root(), file[len(dpattern):])
			} else {
				dest = filepath.Join(exp.Root(), file)
			}
			dest = filepath.Clean(dest)

			hasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
			hasher.Write([]byte(file))
			pathnameChecksum := hasher.Sum(nil)
			key := [32]byte{}
			copy(key[:], pathnameChecksum)
			object := snapshot.Index.LookupObjectForPathnameChecksum(key)
			if object == nil {
				logger.Warn("skipping %s", rel)
				return
			}

			logger.Trace("snapshot", "snapshot %s: create %s, mode=%s, uid=%d, gid=%d", snapshot.Header.GetIndexShortID(), rel, fi.Mode().String(), fi.Uid, fi.Gid)

			dest = filepath.FromSlash(dest)

			f, err := os.Create(dest)
			if err != nil {
				logger.Warn("failed to create restored file %s: %s", dest, err)
				return
			}

			objectHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
			for _, chunkChecksum := range object.Chunks {
				data, err := snapshot.GetChunk(chunkChecksum)
				if err != nil {
					logger.Warn("failed to obtain chunk %064x for %s: %s", chunkChecksum, dest, err)
					f.Close()
					continue
				}

				chunk := snapshot.Index.LookupChunk(chunkChecksum)

				if len(data) != int(chunk.Length) {
					logger.Warn("chunk length mismatch: got=%d, expected=%d", len(data), int(chunk.Length))
					f.Close()
					continue
				} else {
					chunkHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
					chunkHasher.Write(data)
					if !bytes.Equal(chunk.Checksum[:], chunkHasher.Sum(nil)) {
						logger.Warn("chunk checksums mismatch: got=%064x, expected=%064x", chunkHasher.Sum(nil), chunk.Checksum[:])
						f.Close()
						continue
					}
				}
				objectHasher.Write(data)
				f.Write(data)
				filesSize += uint64(len(data))
			}
			if !bytes.Equal(object.Checksum[:], objectHasher.Sum(nil)) {
				logger.Warn("object checksum mismatches: got=%064x, expected=%064x",
					objectHasher.Sum(nil), object.Checksum[:])
			}

			if err := f.Sync(); err != nil {
				logger.Warn("sync failure: %s: %s", dest, err)
			}
			if err := f.Close(); err != nil {
				logger.Warn("close failure: %s: %s", dest, err)
			}
			if err := os.Chmod(dest, fi.Mode()); err != nil {
				logger.Warn("chmod failure: %s: %s", dest, err)
			}
			if err := os.Chown(dest, int(fi.Uid()), int(fi.Gid())); err != nil {
				if err == os.ErrPermission {
					logger.Warn("chown failure: %s: %s", dest, err)
				}
			}
			filesCount++
		}(filename)
	}
	wg.Wait()
}
