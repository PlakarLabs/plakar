package snapshot

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/progress"
)

func (snapshot *Snapshot) Pull(root string, rebase bool, pattern string, showProgress bool) {
	var wg sync.WaitGroup
	maxDirectoriesConcurrency := make(chan bool, 1)
	maxFilesConcurrency := make(chan bool, 1)
	var dest string

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

	var c chan int64
	if showProgress {
		c = progress.NewProgressCount("pull", "restoring directories", int64(len(snapshot.Filesystem.ListDirectories())))
	} else {
		c = make(chan int64)
		go func() {
			for _ = range c {
			}
		}()
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
			c <- 1
			fi, _ := snapshot.Filesystem.LookupInodeForDirectory(directory)
			rel := path.Clean(fmt.Sprintf("./%s", directory))
			if rebase && strings.HasPrefix(directory, dpattern) {
				dest = fmt.Sprintf("%s/%s", root, directory[len(dpattern):])
			} else {
				dest = fmt.Sprintf("%s/%s", root, directory)
			}

			logger.Trace("snapshot", "snapshot %s: mkdir %s, mode=%s, uid=%d, gid=%d", snapshot.Metadata.GetIndexShortID(), rel, fi.Mode.String(), fi.Uid, fi.Gid)
			os.MkdirAll(dest, 0700)
			os.Chmod(dest, fi.Mode)
			os.Chown(dest, int(fi.Uid), int(fi.Gid))
			directoriesCount++
		}(directory)
	}
	wg.Wait()
	close(c)

	if showProgress {
		c = progress.NewProgressCount("pull", "restoring files", int64(len(snapshot.Filesystem.ListFiles())))
	} else {
		c = make(chan int64)
		go func() {
			for _ = range c {
			}
		}()
	}

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

			c <- 1
			fi, _ := snapshot.Filesystem.LookupInodeForFile(file)
			rel := path.Clean(fmt.Sprintf("./%s", file))
			if rebase && strings.HasPrefix(file, dpattern) {
				dest = fmt.Sprintf("%s/%s", root, file[len(dpattern):])
			} else {
				dest = fmt.Sprintf("%s/%s", root, file)
			}
			dest = filepath.Clean(dest)

			object := snapshot.Index.LookupObjectForPathname(file)
			if object == nil {
				logger.Warn("skipping %s", rel)
				return
			}

			logger.Trace("snapshot", "snapshot %s: create %s, mode=%s, uid=%d, gid=%d", snapshot.Metadata.GetIndexShortID(), rel, fi.Mode.String(), fi.Uid, fi.Gid)

			f, err := os.Create(dest)
			if err != nil {
				return
			}

			objectHash := sha256.New()
			for _, chunkChecksum := range object.Chunks {
				data, err := snapshot.GetChunk(chunkChecksum)
				if err != nil {
					f.Close()
					continue
				}

				chunk := snapshot.Index.LookupChunk(chunkChecksum)

				if len(data) != int(chunk.Length) {
					f.Close()
					continue
				} else {
					chunkHash := sha256.New()
					chunkHash.Write(data)
					if !bytes.Equal(chunk.Checksum[:], chunkHash.Sum(nil)) {
						f.Close()
						continue
					}
				}
				objectHash.Write(data)
				f.Write(data)
				filesSize += uint64(len(data))
			}
			if !bytes.Equal(object.Checksum[:], objectHash.Sum(nil)) {
			}

			f.Sync()
			f.Close()
			os.Chmod(dest, fi.Mode)
			os.Chown(dest, int(fi.Uid), int(fi.Gid))
			filesCount++
		}(filename)
	}
	wg.Wait()
	close(c)
}
