package snapshot

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/poolpOrg/plakar/logger"
)

func (snapshot *Snapshot) Pull(root string, rebase bool, pattern string) {
	var wg sync.WaitGroup
	maxDirectoriesConcurrency := make(chan bool, 1)
	maxFilesConcurrency := make(chan bool, 1)
	//maxChunksConcurrency := make(chan bool, 1024)
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
	if _, ok := snapshot.Index.Filesystem.LookupInodeForFile(fpattern); ok {
		//patternIsFile = true
		tmp := strings.Split(dpattern, "/")
		if len(tmp) > 1 {
			dpattern = strings.Join(tmp[:len(tmp)-1], "/")
		}
	}

	directoriesCount := 0
	for _, directory := range snapshot.Index.Filesystem.ListDirectories() {
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
			fi, _ := snapshot.LookupInodeForPathname(directory)
			rel := path.Clean(fmt.Sprintf("./%s", directory))
			if rebase && strings.HasPrefix(directory, dpattern) {
				dest = fmt.Sprintf("%s/%s", root, directory[len(dpattern):])
			} else {
				dest = fmt.Sprintf("%s/%s", root, directory)
			}

			logger.Trace("snapshot %s: mkdir %s, mode=%s, uid=%d, gid=%d", snapshot.Metadata.Uuid, rel, fi.Mode.String(), fi.Uid, fi.Gid)
			os.MkdirAll(dest, 0700)
			os.Chmod(dest, fi.Mode)
			os.Chown(dest, int(fi.Uid), int(fi.Gid))
			directoriesCount++
		}(directory)
	}
	wg.Wait()

	filesCount := 0
	var filesSize uint64 = 0
	for _, filename := range snapshot.Index.Filesystem.ListFiles() {
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
			fi, _ := snapshot.LookupInodeForPathname(file)
			rel := path.Clean(fmt.Sprintf("./%s", file))
			if rebase && strings.HasPrefix(file, dpattern) {
				dest = fmt.Sprintf("%s/%s", root, file[len(dpattern):])
			} else {
				dest = fmt.Sprintf("%s/%s", root, file)
			}
			dest = filepath.Clean(dest)

			checksum, exists := snapshot.Index.Pathnames[file]
			if !exists {
				logger.Warn("skipping %s", rel)
				return
			}

			logger.Trace("snapshot %s: create %s, mode=%s, uid=%d, gid=%d", snapshot.Metadata.Uuid, rel, fi.Mode.String(), fi.Uid, fi.Gid)

			f, err := os.Create(dest)
			if err != nil {
				return
			}
			defer f.Close()

			object, err := snapshot.GetObject(checksum)
			if err != nil {
				return
			}

			objectHash := sha256.New()
			for _, chunkChecksum := range object.Chunks {
				data, err := snapshot.GetChunk(chunkChecksum)
				if err != nil {
					continue
				}

				chunk, _ := snapshot.GetChunkInfo(chunkChecksum)

				if len(data) != int(chunk.Length) {
					continue
				} else {
					chunkHash := sha256.New()
					chunkHash.Write(data)
					if chunk.Checksum != fmt.Sprintf("%032x", chunkHash.Sum(nil)) {
						continue
					}
				}
				objectHash.Write(data)
				f.Write(data)
				filesSize += uint64(len(data))
			}
			if checksum != fmt.Sprintf("%032x", objectHash.Sum(nil)) {
			}

			f.Sync()
			f.Close()
			os.Chmod(dest, fi.Mode)
			os.Chown(dest, int(fi.Uid), int(fi.Gid))
			filesCount++
		}(filename)
	}
	wg.Wait()
}
