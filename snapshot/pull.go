package snapshot

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/poolpOrg/plakar/logger"
)

func (snapshot *Snapshot) Pull(root string, pattern string) {
	var wg sync.WaitGroup
	maxDirectoriesConcurrency := make(chan bool, 1024)
	maxFilesConcurrency := make(chan bool, 1024)
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
	if _, ok := snapshot.Files[fpattern]; ok {
		tmp := strings.Split(dpattern, "/")
		if len(tmp) > 1 {
			dpattern = strings.Join(tmp[:len(tmp)-1], "/")
		}
	}

	directoriesCount := 0
	for directory, fi := range snapshot.Directories {
		if directory != dpattern &&
			!strings.HasPrefix(directory, fmt.Sprintf("%s/", dpattern)) {
			continue
		}
		maxDirectoriesConcurrency <- true
		wg.Add(1)
		go func(fi *FileInfo, directory string) {
			defer wg.Done()
			defer func() { <-maxDirectoriesConcurrency }()
			rel := path.Clean(fmt.Sprintf("./%s", directory))
			dest = path.Clean(fmt.Sprintf("%s/%s", root, directory))
			logger.Trace("snapshot %s: mkdir %s, mode=%s, uid=%d, gid=%d", snapshot.Uuid, rel, fi.Mode.String(), fi.Uid, fi.Gid)
			os.MkdirAll(dest, 0700)
			os.Chmod(dest, fi.Mode)
			os.Chown(dest, int(fi.Uid), int(fi.Gid))
			directoriesCount++
		}(fi, directory)
	}
	wg.Wait()

	filesCount := 0
	var filesSize uint64 = 0
	for file, fi := range snapshot.Files {
		if file != fpattern &&
			!strings.HasPrefix(file, fmt.Sprintf("%s/", fpattern)) {
			continue
		}
		maxFilesConcurrency <- true
		wg.Add(1)
		go func(fi *FileInfo, file string) {
			defer wg.Done()
			defer func() { <-maxFilesConcurrency }()
			rel := path.Clean(fmt.Sprintf("./%s", file))
			dest = fmt.Sprintf("%s/%s", root, file)

			checksum := snapshot.Pathnames[file]

			logger.Trace("snapshot %s: create %s, mode=%s, uid=%d, gid=%d", snapshot.Uuid, rel, fi.Mode.String(), fi.Uid, fi.Gid)

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
			for _, chunk := range object.Chunks {
				data, err := snapshot.GetChunk(chunk.Checksum)
				if err != nil {
					continue
				}

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
			if object.Checksum != fmt.Sprintf("%032x", objectHash.Sum(nil)) {
			}

			f.Close()
			os.Chmod(dest, fi.Mode)
			os.Chown(dest, int(fi.Uid), int(fi.Gid))
			filesCount++
		}(fi, file)
	}
	wg.Wait()
}
