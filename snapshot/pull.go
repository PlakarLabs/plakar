package snapshot

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path"
	"strings"
)

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

		checksum := snapshot.Pathnames[file]

		f, err := os.Create(dest)
		if err != nil {
			continue
		}

		object, err := snapshot.GetObject(checksum)
		if err != nil {
			f.Close()
			continue
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
		}
		if object.Checksum != fmt.Sprintf("%032x", objectHash.Sum(nil)) {
		}

		f.Close()
		os.Chmod(dest, fi.Mode)
		os.Chown(dest, int(fi.Uid), int(fi.Gid))
	}
}
