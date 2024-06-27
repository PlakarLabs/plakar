package snapshot

import (
	"fmt"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/PlakarLabs/plakar/exporter"
	"github.com/PlakarLabs/plakar/logger"
)

func (s *Snapshot) Pull(exp *exporter.Exporter, rebase bool, pattern string) {
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
	if _, ok := s.Filesystem.LookupInodeForFile(fpattern); ok {
		//patternIsFile = true
		tmp := strings.Split(dpattern, "/")
		if len(tmp) > 1 {
			dpattern = strings.Join(tmp[:len(tmp)-1], "/")
		}
	}

	directoriesCount := 0
	for directory := range s.Filesystem.ListDirectories() {
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

			fi, _ := s.Filesystem.LookupInodeForDirectory(directory)
			rel := path.Clean(filepath.Join(".", directory))
			if rebase && strings.HasPrefix(directory, dpattern) {
				dest = filepath.Join(exp.Root(), directory[len(dpattern):])
			} else {
				dest = filepath.Join(exp.Root(), directory)
			}

			logger.Trace("snapshot", "snapshot %s: mkdir %s, mode=%s, uid=%d, gid=%d", s.Header.GetIndexShortID(), rel, fi.Mode().String(), fi.Uid, fi.Gid)

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
	for filename := range s.Filesystem.ListFiles() {
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

			fi, _ := s.Filesystem.LookupInodeForFile(file)
			//rel := path.Clean(filepath.Join(".", file))
			if rebase && strings.HasPrefix(file, dpattern) {
				dest = filepath.Join(exp.Root(), file[len(dpattern):])
			} else {
				dest = filepath.Join(exp.Root(), file)
			}
			dest = filepath.Clean(dest)

			rd, err := s.NewReader(file)
			if err != nil {
				logger.Warn("failed to create reader for %s: %s", file, err)
				return
			}
			defer rd.Close()

			if err := exp.StoreFile(dest, fi, rd); err != nil {
				logger.Warn("failed to store file %s: %s", dest, err)
			}
			filesSize += uint64(fi.Size())
			filesCount++
		}(filename)
	}
	wg.Wait()
}
