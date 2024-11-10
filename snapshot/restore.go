package snapshot

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

type RestoreOptions struct {
	MaxConcurrency uint64
	Rebase         bool
}

func (snap *Snapshot) Restore(exp *exporter.Exporter, pattern string, opts *RestoreOptions) error {
	rebase := opts.Rebase

	hardlinks := make(map[string]string)
	hardlinksMutex := sync.Mutex{}

	var wg sync.WaitGroup
	maxConcurrency := make(chan bool, opts.MaxConcurrency)

	dpattern := path.Clean(pattern)
	fpattern := path.Clean(pattern)

	/* if at root, pretend there's no pattern */
	if dpattern == "/" || dpattern == "." {
		dpattern = ""
		fpattern = ""
	}

	/* if pattern is a file, we rebase dpattern to parent */
	//patternIsFile := false

	fs, err := vfs.NewFilesystem(snap.repository, snap.Header.Root)
	if err != nil {
		return err
	}
	if _, err := fs.Stat(fpattern); err != nil {
		//patternIsFile = true
		tmp := strings.Split(dpattern, "/")
		if len(tmp) > 1 {
			dpattern = strings.Join(tmp[:len(tmp)-1], "/")
		}
	}

	directoriesCount := 0
	for directory := range fs.Directories() {
		if dpattern != "" {
			if directory != dpattern &&
				(!strings.HasPrefix(directory, fmt.Sprintf("%s/", dpattern)) ||
					len(directory) > len(dpattern)) {
				continue
			}
		}
		maxConcurrency <- true
		wg.Add(1)
		go func(directory string) {
			defer wg.Done()
			defer func() { <-maxConcurrency }()

			snap.Event(events.DirectoryEvent(snap.Header.SnapshotID, directory))

			var dest string

			fi, _ := fs.Stat(directory)
			rel := path.Clean(filepath.Join(".", directory))
			if rebase && strings.HasPrefix(directory, dpattern) {
				dest = filepath.Join(exp.Root(), directory[len(dpattern):])
			} else {
				dest = filepath.Join(exp.Root(), directory)
			}
			_ = rel

			dest = filepath.FromSlash(dest)
			if err := exp.CreateDirectory(dest, fi.(*vfs.DirEntry).FileInfo()); err != nil {
				snap.Event(events.DirectoryErrorEvent(snap.Header.SnapshotID, directory, err.Error()))
			} else {
				snap.Event(events.DirectoryOKEvent(snap.Header.SnapshotID, directory))
			}
			directoriesCount++
		}(directory)
	}
	wg.Wait()

	filesCount := 0
	var filesSize uint64 = 0
	for filename := range fs.Files() {
		if fpattern != "" {
			if filename != fpattern &&
				!strings.HasPrefix(filename, fmt.Sprintf("%s/", fpattern)) {
				continue
			}
		}

		maxConcurrency <- true
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			defer func() { <-maxConcurrency }()

			snap.Event(events.FileEvent(snap.Header.SnapshotID, file))

			var dest string

			fi, _ := fs.Stat(file)

			//rel := path.Clean(filepath.Join(".", file))
			if rebase && strings.HasPrefix(file, dpattern) {
				dest = filepath.Join(exp.Root(), file[len(dpattern):])
			} else {
				dest = filepath.Join(exp.Root(), file)
			}
			dest = filepath.Clean(dest)

			if fi.(*vfs.FileEntry).NumLinks > 1 {
				key := fmt.Sprintf("%d:%d", fi.(*vfs.FileEntry).DeviceID, fi.(*vfs.FileEntry).InodeID)
				hardlinksMutex.Lock()
				v, ok := hardlinks[key]
				hardlinksMutex.Unlock()
				if ok {
					os.Link(v, dest)
					filesSize += uint64(fi.(*vfs.FileEntry).Size)
					filesCount++
					return
				} else {
					hardlinksMutex.Lock()
					hardlinks[key] = dest
					hardlinksMutex.Unlock()
				}
			}

			rd, err := snap.NewReader(file)
			if err != nil {
				snap.Event(events.FileErrorEvent(snap.Header.SnapshotID, file, err.Error()))
				return
			}
			defer rd.Close()

			if err := exp.StoreFile(dest, fi.(*vfs.FileEntry).FileInfo(), rd); err != nil {
				snap.Event(events.FileErrorEvent(snap.Header.SnapshotID, file, err.Error()))
			} else {
				snap.Event(events.FileOKEvent(snap.Header.SnapshotID, file))
			}

			filesSize += uint64(fi.(*vfs.FileEntry).Size)
			filesCount++
		}(filename)
	}
	wg.Wait()
	return nil
}
