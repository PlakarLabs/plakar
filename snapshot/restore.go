package snapshot

import (
	"fmt"
	"os"
	"path"
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

type restoreContext struct {
	hardlinks      map[string]string
	hardlinksMutex sync.Mutex
	maxConcurrency chan bool
}

func snapshotRestorePath(snap *Snapshot, fs *vfs.Filesystem, exp exporter.Exporter, target string, base string, pathname string, opts *RestoreOptions, restoreContext *restoreContext, wg *sync.WaitGroup) error {
	snap.Event(events.PathEvent(snap.Header.Identifier, pathname))
	fsinfo, err := fs.Stat(pathname)
	if err != nil {
		snap.Event(events.DirectoryMissingEvent(snap.Header.Identifier, pathname))
		return err
	}

	var dest string
	if opts.Rebase && strings.HasPrefix(pathname, base) {
		dest = path.Join(target, pathname[len(base):])
	} else {
		dest = path.Join(target, pathname)
	}

	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		snap.Event(events.DirectoryEvent(snap.Header.Identifier, pathname))

		if pathname != "/" {
			if err := exp.CreateDirectory(dest); err != nil {
				snap.Event(events.DirectoryErrorEvent(snap.Header.Identifier, pathname, err.Error()))
				return err
			}
		}
		complete := true

		subwg := sync.WaitGroup{}

		children, err := fs.ChildrenIter(dirEntry)
		if err != nil {
			return err
		}
		for child := range children {
			err = snapshotRestorePath(snap, fs, exp, target, base, path.Join(pathname, child.Stat().Name()), opts, restoreContext, &subwg)
			if err != nil {
				complete = false
			}
		}
		subwg.Wait()

		if !complete {
			snap.Event(events.DirectoryCorruptedEvent(snap.Header.Identifier, pathname))
			return err
		} else {
			if pathname != "/" {
				if err := exp.SetPermissions(dest, dirEntry.Stat()); err != nil {
					snap.Event(events.DirectoryErrorEvent(snap.Header.Identifier, pathname, err.Error()))
					return err
				}
			}
			snap.Event(events.DirectoryOKEvent(snap.Header.Identifier, pathname))
			return nil
		}
	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile && fileEntry.Stat().Mode().IsRegular() {
		snap.Event(events.FileEvent(snap.Header.Identifier, pathname))

		restoreContext.maxConcurrency <- true
		wg.Add(1)
		go func(_fileEntry *vfs.FileEntry) {
			defer wg.Done()
			defer func() { <-restoreContext.maxConcurrency }()

			if fileEntry.Stat().Nlink() > 1 {
				key := fmt.Sprintf("%d:%d", fileEntry.Stat().Dev(), fileEntry.Stat().Ino())
				restoreContext.hardlinksMutex.Lock()
				v, ok := restoreContext.hardlinks[key]
				restoreContext.hardlinksMutex.Unlock()
				if ok {
					os.Link(v, dest)
					return
				} else {
					restoreContext.hardlinksMutex.Lock()
					restoreContext.hardlinks[key] = dest
					restoreContext.hardlinksMutex.Unlock()
				}
			}

			rd, err := snap.NewReader(pathname)
			if err != nil {
				snap.Event(events.FileErrorEvent(snap.Header.Identifier, pathname, err.Error()))
				return
			}
			defer rd.Close()

			if err := exp.StoreFile(dest, rd); err != nil {
				snap.Event(events.FileErrorEvent(snap.Header.Identifier, pathname, err.Error()))
			} else if err := exp.SetPermissions(dest, fileEntry.Stat()); err != nil {
				snap.Event(events.FileErrorEvent(snap.Header.Identifier, pathname, err.Error()))
			} else {
				snap.Event(events.FileOKEvent(snap.Header.Identifier, pathname))
			}

		}(fileEntry)
		return nil
	} else {
		return fmt.Errorf("unexpected vfs entry type")
	}
}

func (snap *Snapshot) Restore(exp exporter.Exporter, base string, pathname string, opts *RestoreOptions) error {
	snap.Event(events.StartEvent())
	defer snap.Event(events.DoneEvent())

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	maxConcurrency := opts.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = uint64(snap.Context().GetMaxConcurrency())
	}

	restoreContext := &restoreContext{
		hardlinks:      make(map[string]string),
		hardlinksMutex: sync.Mutex{},
		maxConcurrency: make(chan bool, maxConcurrency),
	}
	defer close(restoreContext.maxConcurrency)

	base = path.Clean(base)
	if base != "/" && !strings.HasSuffix(base, "/") {
		base = base + "/"
	}

	wg := sync.WaitGroup{}
	defer wg.Wait()

	return snapshotRestorePath(snap, fs, exp, base, pathname, pathname, opts, restoreContext, &wg)
}
