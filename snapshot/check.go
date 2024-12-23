package snapshot

import (
	"bytes"
	"fmt"
	"path"
	"sync"

	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

type CheckOptions struct {
	MaxConcurrency uint64
	FastCheck      bool
}

func snapshotCheckPath(snap *Snapshot, fs *vfs.Filesystem, pathname string, opts *CheckOptions, concurrency chan bool, wg *sync.WaitGroup) (bool, error) {
	snap.Event(events.PathEvent(snap.Header.Identifier, pathname))
	fsinfo, err := fs.Stat(pathname)
	if err != nil {
		snap.Event(events.DirectoryMissingEvent(snap.Header.Identifier, pathname))

		return false, err
	}
	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		snap.Event(events.DirectoryEvent(snap.Header.Identifier, pathname))
		complete := true

		children, err := fs.ChildrenIter(dirEntry)
		if err != nil {
			return false, err
		}
		for child := range children {
			ok, err := snapshotCheckPath(snap, fs, path.Join(pathname, child.Stat().Name()), opts, concurrency, wg)
			if err != nil || !ok {
				complete = false
			}
		}

		if !complete {
			snap.Event(events.DirectoryCorruptedEvent(snap.Header.Identifier, pathname))
		} else {
			snap.Event(events.DirectoryOKEvent(snap.Header.Identifier, pathname))
		}
		return complete, err
	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile {
		snap.Event(events.FileEvent(snap.Header.Identifier, pathname))
		if fileEntry.Stat().Mode().IsRegular() {
			concurrency <- true
			wg.Add(1)
			go func(_fileEntry *vfs.FileEntry) {
				defer wg.Done()
				defer func() { <-concurrency }()

				object, err := snap.LookupObject(_fileEntry.Object.Checksum)
				if err != nil {
					snap.Event(events.ObjectMissingEvent(snap.Header.Identifier, _fileEntry.Object.Checksum))
					return
				}

				hasher := snap.repository.Hasher()
				snap.Event(events.ObjectEvent(snap.Header.Identifier, object.Checksum))
				complete := true
				for _, chunk := range object.Chunks {
					snap.Event(events.ChunkEvent(snap.Header.Identifier, chunk.Checksum))
					if opts.FastCheck {
						exists := snap.BlobExists(packfile.TYPE_CHUNK, chunk.Checksum)
						if !exists {
							snap.Event(events.ChunkMissingEvent(snap.Header.Identifier, chunk.Checksum))
							complete = false
							break
						}
						snap.Event(events.ChunkOKEvent(snap.Header.Identifier, chunk.Checksum))
					} else {
						exists := snap.BlobExists(packfile.TYPE_CHUNK, chunk.Checksum)
						if !exists {
							snap.Event(events.ChunkMissingEvent(snap.Header.Identifier, chunk.Checksum))
							complete = false
							break
						}
						data, err := snap.GetBlob(packfile.TYPE_CHUNK, chunk.Checksum)
						if err != nil {
							snap.Event(events.ChunkMissingEvent(snap.Header.Identifier, chunk.Checksum))
							complete = false
							break
						}
						snap.Event(events.ChunkOKEvent(snap.Header.Identifier, chunk.Checksum))

						hasher.Write(data)

						checksum := snap.repository.Checksum(data)
						if !bytes.Equal(checksum[:], chunk.Checksum[:]) {
							snap.Event(events.ChunkCorruptedEvent(snap.Header.Identifier, chunk.Checksum))
							complete = false
							break
						}
					}
				}
				if !complete {
					snap.Event(events.ObjectCorruptedEvent(snap.Header.Identifier, object.Checksum))
				} else {
					snap.Event(events.ObjectOKEvent(snap.Header.Identifier, object.Checksum))
				}

				if !bytes.Equal(hasher.Sum(nil), object.Checksum[:]) {
					snap.Event(events.ObjectCorruptedEvent(snap.Header.Identifier, object.Checksum))
					snap.Event(events.FileCorruptedEvent(snap.Header.Identifier, pathname))
					return
				}
			}(fileEntry)
		}
		snap.Event(events.FileOKEvent(snap.Header.Identifier, pathname))
		return true, nil
	} else {
		return false, fmt.Errorf("unexpected vfs entry type: %T", fsinfo)
	}
}

func (snap *Snapshot) Check(pathname string, opts *CheckOptions) (bool, error) {
	snap.Event(events.StartEvent())
	defer snap.Event(events.DoneEvent())

	fs, err := snap.Filesystem()
	if err != nil {
		return false, err
	}

	maxConcurrency := opts.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = uint64(snap.Context().GetMaxConcurrency())
	}

	maxConcurrencyChan := make(chan bool, maxConcurrency)
	wg := sync.WaitGroup{}
	defer wg.Wait()
	defer close(maxConcurrencyChan)

	return snapshotCheckPath(snap, fs, pathname, opts, maxConcurrencyChan, &wg)
}
