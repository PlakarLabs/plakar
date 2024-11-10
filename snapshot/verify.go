package snapshot

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

type CheckOptions struct {
	MaxConcurrency uint64
	FastCheck      bool
}

func snapshotCheckPath(snap *Snapshot, fs *vfs.Filesystem, pathname string, opts *CheckOptions, concurency chan bool, wg *sync.WaitGroup) (bool, error) {
	snap.Event(events.PathEvent(snap.Header.IndexID, pathname))
	fsinfo, err := fs.Stat(pathname)
	if err != nil {
		snap.Event(events.DirectoryMissingEvent(snap.Header.IndexID, pathname))

		return false, err
	}
	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		snap.Event(events.DirectoryEvent(snap.Header.IndexID, pathname))
		complete := true
		for _, child := range dirEntry.Children {
			ok, err := snapshotCheckPath(snap, fs, filepath.Join(pathname, child.FileInfo.Name()), opts, concurency, wg)
			if err != nil || !ok {
				complete = false
			}
		}
		if !complete {
			snap.Event(events.DirectoryCorruptedEvent(snap.Header.IndexID, pathname))
		} else {
			snap.Event(events.DirectoryOKEvent(snap.Header.IndexID, pathname))
		}
		return complete, err
	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile && fileEntry.FileInfo().Mode().IsRegular() {
		snap.Event(events.FileEvent(snap.Header.IndexID, pathname))

		concurency <- true
		wg.Add(1)
		go func(_fileEntry *vfs.FileEntry) {
			defer wg.Done()
			defer func() { <-concurency }()

			object, err := snap.LookupObject(_fileEntry.Checksum)
			if err != nil {
				snap.Event(events.ObjectMissingEvent(snap.Header.IndexID, _fileEntry.Checksum))
				return
			}

			hasher := snap.repository.Hasher()
			snap.Event(events.ObjectEvent(snap.Header.IndexID, object.Checksum))
			complete := true
			for _, chunk := range object.Chunks {
				snap.Event(events.ChunkEvent(snap.Header.IndexID, chunk.Checksum))
				if opts.FastCheck {
					exists := snap.CheckChunk(chunk.Checksum)
					if !exists {
						snap.Event(events.ChunkMissingEvent(snap.Header.IndexID, chunk.Checksum))
						complete = false
						break
					}
					snap.Event(events.ChunkOKEvent(snap.Header.IndexID, chunk.Checksum))
				} else {
					exists := snap.CheckChunk(chunk.Checksum)
					if !exists {
						snap.Event(events.ChunkMissingEvent(snap.Header.IndexID, chunk.Checksum))
						complete = false
						break
					}
					data, err := snap.GetChunk(chunk.Checksum)
					if err != nil {
						snap.Event(events.ChunkMissingEvent(snap.Header.IndexID, chunk.Checksum))
						complete = false
						break
					}
					snap.Event(events.ChunkOKEvent(snap.Header.IndexID, chunk.Checksum))

					hasher.Write(data)

					checksum := snap.repository.Checksum(data)
					if !bytes.Equal(checksum[:], chunk.Checksum[:]) {
						snap.Event(events.ChunkCorruptedEvent(snap.Header.IndexID, chunk.Checksum))
						complete = false
						break
					}
				}
			}
			if !complete {
				snap.Event(events.ObjectCorruptedEvent(snap.Header.IndexID, object.Checksum))
			} else {
				snap.Event(events.ObjectOKEvent(snap.Header.IndexID, object.Checksum))
			}

			if !bytes.Equal(hasher.Sum(nil), object.Checksum[:]) {
				snap.Event(events.ObjectCorruptedEvent(snap.Header.IndexID, object.Checksum))
				snap.Event(events.FileCorruptedEvent(snap.Header.IndexID, pathname))
				return
			}
			snap.Event(events.FileOKEvent(snap.Header.IndexID, pathname))
		}(fileEntry)
		return true, nil
	} else {
		return false, fmt.Errorf("unexpected vfs entry type")
	}
}

func (snap *Snapshot) Verify(pathname string, opts *CheckOptions) (bool, error) {
	snap.Event(events.StartEvent())
	defer snap.Event(events.DoneEvent())

	fs, err := snap.Filesystem()
	if err != nil {
		return false, err
	}

	maxConcurrency := make(chan bool, opts.MaxConcurrency)
	wg := sync.WaitGroup{}
	defer wg.Wait()
	defer close(maxConcurrency)

	return snapshotCheckPath(snap, fs, pathname, opts, maxConcurrency, &wg)
}
