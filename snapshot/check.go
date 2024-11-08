package snapshot

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/PlakarLabs/plakar/events"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

type CheckOptions struct {
	MaxConcurrency uint64
	FastCheck      bool
}

func snapshotCheckPath(snap *Snapshot, fs *vfs.Filesystem, pathname string, opts *CheckOptions, concurency chan bool, wg *sync.WaitGroup) (bool, error) {
	snap.Event(events.PathEvent(pathname))
	fsinfo, err := fs.Stat(pathname)
	if err != nil {
		snap.Event(events.DirectoryMissingEvent(pathname))

		return false, err
	}
	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		snap.Event(events.DirectoryEvent(pathname))
		complete := true
		for _, child := range dirEntry.Children {
			ok, err := snapshotCheckPath(snap, fs, filepath.Join(pathname, child.FileInfo.Name()), opts, concurency, wg)
			if err != nil || !ok {
				complete = false
			}
		}
		return complete, err
	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile && fileEntry.FileInfo().Mode().IsRegular() {
		snap.Event(events.FileEvent(pathname))

		concurency <- true
		wg.Add(1)
		go func(_fileEntry *vfs.FileEntry) {
			defer wg.Done()
			defer func() { <-concurency }()

			object, err := snap.LookupObject(_fileEntry.Checksum)
			if err != nil {
				snap.Event(events.ObjectMissingEvent(_fileEntry.Checksum))
				return
			}

			hasher := snap.repository.Hasher()
			snap.Event(events.ObjectEvent(object.Checksum))
			for _, chunk := range object.Chunks {
				snap.Event(events.ChunkEvent(chunk.Checksum))
				if opts.FastCheck {
					exists := snap.CheckChunk(chunk.Checksum)
					if !exists {
						snap.Event(events.ChunkMissingEvent(chunk.Checksum))
						return
					}
					snap.Event(events.ChunkOKEvent(chunk.Checksum))
				} else {
					exists := snap.CheckChunk(chunk.Checksum)
					if !exists {
						snap.Event(events.ChunkMissingEvent(chunk.Checksum))
						return
					}
					data, err := snap.GetChunk(chunk.Checksum)
					if err != nil {
						snap.Event(events.ChunkMissingEvent(chunk.Checksum))
						return
					}
					snap.Event(events.ChunkOKEvent(chunk.Checksum))

					hasher.Write(data)

					checksum := snap.repository.Checksum(data)
					if !bytes.Equal(checksum[:], chunk.Checksum[:]) {
						snap.Event(events.ChunkCorruptedEvent(chunk.Checksum))
						return
					}
					snap.Event(events.ObjectOKEvent(object.Checksum))
				}
			}
			if !bytes.Equal(hasher.Sum(nil), object.Checksum[:]) {
				snap.Event(events.ObjectCorruptedEvent(object.Checksum))
				snap.Event(events.FileCorruptedEvent(pathname))
				return
			}
			snap.Event(events.FileOKEvent(pathname))
		}(fileEntry)
		return true, nil
	} else {
		return false, fmt.Errorf("unexpected vfs entry type")
	}
}

func (snap *Snapshot) Check(pathname string, opts *CheckOptions) (bool, error) {
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
