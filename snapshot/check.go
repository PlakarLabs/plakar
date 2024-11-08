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
	snap.Repository().Context().Events().Send(events.PathEvent(pathname))
	fsinfo, err := fs.Stat(pathname)
	if err != nil {
		snap.Repository().Context().Events().Send(events.DirectoryMissingEvent(pathname))

		return false, err
	}
	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		snap.Repository().Context().Events().Send(events.DirectoryEvent(pathname))
		complete := true
		for _, child := range dirEntry.Children {
			ok, err := snapshotCheckPath(snap, fs, filepath.Join(pathname, child.FileInfo.Name()), opts, concurency, wg)
			if err != nil || !ok {
				complete = false
			}
		}
		return complete, err
	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile && fileEntry.FileInfo().Mode().IsRegular() {
		snap.Repository().Context().Events().Send(events.FileEvent(pathname))

		concurency <- true
		wg.Add(1)
		go func(_fileEntry *vfs.FileEntry) {
			defer wg.Done()
			defer func() { <-concurency }()

			object, err := snap.LookupObject(_fileEntry.Checksum)
			if err != nil {
				snap.Repository().Context().Events().Send(events.ObjectMissingEvent(_fileEntry.Checksum))
				return
			}

			hasher := snap.repository.Hasher()
			snap.Repository().Context().Events().Send(events.ObjectEvent(object.Checksum))
			for _, chunk := range object.Chunks {
				snap.Repository().Context().Events().Send(events.ChunkEvent(chunk.Checksum))
				if opts.FastCheck {
					exists := snap.CheckChunk(chunk.Checksum)
					if !exists {
						snap.Repository().Context().Events().Send(events.ChunkMissingEvent(chunk.Checksum))
						return
					}
					snap.Repository().Context().Events().Send(events.ChunkOKEvent(chunk.Checksum))
				} else {
					exists := snap.CheckChunk(chunk.Checksum)
					if !exists {
						snap.Repository().Context().Events().Send(events.ChunkMissingEvent(chunk.Checksum))
						return
					}
					data, err := snap.GetChunk(chunk.Checksum)
					if err != nil {
						snap.Repository().Context().Events().Send(events.ChunkMissingEvent(chunk.Checksum))
						return
					}
					snap.Repository().Context().Events().Send(events.ChunkOKEvent(chunk.Checksum))

					hasher.Write(data)

					checksum := snap.repository.Checksum(data)
					if !bytes.Equal(checksum[:], chunk.Checksum[:]) {
						snap.Repository().Context().Events().Send(events.ChunkCorruptedEvent(chunk.Checksum))
						return
					}
					snap.Repository().Context().Events().Send(events.ObjectOKEvent(object.Checksum))
				}
			}
			if !bytes.Equal(hasher.Sum(nil), object.Checksum[:]) {
				snap.Repository().Context().Events().Send(events.ObjectCorruptedEvent(object.Checksum))
				snap.Repository().Context().Events().Send(events.FileCorruptedEvent(pathname))
				return
			}
			snap.Repository().Context().Events().Send(events.FileOKEvent(pathname))
		}(fileEntry)
		return true, nil
	} else {
		return false, fmt.Errorf("unexpected vfs entry type")
	}
}

func (snap *Snapshot) Check(pathname string, opts *CheckOptions) (bool, error) {
	snap.Repository().Context().Events().Send(events.StartEvent())
	defer snap.Repository().Context().Events().Send(events.DoneEvent())

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
