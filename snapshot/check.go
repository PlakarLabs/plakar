package snapshot

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/PlakarLabs/plakar/events"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

func snapshotCheckPath(snap *Snapshot, fs *vfs.Filesystem, pathname string, fast bool) (bool, error) {
	snap.Repository().Context().Events().Send(events.SnapshotCheckPathnameEvent())
	fmt.Println("Checking pathnames", pathname)
	fsinfo, err := fs.Stat(pathname)
	if err != nil {
		return false, err
	}
	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		snap.Repository().Context().Events().Send(events.SnapshotCheckDirectoryEvent())
		complete := true
		for _, child := range dirEntry.Children {
			//fmt.Println("Checking child", filepath.Join(pathname, child.FileInfo.Name()))
			ok, err := snapshotCheckPath(snap, fs, filepath.Join(pathname, child.FileInfo.Name()), fast)
			if err != nil || !ok {
				complete = false
			}
		}
		return complete, err
	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile && fileEntry.FileInfo().Mode().IsRegular() {
		snap.Repository().Context().Events().Send(events.SnapshotCheckFileEvent())
		fmt.Println("Checking file", pathname)
		object, err := snap.LookupObject(fileEntry.Checksum)
		if err != nil {
			return false, fmt.Errorf("missing object for file %s", pathname)
		}

		complete := true
		hasher := snap.repository.Hasher()
		for _, chunk := range object.Chunks {
			if fast {
				exists := snap.CheckChunk(chunk.Checksum)
				if !exists {
					logger.Warn("%x: missing chunk %x for file %s", snap.Header.GetIndexShortID(), chunk.Checksum, pathname)
					complete = false
				}
			} else {
				exists := snap.CheckChunk(chunk.Checksum)
				if !exists {
					logger.Warn("%x: missing chunk %x for file %s", snap.Header.GetIndexShortID(), chunk.Checksum, pathname)
					complete = false
				}
				data, err := snap.GetChunk(chunk.Checksum)
				if err != nil {
					logger.Warn("%x: missing chunk %x for file %s: %s", snap.Header.GetIndexShortID(), chunk.Checksum, pathname, err)
					complete = false
				}

				hasher.Write(data)

				checksum := snap.repository.Checksum(data)
				if !bytes.Equal(checksum[:], chunk.Checksum[:]) {
					logger.Warn("%x: corrupted chunk %x for file %s", snap.Header.GetIndexShortID(), chunk.Checksum, pathname)
					complete = false
				}
			}
		}
		if !bytes.Equal(hasher.Sum(nil), object.Checksum[:]) {
			logger.Warn("%x: corrupted file %s", snap.Header.GetIndexShortID(), pathname)
			complete = false
		}
		return complete, nil
	} else {
		return false, fmt.Errorf("unexpected vfs entry type")
	}
}

func (snap *Snapshot) Check(pathname string, fast bool) (bool, error) {
	snap.Repository().Context().Events().Send(events.SnapshotCheckStartEvent())
	defer snap.Repository().Context().Events().Send(events.SnapshotCheckDoneEvent())

	fs, err := snap.Filesystem()
	if err != nil {
		return false, err
	}
	return snapshotCheckPath(snap, fs, pathname, fast)
}
