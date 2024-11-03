package snapshot

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

func snapshotCheckPath(snapshot *Snapshot, pathname string, fast bool) (bool, error) {
	fs, err := snapshot.Filesystem()
	if err != nil {
		return false, err
	}
	fsinfo, err := fs.Stat(pathname)
	if err != nil {
		return false, err
	}
	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		complete := true
		for _, child := range dirEntry.Children {
			ok, err := snapshotCheckPath(snapshot, filepath.Join(pathname, child.FileInfo.Name()), fast)
			if err != nil || !ok {
				complete = false
			}
		}
		return complete, err
	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile && fileEntry.FileInfo().Mode().IsRegular() {
		object, err := snapshot.LookupObject(fileEntry.Checksum)
		if err != nil {
			return false, fmt.Errorf("missing object for file %s", pathname)
		}

		complete := true
		hasher := snapshot.repository.Hasher()
		for _, chunk := range object.Chunks {
			if fast {
				exists := snapshot.CheckChunk(chunk.Checksum)
				if !exists {
					logger.Warn("%x: missing chunk %x for file %s", snapshot.Header.GetIndexShortID(), chunk.Checksum, pathname)
					complete = false
				}
			} else {
				exists := snapshot.CheckChunk(chunk.Checksum)
				if !exists {
					logger.Warn("%x: missing chunk %x for file %s", snapshot.Header.GetIndexShortID(), chunk.Checksum, pathname)
					complete = false
				}
				data, err := snapshot.GetChunk(chunk.Checksum)
				if err != nil {
					logger.Warn("%x: missing chunk %x for file %s: %s", snapshot.Header.GetIndexShortID(), chunk.Checksum, pathname, err)
					complete = false
				}

				hasher.Write(data)

				checksum := snapshot.repository.Checksum(data)
				if !bytes.Equal(checksum[:], chunk.Checksum[:]) {
					logger.Warn("%x: corrupted chunk %x for file %s", snapshot.Header.GetIndexShortID(), chunk.Checksum, pathname)
					complete = false
				}
			}
		}
		if !bytes.Equal(hasher.Sum(nil), object.Checksum[:]) {
			logger.Warn("%x: corrupted file %s", snapshot.Header.GetIndexShortID(), pathname)
			complete = false
		}
		return complete, nil
	} else {
		return false, fmt.Errorf("unexpected vfs entry type")
	}
}

func (snap *Snapshot) Check(pathname string, fast bool) (bool, error) {
	return snapshotCheckPath(snap, pathname, fast)
}
