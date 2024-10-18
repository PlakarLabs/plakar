package snapshot

import (
	"bytes"
	"hash"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

func snapshotCheckChunk(snapshot *Snapshot, chunkChecksum [32]byte, hasher hash.Hash, fast bool) (bool, error) {
	if fast {
		return snapshot.CheckChunk(chunkChecksum), nil
	}

	data, err := snapshot.GetChunk(chunkChecksum)
	if err != nil {
		return false, err
	}
	hasher.Write(data)
	return true, nil
}

func snapshotCheckObject(snapshot *Snapshot, checksum [32]byte, fast bool) (bool, error) {
	object, err := snapshot.LookupObject(checksum)
	if err != nil {
		logger.Warn("%s: missing object %064x", snapshot.Header.GetIndexShortID(), checksum)
		return false, nil
	}
	if object == nil {
		logger.Warn("%s: unlisted object %064x", snapshot.Header.GetIndexShortID(), checksum)
		return false, nil
	}

	if fast {
		exists := snapshot.CheckObject(checksum)
		if !exists {
			return false, nil
		}
	} else {
		tmp, err := snapshot.LookupObject(checksum)
		if err != nil {
			return false, err
		}
		if tmp != nil {
			return false, nil
		}
		object = tmp
	}

	ret := true

	objectHasher := snapshot.repository.Hasher()
	for _, chunk := range object.Chunks {
		_, err := snapshotCheckChunk(snapshot, chunk.Checksum, objectHasher, fast)
		if err != nil {
			logger.Warn("%s: chunk %064x: %s", snapshot.Header.GetIndexShortID(), chunk.Checksum, err)
			continue
		}
	}

	if !fast {
		if !bytes.Equal(objectHasher.Sum(nil), checksum[:]) {
			logger.Warn("%s: corrupted object %064x", snapshot.Header.GetIndexShortID(), checksum)
			ret = false
		}
	}
	return ret, nil
}

func snapshotCheckResource(snapshot *Snapshot, resource string, fast bool) (bool, error) {
	fs, err := snapshot.Filesystem()
	if err != nil {
		return false, err
	}

	fsinfo, err := fs.Stat(resource)
	if err != nil {
		return false, err
	}
	if _, isDir := fsinfo.(*vfs.DirEntry); isDir {
		logger.Warn("%s: is a directory %s", snapshot.Header.GetIndexShortID(), resource)
		return false, nil
	} else if fsinfo, isRegular := fsinfo.(*vfs.FileEntry); !isRegular {
		logger.Warn("%s: is not a regular file %s", snapshot.Header.GetIndexShortID(), resource)
		return false, nil
	} else if fsinfo.FileInfo().Mode().IsRegular() {
		object, err := snapshot.LookupObject(fsinfo.Checksum)
		if err != nil {
			return false, err
		}
		if object == nil {
			logger.Warn("%s: no such file %s", snapshot.Header.GetIndexShortID(), resource)
			return false, nil
		}
		return true, nil
	}
	return false, nil
}

func snapshotCheckFull(snapshot *Snapshot, fast bool) (bool, error) {
	ret := true
	for checksum := range snapshot.repository.State().ListChunks() {
		if fast {
			exists := snapshot.CheckChunk(checksum)
			if !exists {
				ret = false
				continue
			}
		} else {
			data, err := snapshot.GetChunk(checksum)
			if err != nil {
				logger.Warn("%s: missing chunk %064x", snapshot.Header.GetIndexShortID(), checksum)
				ret = false
				continue
			}

			tmp := snapshot.repository.Checksum(data)
			if !bytes.Equal(tmp[:], checksum[:]) {
				logger.Warn("%s: corrupted chunk %064x", snapshot.Header.GetIndexShortID(), checksum)
				ret = false
				continue
			}
		}
	}

	for checksum := range snapshot.repository.State().ListObjects() {
		if fast {
			exists := snapshot.CheckObject(checksum)
			if !exists {
				ret = false
				continue
			}
		} else {
			object, err := snapshot.LookupObject(checksum)
			if err != nil {
				logger.Warn("%s: 2missing object %064x", snapshot.Header.GetIndexShortID(), checksum, err)
				ret = false
				continue
			}
			objectHasher := snapshot.repository.Hasher()
			for _, chunk := range object.Chunks {
				data, err := snapshot.GetChunk(chunk.Checksum)
				if err != nil {
					logger.Warn("%s: missing chunk %064x", snapshot.Header.GetIndexShortID(), chunk.Checksum)
					ret = false
					continue
				}
				objectHasher.Write(data)
			}
			if !bytes.Equal(objectHasher.Sum(nil), checksum[:]) {
				logger.Warn("%s: corrupted object %064x", snapshot.Header.GetIndexShortID(), checksum)
				ret = false
				continue
			}
		}
	}

	fs, err := vfs.NewFilesystem(snapshot.repository, snapshot.Header.Root)
	if err != nil {
		return false, err
	}

	for filename := range fs.Files() {
		fsinfo, err := fs.Stat(filename)
		if err != nil {
			logger.Warn("%s: missing file %s", snapshot.Header.GetIndexShortID(), filename)
			ret = false
			continue
		}
		if _, isDir := fsinfo.(*vfs.DirEntry); isDir {
			logger.Warn("%s: is a directory %s", snapshot.Header.GetIndexShortID(), filename)
			ret = false
			continue
		}
		if fsinfo, isRegular := fsinfo.(*vfs.FileEntry); !isRegular {
			logger.Warn("%s: is not a regular file %s", snapshot.Header.GetIndexShortID(), filename)
			ret = false
			continue
		} else if fsinfo.FileInfo().Mode().IsRegular() {
			object, err := snapshot.LookupObject(fsinfo.Checksum)
			if err != nil {
				logger.Warn("%s: missing object for file %s", snapshot.Header.GetIndexShortID(), filename)
				ret = false
				continue
			}
			if object == nil {
				logger.Warn("%s: unlisted object for file %s", snapshot.Header.GetIndexShortID(), filename)
				ret = false
				continue
			}
		}
	}
	return ret, nil
}

func (snapshot *Snapshot) Check(resource string, fast bool) (bool, error) {
	if resource != "" && resource != "/" {
		return snapshotCheckResource(snapshot, resource, fast)
	} else {
		return snapshotCheckFull(snapshot, fast)
	}
}
