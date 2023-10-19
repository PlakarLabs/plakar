package snapshot

import (
	"bytes"
	"hash"

	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/logger"
)

func snapshotCheckChunk(snapshot *Snapshot, chunkChecksum [32]byte, hasher hash.Hash, fast bool) (bool, error) {
	if fast {
		exists, err := snapshot.CheckChunk(chunkChecksum)
		if err != nil {
			return false, err
		}
		return exists, nil
	}

	data, err := snapshot.GetChunk(chunkChecksum)
	if err != nil {
		return false, err
	}
	hasher.Write(data)
	return true, nil
}

func snapshotCheckObject(snapshot *Snapshot, checksum [32]byte, fast bool) (bool, error) {
	object := snapshot.Index.LookupObject(checksum)
	if object == nil {
		logger.Warn("%s: unlisted object %064x", snapshot.Header.GetIndexShortID(), checksum)
		return false, nil
	}

	if fast {
		exists, err := snapshot.CheckObject(checksum)
		if err != nil {
			logger.Warn("%s: could not check object %064x: %s", snapshot.Header.GetIndexShortID(), checksum, err)
			return false, nil
		}
		if !exists {
			return false, nil
		}
	} else {
		tmp := snapshot.Index.GetObject(checksum)
		if tmp != nil {
			return false, nil
		}
		object = tmp
	}

	ret := true

	objectHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
	for _, chunkChecksum := range object.Chunks {
		_, err := snapshotCheckChunk(snapshot, chunkChecksum, objectHasher, fast)
		if err != nil {
			logger.Warn("%s: chunk %064x: %s", snapshot.Header.GetIndexShortID(), chunkChecksum, err)
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
	pathnameID := snapshot.Filesystem.GetPathnameID(resource)
	object := snapshot.Index.LookupObjectForPathname(pathnameID)
	if object == nil {
		logger.Warn("%s: no such file %s", snapshot.Header.GetIndexShortID(), resource)
		return false, nil
	}

	ret, err := snapshotCheckObject(snapshot, object.Checksum, fast)
	if err != nil {
		return false, err
	}
	return ret, nil
}

func snapshotCheckFull(snapshot *Snapshot, fast bool) (bool, error) {
	ret := true
	for _, checksum := range snapshot.Index.ListChunks() {
		if fast {
			exists, err := snapshot.CheckChunk(checksum)
			if err != nil {
				logger.Warn("%s: missing chunk %064x", snapshot.Header.GetIndexShortID(), checksum)
				ret = false
				continue
			}
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

			chunkHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
			chunkHasher.Write(data)
			if !bytes.Equal(chunkHasher.Sum(nil), checksum[:]) {
				logger.Warn("%s: corrupted chunk %064x", snapshot.Header.GetIndexShortID(), checksum)
				ret = false
				continue
			}
		}
	}

	for _, checksum := range snapshot.Index.ListObjects() {
		if fast {
			exists, err := snapshot.CheckObject(checksum)
			if err != nil {
				logger.Warn("%s: missing object %064x", snapshot.Header.GetIndexShortID(), checksum)
				ret = false
				continue
			}
			if !exists {
				ret = false
				continue
			}
		} else {
			object := snapshot.Index.GetObject(checksum)
			objectHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
			for _, chunkChecksum := range object.Chunks {
				indexChunk := snapshot.Index.LookupChunk(chunkChecksum)
				if indexChunk == nil {
					logger.Warn("%s: unlisted chunk %064x", snapshot.Header.GetIndexShortID(), chunkChecksum)
					ret = false
					continue
				}

				data, err := snapshot.GetChunk(chunkChecksum)
				if err != nil {
					logger.Warn("%s: missing chunk %064x", snapshot.Header.GetIndexShortID(), chunkChecksum)
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

	for _, file := range snapshot.Filesystem.ListFiles() {
		pathnameID := snapshot.Filesystem.GetPathnameID(file)
		object := snapshot.Index.LookupObjectForPathname(pathnameID)
		if object == nil {
			logger.Warn("%s: unlisted object for file %s", snapshot.Header.GetIndexShortID(), file)
			ret = false
			continue
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
