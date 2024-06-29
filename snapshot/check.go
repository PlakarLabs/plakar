package snapshot

import (
	"bytes"
	"hash"

	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/logger"
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
	object, err := snapshot.Index.LookupObject(checksum)
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
		tmp, err := snapshot.Index.LookupObject(checksum)
		if err != nil {
			return false, err
		}
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
	hasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
	hasher.Write([]byte(resource))
	pathnameChecksum := hasher.Sum(nil)
	key := [32]byte{}
	copy(key[:], pathnameChecksum)

	object, err := snapshot.Index.LookupObjectForPathnameChecksum(key)
	if err != nil {
		return false, err
	}
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
	for checksum := range snapshot.Index.ListChunks() {
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

			chunkHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
			chunkHasher.Write(data)
			if !bytes.Equal(chunkHasher.Sum(nil), checksum[:]) {
				logger.Warn("%s: corrupted chunk %064x", snapshot.Header.GetIndexShortID(), checksum)
				ret = false
				continue
			}
		}
	}

	for checksum := range snapshot.Index.ListObjects() {
		if fast {
			exists := snapshot.CheckObject(checksum)
			if !exists {
				ret = false
				continue
			}
		} else {
			object, err := snapshot.Index.LookupObject(checksum)
			if err != nil {
				logger.Warn("%s: missing object %064x", snapshot.Header.GetIndexShortID(), checksum)
				ret = false
				continue
			}
			objectHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
			for _, chunkChecksum := range object.Chunks {
				indexChunk, err := snapshot.Index.LookupChunk(chunkChecksum)
				if err != nil {
					logger.Warn("%s: missing chunk %064x", snapshot.Header.GetIndexShortID(), chunkChecksum)
					ret = false
					continue
				}
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

	for filename := range snapshot.Filesystem.Files() {
		hasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
		hasher.Write([]byte(filename))
		pathnameChecksum := hasher.Sum(nil)
		key := [32]byte{}
		copy(key[:], pathnameChecksum)
		object, err := snapshot.Index.LookupObjectForPathnameChecksum(key)
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
	return ret, nil
}

func (snapshot *Snapshot) Check(resource string, fast bool) (bool, error) {
	if resource != "" && resource != "/" {
		return snapshotCheckResource(snapshot, resource, fast)
	} else {
		return snapshotCheckFull(snapshot, fast)
	}
}
