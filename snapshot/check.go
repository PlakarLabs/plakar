package snapshot

import (
	"bytes"
	"crypto/sha256"
	"hash"

	"github.com/poolpOrg/plakar/logger"
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
	object, ok := snapshot.Index.Objects[checksum]
	if !ok {
		logger.Warn("%s: unlisted object %064x", snapshot.Metadata.Uuid, checksum)
		return false, nil
	}

	if fast {
		exists, err := snapshot.CheckObject(checksum)
		if err != nil {
			logger.Warn("%s: could not check object %064x: %s", snapshot.Metadata.Uuid, checksum, err)
			return false, nil
		}
		if !exists {
			return false, nil
		}
	} else {
		tmp, err := snapshot.GetObject(checksum)
		if err != nil {
			logger.Warn("%s: could not fetch object %064x: %s", snapshot.Metadata.Uuid, checksum, err)
			return false, nil
		}
		object = tmp
	}

	ret := true
	objectHash := sha256.New()
	for _, chunkChecksum := range object.Chunks {
		_, err := snapshotCheckChunk(snapshot, chunkChecksum, objectHash, fast)
		if err != nil {
			logger.Warn("%s: chunk %064x: %s", snapshot.Metadata.Uuid, chunkChecksum, err)
			continue
		}
	}

	if !fast {
		if !bytes.Equal(objectHash.Sum(nil), checksum[:]) {
			logger.Warn("%s: corrupted object %064x", snapshot.Metadata.Uuid, checksum)
			ret = false
		}
	}
	return ret, nil
}

func snapshotCheckResource(snapshot *Snapshot, resource string, fast bool) (bool, error) {
	checksum, ok := snapshot.Index.Pathnames[resource]
	if !ok {
		logger.Warn("%s: no such file %s", snapshot.Metadata.Uuid, resource)
		return false, nil
	}

	ret, err := snapshotCheckObject(snapshot, checksum, fast)
	if err != nil {
		return false, err
	}
	return ret, nil
}

func snapshotCheckFull(snapshot *Snapshot, fast bool) (bool, error) {
	ret := true
	for _, chunk := range snapshot.Index.Chunks {
		if fast {
			exists, err := snapshot.CheckChunk(chunk.Checksum)
			if err != nil {
				logger.Warn("%s: missing chunk %064x", snapshot.Metadata.Uuid, chunk.Checksum)
				ret = false
				continue
			}
			if !exists {
				ret = false
				continue
			}
		} else {
			data, err := snapshot.GetChunk(chunk.Checksum)
			if err != nil {
				logger.Warn("%s: missing chunk %064x", snapshot.Metadata.Uuid, chunk.Checksum)
				ret = false
				continue
			}

			chunkHash := sha256.New()
			chunkHash.Write(data)
			if !bytes.Equal(chunkHash.Sum(nil), chunk.Checksum[:]) {
				logger.Warn("%s: corrupted chunk %064x", snapshot.Metadata.Uuid, chunk.Checksum)
				ret = false
				continue
			}
		}
	}

	for checksum := range snapshot.Index.Objects {
		if fast {
			exists, err := snapshot.CheckObject(checksum)
			if err != nil {
				logger.Warn("%s: missing object %064x", snapshot.Metadata.Uuid, checksum)
				ret = false
				continue
			}
			if !exists {
				ret = false
				continue
			}
		} else {
			object, err := snapshot.GetObject(checksum)
			if err != nil {
				logger.Warn("%s: missing object %064x", snapshot.Metadata.Uuid, checksum)
				ret = false
				continue
			}

			objectHash := sha256.New()
			for _, chunkChecksum := range object.Chunks {
				_, ok := snapshot.GetChunkInfo(chunkChecksum)
				if !ok {
					logger.Warn("%s: unlisted chunk %064x", snapshot.Metadata.Uuid, chunkChecksum)
					ret = false
					continue
				}

				data, err := snapshot.GetChunk(chunkChecksum)
				if err != nil {
					logger.Warn("%s: missing chunk %064x", snapshot.Metadata.Uuid, chunkChecksum)
					ret = false
					continue
				}
				objectHash.Write(data)
			}
			if !bytes.Equal(objectHash.Sum(nil), checksum[:]) {
				logger.Warn("%s: corrupted object %064x", snapshot.Metadata.Uuid, checksum)
				ret = false
				continue
			}
		}
	}

	for _, file := range snapshot.Index.Filesystem.ListFiles() {
		checksum, ok := snapshot.Index.Pathnames[file]
		if !ok {
			logger.Warn("%s: unlisted file %s", snapshot.Metadata.Uuid, file)
			ret = false
			continue
		}
		_, ok = snapshot.Index.Objects[checksum]
		if !ok {
			logger.Warn("%s: unlisted object %064x", snapshot.Metadata.Uuid, checksum)
			ret = false
			continue
		}
	}
	return ret, nil
}

func (snapshot *Snapshot) Check(resource string, fast bool) (bool, error) {
	if resource != "" {
		return snapshotCheckResource(snapshot, resource, fast)
	} else {
		return snapshotCheckFull(snapshot, fast)
	}
}
