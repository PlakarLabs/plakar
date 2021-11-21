package snapshot

import (
	"crypto/sha256"
	"fmt"
	"hash"

	"github.com/poolpOrg/plakar/logger"
)

func snapshotCheckChunk(snapshot *Snapshot, chunk *Chunk, hasher hash.Hash, fast bool) (bool, error) {
	if fast {
		exists, err := snapshot.CheckChunk(chunk.Checksum)
		if err != nil {
			return false, err
		}
		return exists, nil
	}

	data, err := snapshot.GetChunk(chunk.Checksum)
	if err != nil {
		return false, err
	}
	hasher.Write(data)
	return true, nil
}

func snapshotCheckObject(snapshot *Snapshot, checksum string, fast bool) (bool, error) {
	object, ok := snapshot.Objects[checksum]
	if !ok {
		logger.Warn("%s: unlisted object %s", snapshot.Uuid, checksum)
		return false, nil
	}

	if fast {
		exists, err := snapshot.CheckObject(checksum)
		if err != nil {
			logger.Warn("%s: could not check object %s: %s", snapshot.Uuid, checksum, err)
			return false, nil
		}
		if !exists {
			return false, nil
		}
	} else {
		tmp, err := snapshot.GetObject(checksum)
		if err != nil {
			logger.Warn("%s: could not fetch object %s: %s", snapshot.Uuid, checksum, err)
			return false, nil
		}
		object = tmp
	}

	ret := true
	objectHash := sha256.New()
	for _, chunk := range object.Chunks {
		_, err := snapshotCheckChunk(snapshot, chunk, objectHash, fast)
		if err != nil {
			logger.Warn("%s: chunk %s: %s", snapshot.Uuid, chunk.Checksum, err)
			continue
		}
	}

	if !fast {
		if fmt.Sprintf("%032x", objectHash.Sum(nil)) != checksum {
			logger.Warn("%s: corrupted object %s", snapshot.Uuid, checksum)
			ret = false
		}
	}
	return ret, nil
}

func snapshotCheckResource(snapshot *Snapshot, resource string, fast bool) (bool, error) {
	checksum, ok := snapshot.Pathnames[resource]
	if !ok {
		logger.Warn("%s: no such file %s", snapshot.Uuid, resource)
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
	for _, chunk := range snapshot.Chunks {
		if fast {
			exists, err := snapshot.CheckChunk(chunk.Checksum)
			if err != nil {
				logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
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
				logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
				ret = false
				continue
			}

			chunkHash := sha256.New()
			chunkHash.Write(data)
			if fmt.Sprintf("%032x", chunkHash.Sum(nil)) != chunk.Checksum {
				logger.Warn("%s: corrupted chunk %s", snapshot.Uuid, chunk.Checksum)
				ret = false
				continue
			}
		}
	}

	for checksum := range snapshot.Objects {
		if fast {
			exists, err := snapshot.CheckObject(checksum)
			if err != nil {
				logger.Warn("%s: missing object %s", snapshot.Uuid, checksum)
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
				logger.Warn("%s: missing object %s", snapshot.Uuid, checksum)
				ret = false
				continue
			}

			objectHash := sha256.New()
			for _, chunk := range object.Chunks {
				_, ok := snapshot.Chunks[chunk.Checksum]
				if !ok {
					logger.Warn("%s: unlisted chunk %s", snapshot.Uuid, chunk.Checksum)
					ret = false
					continue
				}

				data, err := snapshot.GetChunk(chunk.Checksum)
				if err != nil {
					logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
					ret = false
					continue
				}
				objectHash.Write(data)
			}
			if fmt.Sprintf("%032x", objectHash.Sum(nil)) != checksum {
				logger.Warn("%s: corrupted object %s", snapshot.Uuid, checksum)
				ret = false
				continue
			}
		}
	}

	for file := range snapshot.Filesystem.Files {
		checksum, ok := snapshot.Pathnames[file]
		if !ok {
			logger.Warn("%s: unlisted file %s", snapshot.Uuid, file)
			ret = false
			continue
		}
		_, ok = snapshot.Objects[checksum]
		if !ok {
			logger.Warn("%s: unlisted object %s", snapshot.Uuid, checksum)
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
