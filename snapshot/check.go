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
	object := snapshot.Index.LookupObject(checksum)
	if object == nil {
		logger.Warn("%s: unlisted object %064x", snapshot.Metadata.IndexID, checksum)
		return false, nil
	}

	if fast {
		exists, err := snapshot.CheckObject(checksum)
		if err != nil {
			logger.Warn("%s: could not check object %064x: %s", snapshot.Metadata.IndexID, checksum, err)
			return false, nil
		}
		if !exists {
			return false, nil
		}
	} else {
		tmp, err := snapshot.GetObject(checksum)
		if err != nil {
			logger.Warn("%s: could not fetch object %064x: %s", snapshot.Metadata.IndexID, checksum, err)
			return false, nil
		}
		object = tmp
	}

	ret := true
	objectHash := sha256.New()
	for _, chunkChecksum := range object.Chunks {
		_, err := snapshotCheckChunk(snapshot, chunkChecksum, objectHash, fast)
		if err != nil {
			logger.Warn("%s: chunk %064x: %s", snapshot.Metadata.IndexID, chunkChecksum, err)
			continue
		}
	}

	if !fast {
		if !bytes.Equal(objectHash.Sum(nil), checksum[:]) {
			logger.Warn("%s: corrupted object %064x", snapshot.Metadata.IndexID, checksum)
			ret = false
		}
	}
	return ret, nil
}

func snapshotCheckResource(snapshot *Snapshot, resource string, fast bool) (bool, error) {
	object := snapshot.Index.LookupObjectForPathname(resource)
	if object == nil {
		logger.Warn("%s: no such file %s", snapshot.Metadata.IndexID, resource)
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
				logger.Warn("%s: missing chunk %064x", snapshot.Metadata.IndexID, checksum)
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
				logger.Warn("%s: missing chunk %064x", snapshot.Metadata.IndexID, checksum)
				ret = false
				continue
			}

			chunkHash := sha256.New()
			chunkHash.Write(data)
			if !bytes.Equal(chunkHash.Sum(nil), checksum[:]) {
				logger.Warn("%s: corrupted chunk %064x", snapshot.Metadata.IndexID, checksum)
				ret = false
				continue
			}
		}
	}

	for _, checksum := range snapshot.Index.ListObjects() {
		if fast {
			exists, err := snapshot.CheckObject(checksum)
			if err != nil {
				logger.Warn("%s: missing object %064x", snapshot.Metadata.IndexID, checksum)
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
				logger.Warn("%s: missing object %064x: %s", snapshot.Metadata.IndexID, checksum, err)
				ret = false
				continue
			}

			objectHash := sha256.New()
			for _, chunkChecksum := range object.Chunks {
				_, ok := snapshot.Index.GetChunkInfo(chunkChecksum)
				if !ok {
					logger.Warn("%s: unlisted chunk %064x", snapshot.Metadata.IndexID, chunkChecksum)
					ret = false
					continue
				}

				data, err := snapshot.GetChunk(chunkChecksum)
				if err != nil {
					logger.Warn("%s: missing chunk %064x", snapshot.Metadata.IndexID, chunkChecksum)
					ret = false
					continue
				}
				objectHash.Write(data)
			}
			if !bytes.Equal(objectHash.Sum(nil), checksum[:]) {
				logger.Warn("%s: corrupted object %064x", snapshot.Metadata.IndexID, checksum)
				ret = false
				continue
			}
		}
	}

	for _, file := range snapshot.Index.Filesystem.ListFiles() {
		object := snapshot.Index.LookupObjectForPathname(file)
		if object == nil {
			logger.Warn("%s: unlisted object for file %064x", snapshot.Metadata.IndexID, file)
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
