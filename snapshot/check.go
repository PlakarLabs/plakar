package snapshot

import (
	"bytes"
	"crypto/sha256"
	"hash"

	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/progress"
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
		logger.Warn("%s: unlisted object %064x", snapshot.Metadata.GetIndexShortID(), checksum)
		return false, nil
	}

	if fast {
		exists, err := snapshot.CheckObject(checksum)
		if err != nil {
			logger.Warn("%s: could not check object %064x: %s", snapshot.Metadata.GetIndexShortID(), checksum, err)
			return false, nil
		}
		if !exists {
			return false, nil
		}
	} else {
		tmp, err := snapshot.GetObject(checksum)
		if err != nil {
			logger.Warn("%s: could not fetch object %064x: %s", snapshot.Metadata.GetIndexShortID(), checksum, err)
			return false, nil
		}
		object = tmp
	}

	ret := true
	objectHash := sha256.New()
	for _, chunkChecksum := range object.Chunks {
		_, err := snapshotCheckChunk(snapshot, chunkChecksum, objectHash, fast)
		if err != nil {
			logger.Warn("%s: chunk %064x: %s", snapshot.Metadata.GetIndexShortID(), chunkChecksum, err)
			continue
		}
	}

	if !fast {
		if !bytes.Equal(objectHash.Sum(nil), checksum[:]) {
			logger.Warn("%s: corrupted object %064x", snapshot.Metadata.GetIndexShortID(), checksum)
			ret = false
		}
	}
	return ret, nil
}

func snapshotCheckResource(snapshot *Snapshot, resource string, fast bool, showProgress bool) (bool, error) {
	object := snapshot.Index.LookupObjectForPathname(resource)
	if object == nil {
		logger.Warn("%s: no such file %s", snapshot.Metadata.GetIndexShortID(), resource)
		return false, nil
	}

	ret, err := snapshotCheckObject(snapshot, object.Checksum, fast)
	if err != nil {
		return false, err
	}
	return ret, nil
}

func snapshotCheckFull(snapshot *Snapshot, fast bool, showProgress bool) (bool, error) {
	var c chan int64

	ret := true
	if showProgress {
		c = progress.NewProgressCount("check", "checking chunks", int64(len(snapshot.Index.ListChunks())))
	} else {
		c = make(chan int64)
		go func() {
			for _ = range c {
			}
		}()
	}
	for _, checksum := range snapshot.Index.ListChunks() {
		if fast {
			exists, err := snapshot.CheckChunk(checksum)
			if err != nil {
				logger.Warn("%s: missing chunk %064x", snapshot.Metadata.GetIndexShortID(), checksum)
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
				logger.Warn("%s: missing chunk %064x", snapshot.Metadata.GetIndexShortID(), checksum)
				ret = false
				continue
			}

			chunkHash := sha256.New()
			chunkHash.Write(data)
			if !bytes.Equal(chunkHash.Sum(nil), checksum[:]) {
				logger.Warn("%s: corrupted chunk %064x", snapshot.Metadata.GetIndexShortID(), checksum)
				ret = false
				continue
			}
		}
		c <- 1

	}
	close(c)

	if showProgress {
		c = progress.NewProgressCount("check", "checking objects", int64(len(snapshot.Index.ListObjects())))
	} else {
		c = make(chan int64)
		go func() {
			for _ = range c {
			}
		}()
	}
	for _, checksum := range snapshot.Index.ListObjects() {
		if fast {
			exists, err := snapshot.CheckObject(checksum)
			if err != nil {
				logger.Warn("%s: missing object %064x", snapshot.Metadata.GetIndexShortID(), checksum)
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
				logger.Warn("%s: missing object %064x: %s", snapshot.Metadata.GetIndexShortID(), checksum, err)
				ret = false
				continue
			}

			objectHash := sha256.New()
			for _, chunkChecksum := range object.Chunks {
				indexChunk := snapshot.Index.LookupChunk(chunkChecksum)
				if indexChunk == nil {
					logger.Warn("%s: unlisted chunk %064x", snapshot.Metadata.GetIndexShortID(), chunkChecksum)
					ret = false
					continue
				}

				data, err := snapshot.GetChunk(chunkChecksum)
				if err != nil {
					logger.Warn("%s: missing chunk %064x", snapshot.Metadata.GetIndexShortID(), chunkChecksum)
					ret = false
					continue
				}
				objectHash.Write(data)
			}
			if !bytes.Equal(objectHash.Sum(nil), checksum[:]) {
				logger.Warn("%s: corrupted object %064x", snapshot.Metadata.GetIndexShortID(), checksum)
				ret = false
				continue
			}
		}
		c <- 1
	}
	close(c)

	if showProgress {
		c = progress.NewProgressCount("check", "checking pathnames", int64(len(snapshot.Filesystem.ListFiles())))
	} else {
		c = make(chan int64)
		go func() {
			for _ = range c {
			}
		}()
	}
	for _, file := range snapshot.Filesystem.ListFiles() {
		object := snapshot.Index.LookupObjectForPathname(file)
		if object == nil {
			logger.Warn("%s: unlisted object for file %s", snapshot.Metadata.GetIndexShortID(), file)
			ret = false
			continue
		}
		c <- 1

	}
	close(c)
	return ret, nil
}

func (snapshot *Snapshot) Check(resource string, fast bool, showProgress bool) (bool, error) {
	if resource != "" {
		return snapshotCheckResource(snapshot, resource, fast, showProgress)
	} else {
		return snapshotCheckFull(snapshot, fast, showProgress)
	}
}
