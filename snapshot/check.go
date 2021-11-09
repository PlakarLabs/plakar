package snapshot

import (
	"crypto/sha256"
	"fmt"

	"github.com/poolpOrg/plakar/logger"
)

func snapshotCheckResource(snapshot *Snapshot, resource string) bool {
	checksum, ok := snapshot.Pathnames[resource]
	if !ok {
		logger.Warn("%s: unlisted file %s", snapshot.Uuid, resource)
		return false
	}

	object, ok := snapshot.Objects[checksum]
	if !ok {
		logger.Warn("%s: unlisted object %s", snapshot.Uuid, checksum)
		return false
	}

	ret := true
	objectHash := sha256.New()
	for _, chunk := range object.Chunks {
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
	}
	return ret
}

func snapshotFastCheckResource(snapshot *Snapshot, resource string) bool {
	checksum, ok := snapshot.Pathnames[resource]
	if !ok {
		logger.Warn("%s: unlisted file %s", snapshot.Uuid, resource)
		return false
	}

	object, ok := snapshot.Objects[checksum]
	if !ok {
		logger.Warn("%s: unlisted object %s", snapshot.Uuid, checksum)
		return false
	}

	ret := true
	for _, chunk := range object.Chunks {
		exists, err := snapshot.CheckChunk(chunk.Checksum)
		if err != nil {
			logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
			ret = false
			continue
		}
		if !exists {
			logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
			ret = false
			continue
		}
	}
	return ret
}

func (snapshot *Snapshot) Check(resource string) bool {
	if resource != "" {
		return snapshotCheckResource(snapshot, resource)
	}

	ret := true
	for _, chunk := range snapshot.Chunks {
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

	for checksum := range snapshot.Objects {
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

	for file := range snapshot.Files {
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
	return ret
}

func (snapshot *Snapshot) FastCheck(resource string) bool {
	if resource != "" {
		return snapshotFastCheckResource(snapshot, resource)
	}

	ret := true
	for _, chunk := range snapshot.Chunks {
		exists, err := snapshot.CheckChunk(chunk.Checksum)
		if err != nil {
			logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
			ret = false
			continue
		}
		if !exists {
			logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
			ret = false
			continue
		}
	}

	for checksum := range snapshot.Objects {
		exists, err := snapshot.CheckObject(checksum)
		if err != nil {
			logger.Warn("%s: missing object %s", snapshot.Uuid, checksum)
			ret = false
			continue
		}
		if !exists {
			logger.Warn("%s: missing object %s", snapshot.Uuid, checksum)
			ret = false
			continue
		}
	}

	for file := range snapshot.Files {
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
	return ret
}
