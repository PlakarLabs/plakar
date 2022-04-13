package snapshot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/filesystem"
	"github.com/poolpOrg/plakar/logger"
)

func (snapshot *Snapshot) GetCachedObject(pathname string) (*CachedObject, error) {
	secret := snapshot.store.GetSecret()
	cache := snapshot.store.GetCache()

	pathHash := sha256.New()
	pathHash.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHash.Sum(nil))

	data, err := cache.GetPath(hashedPath)
	if err != nil {
		logger.Trace("%s: cache.GetPath(%s): KO", snapshot.Metadata.Uuid, pathname)
		return nil, err
	}
	logger.Trace("%s: cache.GetPath(%s): OK", snapshot.Metadata.Uuid, pathname)

	if snapshot.store.Configuration().Encryption != "" {
		tmp, err := encryption.Decrypt(secret, data)
		if err != nil {
			return nil, err
		}
		data = tmp
	}

	data, err = compression.Inflate(data)
	if err != nil {
		return nil, err
	}

	cacheObject := CachedObject{}
	err = json.Unmarshal(data, &cacheObject)
	if err != nil {
		return nil, err
	}
	//	cacheObject.Info.path = pathname
	return &cacheObject, nil
}

func (snapshot *Snapshot) PutCachedObject(pathname string, object Object, fi filesystem.Fileinfo) error {
	secret := snapshot.store.GetSecret()
	cache := snapshot.store.GetCache()

	pathHash := sha256.New()
	pathHash.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHash.Sum(nil))

	cacheObject := CachedObject{}
	cacheObject.Checksum = object.Checksum

	cacheObject.Chunks = make([]*Chunk, 0)
	for _, chunkChecksum := range object.Chunks {
		chunk, _ := snapshot.GetChunkInfo(chunkChecksum)
		cacheObject.Chunks = append(cacheObject.Chunks, chunk)
	}

	cacheObject.ContentType = object.ContentType
	cacheObject.Info = fi

	jobject, err := json.Marshal(cacheObject)
	if err != nil {
		return err
	}

	jobject = compression.Deflate(jobject)
	if snapshot.store.Configuration().Encryption != "" {
		tmp, err := encryption.Encrypt(secret, jobject)
		if err != nil {
			return err
		}
		jobject = tmp
	}

	logger.Trace("%s: cache.PutPath(%s)", snapshot.Metadata.Uuid, pathname)
	cache.PutPath(hashedPath, jobject)
	return nil
}
