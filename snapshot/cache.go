package snapshot

import (
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/poolpOrg/plakar/compression"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/filesystem"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/objects"
	"github.com/poolpOrg/plakar/profiler"
	"github.com/vmihailenco/msgpack/v5"
)

// CachedObject needs to be killed
type CachedObject struct {
	Checksum    [32]byte
	Chunks      []*objects.Chunk
	ContentType string
	Info        filesystem.Fileinfo
}

func (snapshot *Snapshot) GetCachedObject(pathname string) (*CachedObject, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetCachedObject", time.Since(t0))
	}()
	secret := snapshot.repository.GetSecret()
	cache := snapshot.repository.GetCache()

	pathHash := sha256.New()
	pathHash.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHash.Sum(nil))

	data, err := cache.GetPath(snapshot.repository.Configuration().RepositoryID.String(), hashedPath)
	if err != nil {
		logger.Trace("snapshot", "%s: cache.GetPath(%s): KO", snapshot.Metadata.GetIndexShortID(), pathname)
		return nil, err
	}
	logger.Trace("snapshot", "%s: cache.GetPath(%s): OK", snapshot.Metadata.GetIndexShortID(), pathname)

	if snapshot.repository.Configuration().Encryption != "" {
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
	err = msgpack.Unmarshal(data, &cacheObject)
	if err != nil {
		return nil, err
	}
	//	cacheObject.Info.path = pathname
	return &cacheObject, nil
}

func (snapshot *Snapshot) PutCachedObject(pathname string, object objects.Object, fi filesystem.Fileinfo) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutCachedObject", time.Since(t0))
	}()
	secret := snapshot.repository.GetSecret()
	cache := snapshot.repository.GetCache()

	pathHash := sha256.New()
	pathHash.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHash.Sum(nil))

	cacheObject := CachedObject{}
	cacheObject.Checksum = object.Checksum

	cacheObject.Chunks = make([]*objects.Chunk, 0)
	for _, chunkChecksum := range object.Chunks {
		chunk := snapshot.Index.LookupChunk(chunkChecksum)
		cacheObject.Chunks = append(cacheObject.Chunks, chunk)
	}

	cacheObject.ContentType = object.ContentType
	cacheObject.Info = fi

	jobject, err := msgpack.Marshal(cacheObject)
	if err != nil {
		return err
	}

	jobject = compression.Deflate(jobject)
	if snapshot.repository.Configuration().Encryption != "" {
		tmp, err := encryption.Encrypt(secret, jobject)
		if err != nil {
			return err
		}
		jobject = tmp
	}

	logger.Trace("snapshot", "%s: cache.PutPath(%s)", snapshot.Metadata.GetIndexShortID(), pathname)
	cache.PutPath(snapshot.repository.Configuration().RepositoryID.String(), hashedPath, jobject)
	return nil
}
