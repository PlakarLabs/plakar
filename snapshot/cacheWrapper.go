package snapshot

import (
	"fmt"
	"time"

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/PlakarLabs/plakar/vfs"
	"github.com/vmihailenco/msgpack/v5"
)

// CachedObject needs to be killed
type CachedObject struct {
	Checksum    [32]byte
	Chunks      []*objects.Chunk
	ContentType string
	Info        vfs.FileInfo
}

func (snapshot *Snapshot) GetCachedObject(pathname string) (*CachedObject, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetCachedObject", time.Since(t0))
	}()
	secret := snapshot.repository.GetSecret()
	cache := snapshot.repository.GetCache()

	pathHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
	pathHasher.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHasher.Sum(nil))

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

	data, err = compression.Inflate(snapshot.repository.Configuration().Compression, data)
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

func (snapshot *Snapshot) PutCachedObject(pathname string, object objects.Object, fi vfs.FileInfo) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutCachedObject", time.Since(t0))
	}()
	secret := snapshot.repository.GetSecret()
	cache := snapshot.repository.GetCache()

	pathHasher := encryption.GetHasher(snapshot.repository.Configuration().Hashing)
	pathHasher.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHasher.Sum(nil))

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

	jobject, err = compression.Deflate(snapshot.repository.Configuration().Compression, jobject)
	if err != nil {
		return err
	}

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
