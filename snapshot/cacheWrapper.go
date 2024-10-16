package snapshot

import (
	"fmt"
	"time"

	"github.com/PlakarLabs/plakar/hashing"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/vmihailenco/msgpack/v5"
)

// CachedObject needs to be killed
type CachedObject struct {
	Checksum    [32]byte
	Chunks      []*objects.Chunk
	ContentType string
	Info        objects.FileInfo
}

func (snapshot *Snapshot) GetCachedObject(pathname string) (*CachedObject, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetCachedObject", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()

	pathHasher := hashing.GetHasher(snapshot.repository.Configuration().Hashing)
	pathHasher.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHasher.Sum(nil))

	data, err := cache.GetPath(snapshot.repository.Configuration().StoreID.String(), hashedPath)
	if err != nil {
		logger.Trace("snapshot", "%s: cache.GetPath(%s): KO", snapshot.Header.GetIndexShortID(), pathname)
		return nil, err
	}
	logger.Trace("snapshot", "%s: cache.GetPath(%s): OK", snapshot.Header.GetIndexShortID(), pathname)

	cacheObject := CachedObject{}
	err = msgpack.Unmarshal(data, &cacheObject)
	if err != nil {
		return nil, err
	}
	return &cacheObject, nil
}

func (snapshot *Snapshot) PutCachedObject(pathname string, object objects.Object, fi objects.FileInfo) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutCachedObject", time.Since(t0))
	}()
	cache := snapshot.repository.GetCache()

	pathHasher := hashing.GetHasher(snapshot.repository.Configuration().Hashing)
	pathHasher.Write([]byte(pathname))
	hashedPath := fmt.Sprintf("%032x", pathHasher.Sum(nil))

	cacheObject := CachedObject{}
	cacheObject.Checksum = object.Checksum

	cacheObject.Chunks = make([]*objects.Chunk, 0)
	for _, chunkChecksum := range object.Chunks {
		chunk, err := snapshot.Index.LookupChunk(chunkChecksum)
		if err != nil {
			return err
		}
		cacheObject.Chunks = append(cacheObject.Chunks, chunk)
	}

	cacheObject.ContentType = object.ContentType
	cacheObject.Info = fi

	jobject, err := msgpack.Marshal(cacheObject)
	if err != nil {
		return err
	}

	logger.Trace("snapshot", "%s: cache.PutPath(%s)", snapshot.Header.GetIndexShortID(), pathname)
	cache.PutPath(snapshot.repository.Configuration().StoreID.String(), hashedPath, jobject)
	return nil
}
