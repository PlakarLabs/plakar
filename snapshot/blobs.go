package snapshot

import (
	"io"
	"time"

	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/profiler"
)

func (snap *Snapshot) PutBlob(blobType packfile.BlobType, checksum [32]byte, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.PutBlob", time.Since(t0))
	}()
	logger.Trace("snapshot", "%x: PutBlob(%064x)", snap.Header.GetIndexShortID(), checksum)

	encoded, err := snap.repository.Encode(data)
	if err != nil {
		return err
	}

	snap.packerChan <- &PackerMsg{Type: blobType, Timestamp: time.Now(), Checksum: checksum, Data: encoded}
	return nil
}

func (snapshot *Snapshot) GetBlob(blobType packfile.BlobType, checksum [32]byte) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.GetBlob", time.Since(t0))
	}()
	logger.Trace("snapshot", "%x: GetBlob(%x)", snapshot.Header.GetIndexShortID(), checksum)

	rd, _, err := snapshot.repository.GetBlob(blobType, checksum)
	if err != nil {
		return nil, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (snapshot *Snapshot) BlobExists(blobType packfile.BlobType, checksum [32]byte) bool {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.CheckBlob", time.Since(t0))
	}()
	logger.Trace("snapshot", "%x: CheckBlob(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	return snapshot.repository.BlobExists(blobType, checksum)
}
