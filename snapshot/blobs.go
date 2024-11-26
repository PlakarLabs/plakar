package snapshot

import (
	"io"
	"time"

	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/packfile"
)

func (snap *Snapshot) PutBlob(Type packfile.Type, checksum [32]byte, data []byte) error {
	logger.Trace("snapshot", "%x: PutBlob(%064x)", snap.Header.GetIndexShortID(), checksum)

	encoded, err := snap.repository.Encode(data)
	if err != nil {
		return err
	}

	snap.packerChan <- &PackerMsg{Type: Type, Timestamp: time.Now(), Checksum: checksum, Data: encoded}
	return nil
}

func (snapshot *Snapshot) GetBlob(Type packfile.Type, checksum [32]byte) ([]byte, error) {
	logger.Trace("snapshot", "%x: GetBlob(%x)", snapshot.Header.GetIndexShortID(), checksum)

	rd, _, err := snapshot.repository.GetBlob(Type, checksum)
	if err != nil {
		return nil, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (snapshot *Snapshot) BlobExists(Type packfile.Type, checksum [32]byte) bool {
	logger.Trace("snapshot", "%x: CheckBlob(%064x)", snapshot.Header.GetIndexShortID(), checksum)

	return snapshot.repository.BlobExists(Type, checksum)
}
