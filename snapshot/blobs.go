package snapshot

import (
	"io"
	"time"

	"github.com/PlakarKorp/plakar/packfile"
)

func (snap *Snapshot) PutBlob(Type packfile.Type, checksum [32]byte, data []byte) error {
	snap.Logger().Trace("snapshot", "%x: PutBlob(%064x)", snap.Header.GetIndexShortID(), checksum)

	encoded, err := snap.repository.EncodeBuffer(data)
	if err != nil {
		return err
	}

	snap.packerChan <- &PackerMsg{Type: Type, Timestamp: time.Now(), Checksum: checksum, Data: encoded}
	return nil
}

func (snap *Snapshot) GetBlob(Type packfile.Type, checksum [32]byte) ([]byte, error) {
	snap.Logger().Trace("snapshot", "%x: GetBlob(%x)", snap.Header.GetIndexShortID(), checksum)

	rd, _, err := snap.repository.GetBlob(Type, checksum)
	if err != nil {
		return nil, err
	}

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (snap *Snapshot) BlobExists(Type packfile.Type, checksum [32]byte) bool {
	snap.Logger().Trace("snapshot", "%x: CheckBlob(%064x)", snap.Header.GetIndexShortID(), checksum)

	return snap.repository.BlobExists(Type, checksum)
}
