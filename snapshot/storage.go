package snapshot

import (
	"encoding/json"
)

func snapshotFromBytes(data []byte) (*Snapshot, error) {
	var snapshotStorage SnapshotStorage
	if err := json.Unmarshal(data, &snapshotStorage); err != nil {
		return nil, err
	}

	snapshot := &Snapshot{}
	snapshot.Metadata = snapshotStorage.Metadata
	snapshot.Index = snapshotStorage.Index
	snapshot.Index.Filesystem.Reindex()

	return snapshot, nil
}

func snapshotToBytes(snapshot *Snapshot) ([]byte, error) {
	snapshotStorage := SnapshotStorage{}
	snapshotStorage.Metadata = snapshot.Metadata
	snapshotStorage.Index = snapshot.Index

	serialized, err := json.Marshal(snapshotStorage)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}
