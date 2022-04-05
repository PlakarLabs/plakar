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

func indexFromBytes(data []byte) (*Index, error) {
	var index Index
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, err
	}
	index.Filesystem.Reindex()
	return &index, nil
}

func indexToBytes(index *Index) ([]byte, error) {
	serialized, err := json.Marshal(index)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}

func metadataFromBytes(data []byte) (*Metadata, error) {
	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func metadataToBytes(metadata *Metadata) ([]byte, error) {
	serialized, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}
