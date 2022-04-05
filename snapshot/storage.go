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

	snapshot.Filesystem = snapshotStorage.Filesystem

	snapshot.Pathnames = snapshotStorage.Pathnames
	snapshot.Objects = snapshotStorage.Objects
	snapshot.Chunks = snapshotStorage.Chunks
	snapshot.ChunkToObjects = snapshotStorage.ChunkToObjects
	snapshot.ContentTypeToObjects = snapshotStorage.ContentTypeToObjects
	snapshot.ObjectToPathnames = snapshotStorage.ObjectToPathnames

	snapshot.Filesystem.Reindex()

	return snapshot, nil
}

func snapshotToBytes(snapshot *Snapshot) ([]byte, error) {
	snapshotStorage := SnapshotStorage{}
	snapshotStorage.Metadata = snapshot.Metadata

	snapshotStorage.Filesystem = snapshot.Filesystem

	snapshotStorage.Pathnames = snapshot.Pathnames
	snapshotStorage.Objects = snapshot.Objects
	snapshotStorage.Chunks = snapshot.Chunks
	snapshotStorage.ChunkToObjects = snapshot.ChunkToObjects
	snapshotStorage.ObjectToPathnames = snapshot.ObjectToPathnames
	snapshotStorage.ContentTypeToObjects = snapshot.ContentTypeToObjects

	serialized, err := json.Marshal(snapshotStorage)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}
