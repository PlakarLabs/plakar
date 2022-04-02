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
	snapshot.Uuid = snapshotStorage.Uuid
	snapshot.CreationTime = snapshotStorage.CreationTime
	snapshot.Version = snapshotStorage.Version
	snapshot.Hostname = snapshotStorage.Hostname
	snapshot.Username = snapshotStorage.Username
	snapshot.CommandLine = snapshotStorage.CommandLine
	snapshot.MachineID = snapshotStorage.MachineID

	snapshot.Filesystem = snapshotStorage.Filesystem

	snapshot.Pathnames = snapshotStorage.Pathnames
	snapshot.Objects = snapshotStorage.Objects
	snapshot.Chunks = snapshotStorage.Chunks
	snapshot.ChunkToObjects = snapshotStorage.ChunkToObjects
	snapshot.ContentTypeToObjects = snapshotStorage.ContentTypeToObjects
	snapshot.ObjectToPathnames = snapshotStorage.ObjectToPathnames

	snapshot.Size = snapshotStorage.Size

	snapshot.Filesystem.Reindex()

	return snapshot, nil
}

func snapshotToBytes(snapshot *Snapshot) ([]byte, error) {
	snapshotStorage := SnapshotStorage{}
	snapshotStorage.Uuid = snapshot.Uuid
	snapshotStorage.CreationTime = snapshot.CreationTime
	snapshotStorage.Version = snapshot.Version
	snapshotStorage.Hostname = snapshot.Hostname
	snapshotStorage.Username = snapshot.Username
	snapshotStorage.CommandLine = snapshot.CommandLine
	snapshotStorage.MachineID = snapshot.MachineID

	snapshotStorage.Filesystem = snapshot.Filesystem

	snapshotStorage.Pathnames = snapshot.Pathnames
	snapshotStorage.Objects = snapshot.Objects
	snapshotStorage.Chunks = snapshot.Chunks
	snapshotStorage.ChunkToObjects = snapshot.ChunkToObjects
	snapshotStorage.ObjectToPathnames = snapshot.ObjectToPathnames
	snapshotStorage.ContentTypeToObjects = snapshot.ContentTypeToObjects
	snapshotStorage.Size = snapshot.Size

	serialized, err := json.Marshal(snapshotStorage)
	if err != nil {
		return nil, err
	}

	return serialized, nil
}
