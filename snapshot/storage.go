package snapshot

import "encoding/json"

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
	snapshot.Roots = snapshotStorage.Roots
	snapshot.Tree = snapshotStorage.Tree

	snapshot.Directories = make(map[string]*Fileinfo)
	for _, directory := range snapshotStorage.Directories {
		snapshot.Directories[directory], _ = snapshot.GetInode(directory)
	}

	snapshot.Files = make(map[string]*Fileinfo)
	for _, file := range snapshotStorage.Files {
		snapshot.Files[file], _ = snapshot.GetInode(file)
	}

	snapshot.NonRegular = make(map[string]*Fileinfo)
	for _, file := range snapshotStorage.NonRegular {
		snapshot.NonRegular[file], _ = snapshot.GetInode(file)
	}

	snapshot.Filenames = snapshotStorage.Filenames
	snapshot.Objects = snapshotStorage.Objects
	snapshot.Chunks = snapshotStorage.Chunks
	snapshot.ChunkToObjects = snapshotStorage.ChunkToObjects
	snapshot.ContentTypeToObjects = snapshotStorage.ContentTypeToObjects
	snapshot.ObjectToPathnames = snapshotStorage.ObjectToPathnames

	snapshot.Size = snapshotStorage.Size

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
	snapshotStorage.Roots = snapshot.Roots
	snapshotStorage.Tree = snapshot.Tree

	snapshotStorage.Directories = make([]string, 0)
	for directory := range snapshot.Directories {
		snapshotStorage.Directories = append(snapshotStorage.Directories, directory)
	}

	snapshotStorage.Files = make([]string, 0)
	for file := range snapshot.Files {
		snapshotStorage.Files = append(snapshotStorage.Files, file)
	}

	snapshotStorage.NonRegular = make([]string, 0)
	for file := range snapshot.NonRegular {
		snapshotStorage.NonRegular = append(snapshotStorage.NonRegular, file)
	}

	snapshotStorage.Filenames = snapshot.Filenames
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
