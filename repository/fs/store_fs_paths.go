package fs

import "fmt"

func (store *FSStore) PathPurge() string {
	return fmt.Sprintf("%s/purge", store.root)
}

func (store *FSStore) PathChunks() string {
	return fmt.Sprintf("%s/chunks", store.root)
}

func (store *FSStore) PathObjects() string {
	return fmt.Sprintf("%s/objects", store.root)
}

func (store *FSStore) PathTransactions() string {
	return fmt.Sprintf("%s/transactions", store.root)
}

func (store *FSStore) PathSnapshots() string {
	return fmt.Sprintf("%s/snapshots", store.root)
}

func (store *FSStore) PathChunkBucket(checksum string) string {
	return fmt.Sprintf("%s/chunks/%s", store.root, checksum[0:2])
}

func (store *FSStore) PathObjectBucket(checksum string) string {
	return fmt.Sprintf("%s/objects/%s", store.root, checksum[0:2])
}

func (store *FSStore) PathSnapshotBucket(checksum string) string {
	return fmt.Sprintf("%s/snapshots/%s", store.root, checksum[0:2])
}

func (store *FSStore) PathChunk(checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathChunkBucket(checksum), checksum)
}

func (store *FSStore) PathObject(checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathObjectBucket(checksum), checksum)
}

func (store *FSStore) PathSnapshot(checksum string) string {
	return fmt.Sprintf("%s/%s", store.PathSnapshotBucket(checksum), checksum)
}

func (transaction *FSTransaction) Path() string {
	return fmt.Sprintf("%s/%s/%s", transaction.store.PathTransactions(),
		transaction.Uuid[0:2], transaction.Uuid)
}

func (transaction *FSTransaction) PathObjects() string {
	return fmt.Sprintf("%s/objects", transaction.Path())
}

func (transaction *FSTransaction) PathObjectBucket(checksum string) string {
	return fmt.Sprintf("%s/%s", transaction.PathObjects(), checksum[0:2])
}

func (transaction *FSTransaction) PathObject(checksum string) string {
	return fmt.Sprintf("%s/%s", transaction.PathObjectBucket(checksum), checksum)
}

func (transaction *FSTransaction) PathChunks() string {
	return fmt.Sprintf("%s/chunks", transaction.Path())
}

func (transaction *FSTransaction) PathChunkBucket(checksum string) string {
	return fmt.Sprintf("%s/%s", transaction.PathChunks(), checksum[0:2])
}

func (transaction *FSTransaction) PathChunk(checksum string) string {
	return fmt.Sprintf("%s/%s", transaction.PathChunkBucket(checksum), checksum)
}
