package snapshot

import (
	"errors"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/snapshot/btree"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	ErrReadOnly = errors.New("read-only store")
)

// RepositoryStore implements btree.Storer
type SnapshotStore[K any, V any] struct {
	readonly bool
	blobtype packfile.Type
	snap     *Snapshot
}

func (s *SnapshotStore[K, V]) Get(sum objects.Checksum) (node btree.Node[K, objects.Checksum, V], err error) {
	bytes, err := s.snap.GetBlob(s.blobtype, sum)
	if err != nil {
		return
	}
	err = msgpack.Unmarshal(bytes, &node)
	return
}

func (s *SnapshotStore[K, V]) Update(sum objects.Checksum, node btree.Node[K, objects.Checksum, V]) error {
	return ErrReadOnly
}

func (s *SnapshotStore[K, V]) Put(node btree.Node[K, objects.Checksum, V]) (objects.Checksum, error) {
	if s.readonly {
		return objects.Checksum{}, ErrReadOnly
	}

	bytes, err := msgpack.Marshal(&node)
	if err != nil {
		return objects.Checksum{}, err
	}

	sum := s.snap.repository.Checksum(bytes)
	if !s.snap.BlobExists(s.blobtype, sum) {
		if err = s.snap.PutBlob(s.blobtype, sum, bytes); err != nil {
			return objects.Checksum{}, err
		}
	}
	return sum, nil
}
