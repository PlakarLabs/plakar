package snapshot

import (
	"errors"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
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

func (s *SnapshotStore[K, V]) Get(sum objects.Checksum) (*btree.Node[K, objects.Checksum, V], error) {
	bytes, err := s.snap.GetBlob(s.blobtype, sum)
	if err != nil {
		return nil, err
	}
	node := &btree.Node[K, objects.Checksum, V]{}
	err = msgpack.Unmarshal(bytes, node)
	return node, err
}

func (s *SnapshotStore[K, V]) Update(sum objects.Checksum, node *btree.Node[K, objects.Checksum, V]) error {
	return ErrReadOnly
}

func (s *SnapshotStore[K, V]) Put(node *btree.Node[K, objects.Checksum, V]) (objects.Checksum, error) {
	if s.readonly {
		return objects.Checksum{}, ErrReadOnly
	}

	bytes, err := msgpack.Marshal(node)
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

// persistIndex saves a btree[K, P, V] index to the snapshot.  The
// pointer type P is converted to a checksum.
func persistIndex[K, P, V any](snap *Snapshot, tree *btree.BTree[K, P, V], t packfile.Type) (csum objects.Checksum, err error) {
	root, err := btree.Persist(tree, &SnapshotStore[K, V]{
		readonly: false,
		blobtype: t,
		snap:     snap,
	})
	if err != nil {
		return
	}

	bytes, err := msgpack.Marshal(&btree.BTree[K, objects.Checksum, V]{
		Order: tree.Order,
		Root:  root,
	})
	if err != nil {
		return
	}

	csum = snap.repository.Checksum(bytes)
	if !snap.BlobExists(t, csum) {
		err = snap.PutBlob(t, csum, bytes)
	}
	return
}
