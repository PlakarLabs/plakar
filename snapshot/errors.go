package snapshot

import (
	"strings"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/vmihailenco/msgpack/v5"
)

type ErrorItem struct {
	Name  string `msgpack:"name" json:"name"`
	Error string `msgpack:"error" json:"error"`
}

func (snapshot *Snapshot) Errors(beneath string) (<-chan ErrorItem, error) {
	if !strings.HasSuffix(beneath, "/") {
		beneath += "/"
	}

	c := make(chan ErrorItem)

	go func() {
		defer close(c)

		bytes, err := snapshot.GetBlob(packfile.TYPE_ERROR, snapshot.Header.Errors)
		if err != nil {
			return
		}

		var root btree.BTree[string, objects.Checksum, ErrorItem]
		if err := msgpack.Unmarshal(bytes, &root); err != nil {
			return
		}

		storage := SnapshotStore[string, ErrorItem]{
			blobtype: packfile.TYPE_ERROR,
			snap:     snapshot,
		}
		tree := btree.FromStorage(root.Root, &storage, strings.Compare, root.Order)

		iter, err := tree.ScanFrom(beneath)
		if err != nil {
			return
		}

		for iter.Next() {
			_, item := iter.Current()
			if !strings.HasPrefix(item.Name, beneath) {
				break
			}
			c <- item
		}
	}()

	return c, nil
}
