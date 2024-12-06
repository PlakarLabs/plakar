package snapshot

import (
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/snapshot/btree"
	"github.com/vmihailenco/msgpack/v5"
)

type ErrorItem struct {
	Name  string `msgpack:"name" json:"name"`
	Error string `msgpack:"error" json:"error"`
}

type ErrorEntry btree.BTree[string, objects.Checksum, ErrorItem]

func ErrorItemFromBytes(data []byte) (*ErrorItem, error) {
	entry := &ErrorItem{}
	err := msgpack.Unmarshal(data, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func ErrorEntryFromBytes(data []byte) (*ErrorEntry, error) {
	entry := &ErrorEntry{}
	err := msgpack.Unmarshal(data, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (e *ErrorEntry) ToBytes() ([]byte, error) {
	return msgpack.Marshal(e)
}

func pathCmp(a, b string) int {
	if strings.HasPrefix(a, b) {
		return -1
	}
	return strings.Compare(a, b)
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

		root, err := ErrorEntryFromBytes(bytes)
		if err != nil {
			return
		}

		storage := SnapshotStore[string, ErrorItem]{
			blobtype: packfile.TYPE_ERROR,
			snap:     snapshot,
		}
		tree := btree.FromStorage(root.Root, &storage, strings.Compare, root.Order)

		iter, err := tree.ScanFrom(beneath, pathCmp)
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
