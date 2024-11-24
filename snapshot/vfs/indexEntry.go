package vfs

import (
	"github.com/PlakarKorp/plakar/objects"
	"github.com/vmihailenco/msgpack/v5"
)

type IndexRecord struct {
	Key   string           `msgpack:"key"`
	Value objects.Checksum `msgpack:"value"`
}

type IndexEntry struct {
	Next *objects.Checksum `msgpack:"next,omitempty"`
	Prev *objects.Checksum `msgpack:"prev,omitempty"`

	Entries []IndexRecord `msgpack:"entries"`
}

func IndexEntryFromBytes(data []byte) (*IndexEntry, error) {
	index := &IndexEntry{}
	if err := msgpack.Unmarshal(data, index); err != nil {
		return nil, err
	}
	return index, nil
}

func (index *IndexEntry) ToBytes() ([]byte, error) {
	return msgpack.Marshal(index)
}
