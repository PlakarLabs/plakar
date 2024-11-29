package objects

import "github.com/vmihailenco/msgpack/v5"

type List struct {
	Count uint64    `msgpack:"count"`
	Head  *Checksum `msgpack:"head,omitempty"`
	Tail  *Checksum `msgpack:"tail,omitempty"`
}

func NewList() *List {
	return &List{}
}

func (l *List) ToBytes() ([]byte, error) {
	return msgpack.Marshal(l)
}

type ListEntry struct {
	Predecessor *Checksum `msgpack:"predecessor,omitempty"`
	Successor   *Checksum `msgpack:"successor,omitempty"`
}

func NewListEntry(checksum Checksum) *ListEntry {
	return &ListEntry{}
}

func ListEntryFromBytes(serialized []byte) (*ListEntry, error) {
	var le ListEntry
	if err := msgpack.Unmarshal(serialized, &le); err != nil {
		return nil, err
	}
	return &le, nil
}

func (le *ListEntry) ToBytes() ([]byte, error) {
	return msgpack.Marshal(le)
}
