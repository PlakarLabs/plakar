package objects

import "github.com/vmihailenco/msgpack/v5"

type List struct {
	Count uint64    `msgpack:"count"`
	Head  *Checksum `msgpack:"head,omitempty"`
}

func NewList() *List {
	return &List{}
}

func (l *List) ToBytes() ([]byte, error) {
	return msgpack.Marshal(l)
}
