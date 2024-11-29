package vfs

import (
	"github.com/vmihailenco/msgpack/v5"
)

type ErrorEntry struct {
	Name  string `msgpack:"name" json:"name"`
	Error string `msgpack:"error" json:"error"`
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
