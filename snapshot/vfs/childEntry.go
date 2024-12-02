package vfs

import (
	"github.com/PlakarKorp/plakar/objects"
	"github.com/vmihailenco/msgpack/v5"
)

type ChildEntry struct {
	Successor *objects.Checksum `msgpack:"successor,omitempty" json:"successor,omitempty"`
	Lchecksum objects.Checksum  `msgpack:"checksum" json:"checksum"`
	LfileInfo objects.FileInfo  `msgpack:"file_info" json:"file_info"`
	Lsummary  *Summary          `msgpack:"summary,omitempty" json:"summary,omitempty"`
}

func (c *ChildEntry) Checksum() objects.Checksum {
	return c.Lchecksum
}
func (c *ChildEntry) Stat() objects.FileInfo {
	return c.LfileInfo
}

func ChildEntryFromBytes(serialized []byte) (*ChildEntry, error) {
	var d ChildEntry
	if err := msgpack.Unmarshal(serialized, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (c *ChildEntry) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}
