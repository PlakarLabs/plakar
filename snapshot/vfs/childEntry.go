package vfs

import (
	"github.com/PlakarKorp/plakar/objects"
	"github.com/vmihailenco/msgpack/v5"
)

type ChildEntry struct {
	Lchecksum objects.Checksum `msgpack:"checksum" json:"Checksum"`
	LfileInfo objects.FileInfo `msgpack:"fileInfo" json:"FileInfo"`
	Lsummary  *Summary         `msgpack:"summary,omitempty" json:"Summary,omitempty"`
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
