package vfs

import (
	"sort"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/vmihailenco/msgpack/v5"
)

type ChildEntry struct {
	Checksum   [32]byte         `msgpack:"checksum"`
	FileInfo   objects.FileInfo `msgpack:"fileInfo"`
	Statistics *Statistics      `msgpack:"statistics,omitempty"`
}

type DirEntry struct {
	Version    uint32              `msgpack:"version"`
	ParentPath string              `msgpack:"parentPath"`
	Type       importer.RecordType `msgpack:"type"`
	FileInfo   objects.FileInfo    `msgpack:"fileInfo"`

	/* Directory specific fields */
	Children   []ChildEntry `msgpack:"children,omitempty"`
	Statistics Statistics   `msgpack:"statistics"`

	/* Windows specific fields */
	AlternateDataStreams []AlternateDataStream `msgpack:"alternateDataStreams,omitempty"`
	SecurityDescriptor   []byte                `msgpack:"securityDescriptor,omitempty"`
	FileAttributes       uint32                `msgpack:"fileAttributes,omitempty"`

	/* Unix fields */
	ExtendedAttributes []ExtendedAttribute `msgpack:"extendedAttributes,omitempty"`

	/* Custom metadata and tags */
	CustomMetadata []CustomMetadata `msgpack:"customMetadata,omitempty"`
	Tags           []string         `msgpack:"tags,omitempty"`
}

func (*DirEntry) fsEntry() {}

func NewDirectoryEntry(parentPath string, record *importer.ScanRecord) *DirEntry {
	ExtendedAttributes := make([]ExtendedAttribute, 0)
	for name, value := range record.ExtendedAttributes {
		ExtendedAttributes = append(ExtendedAttributes, ExtendedAttribute{
			Name:  name,
			Value: value,
		})
	}

	sort.Slice(ExtendedAttributes, func(i, j int) bool {
		return ExtendedAttributes[i].Name < ExtendedAttributes[j].Name
	})

	return &DirEntry{
		Version:            VERSION,
		Type:               record.Type,
		FileInfo:           record.FileInfo,
		ExtendedAttributes: ExtendedAttributes,
		ParentPath:         parentPath,
	}
}

func DirEntryFromBytes(serialized []byte) (*DirEntry, error) {
	var d DirEntry
	if err := msgpack.Unmarshal(serialized, &d); err != nil {
		return nil, err
	}
	if d.Children == nil {
		d.Children = make([]ChildEntry, 0)
	}
	if d.AlternateDataStreams == nil {
		d.AlternateDataStreams = make([]AlternateDataStream, 0)
	}
	if d.SecurityDescriptor == nil {
		d.SecurityDescriptor = make([]byte, 0)
	}
	if d.ExtendedAttributes == nil {
		d.ExtendedAttributes = make([]ExtendedAttribute, 0)
	}
	if d.CustomMetadata == nil {
		d.CustomMetadata = make([]CustomMetadata, 0)
	}
	if d.Tags == nil {
		d.Tags = make([]string, 0)
	}
	return &d, nil
}

func (d *DirEntry) AddChild(checksum [32]byte, fileInfo objects.FileInfo, statistics *Statistics) {
	d.Children = append(d.Children, ChildEntry{
		Checksum:   checksum,
		FileInfo:   fileInfo,
		Statistics: statistics,
	})
}

func (d *DirEntry) AddTags(tags []string) {
	d.Tags = tags
}

func (d *DirEntry) Serialize() ([]byte, error) {
	data, err := msgpack.Marshal(d)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (d *DirEntry) Stat() *objects.FileInfo {
	return &d.FileInfo
}
