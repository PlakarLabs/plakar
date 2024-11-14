package vfs

import (
	"sort"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/vmihailenco/msgpack/v5"
)

type FSEntry interface {
	fsEntry()
}

type FileEntry struct {
	Version    uint32              `msgpack:"version"`
	ParentPath string              `msgpack:"parentPath"`
	Type       importer.RecordType `msgpack:"type"`
	FileInfo   objects.FileInfo    `msgpack:"fileInfo"`

	/* File specific fields */
	SymlinkTarget string          `msgpack:"symlinkTarget,omitempty"`
	Object        *objects.Object `msgpack:"object,omitempty" json:"Object,omitempty"` // nil for !regular files

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

func (*FileEntry) fsEntry() {}

func NewFileEntry(parentPath string, record *importer.ScanRecord) *FileEntry {
	target := ""
	if record.Type == importer.RecordTypeSymlink {
		target = record.Target
	}

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

	return &FileEntry{
		Version:            VERSION,
		Type:               record.Type,
		FileInfo:           record.FileInfo,
		SymlinkTarget:      target,
		ExtendedAttributes: ExtendedAttributes,
		ParentPath:         parentPath,
	}
}

func FileEntryFromBytes(serialized []byte) (*FileEntry, error) {
	var f FileEntry
	if err := msgpack.Unmarshal(serialized, &f); err != nil {
		return nil, err
	}
	if f.AlternateDataStreams == nil {
		f.AlternateDataStreams = make([]AlternateDataStream, 0)
	}
	if f.SecurityDescriptor == nil {
		f.SecurityDescriptor = make([]byte, 0)
	}
	if f.ExtendedAttributes == nil {
		f.ExtendedAttributes = make([]ExtendedAttribute, 0)
	}
	if f.CustomMetadata == nil {
		f.CustomMetadata = make([]CustomMetadata, 0)
	}
	if f.Tags == nil {
		f.Tags = make([]string, 0)
	}
	return &f, nil
}

func (f *FileEntry) Serialize() ([]byte, error) {
	data, err := msgpack.Marshal(f)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f *FileEntry) Stat() *objects.FileInfo {
	return &f.FileInfo
}
