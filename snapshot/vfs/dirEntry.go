package vfs

import (
	"path/filepath"
	"sort"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/vmihailenco/msgpack/v5"
)

type DirEntry struct {
	Version    uint32              `msgpack:"version" json:"version"`
	ParentPath string              `msgpack:"parent_path" json:"parent_path"`
	Type       importer.RecordType `msgpack:"type" json:"type"`
	FileInfo   objects.FileInfo    `msgpack:"file_info" json:"file_info"`

	/* Directory specific fields */
	Children *objects.Checksum `msgpack:"children,omitempty" json:"children,omitempty"`
	Summary  Summary           `msgpack:"summary" json:"summary"`

	/* Windows specific fields */
	AlternateDataStreams []AlternateDataStream `msgpack:"alternate_data_streams,omitempty" json:"alternate_data_streams"`
	SecurityDescriptor   []byte                `msgpack:"security_descriptor,omitempty" json:"security_descriptor"`
	FileAttributes       uint32                `msgpack:"file_attributes,omitempty" json:"file_attributes"`

	/* Unix fields */
	ExtendedAttributes []ExtendedAttribute `msgpack:"extended_attributes,omitempty" json:"extended_attributes"`

	/* Custom metadata and tags */
	Classifications []Classification `msgpack:"classifications,omitempty" json:"classifications"`
	CustomMetadata  []CustomMetadata `msgpack:"custom_metadata,omitempty" json:"custom_metadata"`
	Tags            []string         `msgpack:"tags,omitempty" json:"tags"`

	/* Errors */
	Errors *objects.Checksum `msgpack:"errors,omitempty" json:"errors,omitempty"`
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

func (d *DirEntry) Size() int64 {
	return d.Stat().Size()
}

func (d *DirEntry) Name() string {
	return d.Stat().Name()
}

func (d *DirEntry) Path() string {
	return filepath.Join(d.ParentPath, d.Name())
}

func (d *DirEntry) AddClassification(analyzer string, classes []string) {
	d.Classifications = append(d.Classifications, Classification{
		Analyzer: analyzer,
		Classes:  classes,
	})
}
