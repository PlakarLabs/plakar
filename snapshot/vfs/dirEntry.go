package vfs

import (
	"sort"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/vmihailenco/msgpack/v5"
)

type AggregatedStats struct {
	NFiles uint64 `msgpack:"NFiles,omitempty"` // Total number of files in the directory
	NDirs  uint64 `msgpack:"NDirs,omitempty"`  // Total number of subdirectories in the directory
	Size   uint64 `msgpack:"Size,omitempty"`   // Total size of all files in the directory
}

type ChildEntry struct {
	Checksum        [32]byte         `msgpack:"checksum"`
	FileInfo        objects.FileInfo `msgpack:"fileInfo"`
	AggregatedStats *AggregatedStats `msgpack:"aggregatedStats,omitempty"`
}

type DirEntry struct {
	Version uint32              `msgpack:"version"` // Version number of the file entry structure for compatibility
	Type    importer.RecordType `msgpack:"type"`    // Type of entry (directory)
	Info    objects.FileInfo    `msgpack:"info"`

	Children        []ChildEntry    `msgpack:"children,omitempty"` // List of child entries' serialized checksums (files and subdirectories)
	AggregatedStats AggregatedStats `msgpack:"aggregatedStats,omitempty"`

	AlternateDataStreams []AlternateDataStream `msgpack:"alternateDataStreams,omitempty"`
	SecurityDescriptor   []byte                `msgpack:"securityDescriptor,omitempty"` // Security descriptor (optional)
	FileAttributes       uint32                `msgpack:"fileAttributes,omitempty"`     // Platform-specific attributes (e.g., hidden, system, etc.)

	ExtendedAttributes []ExtendedAttribute `msgpack:"extendedAttributes,omitempty"` // Extended attributes (xattrs) (optional)
	CustomMetadata     []CustomMetadata    `msgpack:"customMetadata,omitempty"`     // Custom key-value metadata defined by the user (optional)

	Tags       []string `msgpack:"tags,omitempty"`       // List of tags associated with the directory (optional)
	ParentPath string   `msgpack:"parentPath,omitempty"` // Path to the parent directory (optional)

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
		Info:               record.Stat,
		ExtendedAttributes: ExtendedAttributes,
		ParentPath:         parentPath,
	}
}

func DirEntryFromBytes(serialized []byte) (*DirEntry, error) {
	var d DirEntry
	if err := msgpack.Unmarshal(serialized, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (d *DirEntry) AddChild(checksum [32]byte, fileInfo objects.FileInfo, aggregatedStats *AggregatedStats) {
	d.Children = append(d.Children, ChildEntry{
		Checksum:        checksum,
		FileInfo:        fileInfo,
		AggregatedStats: aggregatedStats,
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

func (d *DirEntry) FileInfo() *objects.FileInfo {
	return &d.Info
}
