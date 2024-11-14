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

// FileEntry represents the comprehensive structure for a file entry, capturing all relevant metadata
type FileEntry struct {
	Version       uint32              `msgpack:"version"` // Version number of the file entry structure for compatibility
	Type          importer.RecordType `msgpack:"type"`    // Type of entry (file, directory, symlink, etc.)
	Info          objects.FileInfo    `msgpack:"info"`
	SymlinkTarget string              `msgpack:"symlinkTarget,omitempty"` // Target path if the entry is a symbolic link (optional)

	Object *objects.Object `msgpack:"object,omitempty"` // Object metadata (optional)

	AlternateDataStreams []AlternateDataStream `msgpack:"alternateDataStreams,omitempty"`
	SecurityDescriptor   []byte                `msgpack:"securityDescriptor,omitempty"` // Security descriptor (optional)
	FileAttributes       uint32                `msgpack:"fileAttributes,omitempty"`     // Platform-specific attributes (e.g., hidden, system, etc.)

	ExtendedAttributes []ExtendedAttribute `msgpack:"extendedAttributes,omitempty"` // Extended attributes (xattrs) (optional)
	CustomMetadata     []CustomMetadata    `msgpack:"customMetadata,omitempty"`     // Custom key-value metadata defined by the user (optional)

	Tags       []string `msgpack:"tags,omitempty"`       // List of tags associated with the file or directory (optional)
	ParentPath string   `msgpack:"parentPath,omitempty"` // Path to the parent directory (optional)
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
		Info:               record.Stat,
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
	return &f, nil
}

func (f *FileEntry) AddFileAttributes(fileAttributes uint32) {
	f.FileAttributes = fileAttributes
}

func (f *FileEntry) AddSymlinkTarget(symlinkTarget string) {
	f.SymlinkTarget = symlinkTarget
}

func (f *FileEntry) AddTags(tags []string) {
	f.Tags = tags
}

func (f *FileEntry) Serialize() ([]byte, error) {
	data, err := msgpack.Marshal(f)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (f *FileEntry) FileInfo() *objects.FileInfo {
	return &f.Info
}
