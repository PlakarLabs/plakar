package vfs

import (
	"os"
	"sort"
	"time"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/snapshot/importer"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION = 001

///

type ObjectType string

const (
	ObjectTypeFile    ObjectType = "file"
	ObjectTypeDir     ObjectType = "directory"
	ObjectTypeSymlink ObjectType = "symlink"
	ObjectTypeDevice  ObjectType = "device"
	ObjectTypePipe    ObjectType = "pipe"
	ObjectTypeSocket  ObjectType = "socket"
)

// FileAttributes represents platform-specific file attributes (e.g., hidden, system, etc.)
type FileAttributes struct {
	IsHidden    bool `msgpack:"isHidden,omitempty"`    // Hidden file attribute (Windows, Linux)
	IsSystem    bool `msgpack:"isSystem,omitempty"`    // System file attribute (Windows)
	IsReadonly  bool `msgpack:"isReadonly,omitempty"`  // Read-only attribute
	IsArchive   bool `msgpack:"isArchive,omitempty"`   // Archive attribute (Windows)
	IsTemporary bool `msgpack:"isTemporary,omitempty"` // Temporary file (Windows)
}

type FSEntry interface {
	fsEntry()
}

type ExtendedAttribute struct {
	Name  string `msgpack:"name"`
	Value []byte `msgpack:"value"`
}

type CustomMetadata struct {
	Key   string `msgpack:"key"`
	Value string `msgpack:"value"`
}

// FileEntry represents the comprehensive structure for a file entry, capturing all relevant metadata
type FileEntry struct {
	Version            uint32              `msgpack:"version"`                      // Version number of the file entry structure for compatibility
	Name               string              `msgpack:"name"`                         // Name of the file
	Type               importer.RecordType `msgpack:"type"`                         // Type of entry (file, directory, symlink, etc.)
	Size               int64               `msgpack:"size,omitempty"`               // Size of the file in bytes (optional for directories)
	Permissions        os.FileMode         `msgpack:"permissions"`                  // File permissions (read/write/execute)
	ModTime            time.Time           `msgpack:"modTime"`                      // Modification time of the file or directory
	DeviceID           uint64              `msgpack:"deviceID,omitempty"`           // Device ID for special files (block/character devices)
	InodeID            uint64              `msgpack:"inodeID,omitempty"`            // Inode ID for special files (block/character devices)
	UserID             uint64              `msgpack:"userID,omitempty"`             // User ID of the owner (optional)
	GroupID            uint64              `msgpack:"groupID,omitempty"`            // Group ID of the owner (optional)
	NumLinks           uint16              `msgpack:"numLinks,omitempty"`           // Number of hard links to the file (optional)
	Checksum           [32]byte            `msgpack:"checksum,omitempty"`           // Checksum of the file contents (SHA-256, etc.)
	Chunks             []objects.Chunk     `msgpack:"chunks,omitempty"`             // List of chunk checksums (optional)
	ExtendedAttributes []ExtendedAttribute `msgpack:"extendedAttributes,omitempty"` // Extended attributes (xattrs) (optional)
	FileAttributes     FileAttributes      `msgpack:"fileAttributes,omitempty"`     // Platform-specific attributes (e.g., hidden, system, etc.)
	SymlinkTarget      string              `msgpack:"symlinkTarget,omitempty"`      // Target path if the entry is a symbolic link (optional)
	ContentType        string              `msgpack:"contentType,omitempty"`        // MIME type of the file (optional)
	CustomMetadata     []CustomMetadata    `msgpack:"customMetadata,omitempty"`     // Custom key-value metadata defined by the user (optional)
	Tags               []string            `msgpack:"tags,omitempty"`               // List of tags associated with the file or directory (optional)
	ParentPath         string              `msgpack:"parentPath,omitempty"`         // Path to the parent directory (optional)
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
		Name:               record.Stat.Name(),
		Type:               record.Type,
		Size:               record.Stat.Size(),
		Permissions:        record.Stat.Mode(),
		ModTime:            record.Stat.ModTime(),
		DeviceID:           record.Stat.Dev(),
		InodeID:            record.Stat.Ino(),
		UserID:             record.Stat.Uid(),
		GroupID:            record.Stat.Gid(),
		NumLinks:           record.Stat.Nlink(),
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

func (f *FileEntry) AddChecksum(checksum [32]byte) {
	f.Checksum = checksum
}

func (f *FileEntry) AddChunk(chunk objects.Chunk) {
	f.Chunks = append(f.Chunks, chunk)
}

//func (f *FileEntry) AddExtendedAttributes(extendedAttributes map[string][]byte) {
//	f.ExtendedAttributes = extendedAttributes
//}

func (f *FileEntry) AddFileAttributes(fileAttributes FileAttributes) {
	f.FileAttributes = fileAttributes
}

func (f *FileEntry) AddSymlinkTarget(symlinkTarget string) {
	f.SymlinkTarget = symlinkTarget
}

//func (f *FileEntry) AddCustomMetadata(customMetadata map[string]string) {
//	f.CustomMetadata = customMetadata
//}

func (f *FileEntry) AddContentType(contentType string) {
	f.ContentType = contentType
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
	return &objects.FileInfo{
		Lname:    f.Name,
		Lsize:    f.Size,
		Lmode:    f.Permissions,
		LmodTime: f.ModTime,
		Ldev:     f.DeviceID,
		Lino:     f.InodeID,
		Luid:     f.UserID,
		Lgid:     f.GroupID,
		Lnlink:   f.NumLinks,
	}
}

type ChildEntry struct {
	Checksum           [32]byte
	FileInfo           objects.FileInfo
	ExtendedAttributes map[string][]byte
}

// DirEntry represents the comprehensive structure for a directory entry
type DirEntry struct {
	Version            uint32              `msgpack:"version"`                      // Version number of the file entry structure for compatibility
	Name               string              `msgpack:"name"`                         // Name of the directory
	Type               importer.RecordType `msgpack:"type"`                         // Type of entry (directory)
	Size               int64               `msgpack:"size,omitempty"`               // Size of the file in bytes (optional for directories)
	Permissions        os.FileMode         `msgpack:"permissions"`                  // Directory permissions (read/write/execute)
	ModTime            time.Time           `msgpack:"modTime"`                      // Modification time of the directory
	DeviceID           uint64              `msgpack:"deviceID,omitempty"`           // Device ID for special files (block/character devices)
	InodeID            uint64              `msgpack:"inodeID,omitempty"`            // Inode ID for special files (block/character devices)
	UserID             uint64              `msgpack:"userID,omitempty"`             // User ID of the owner (optional)
	GroupID            uint64              `msgpack:"groupID,omitempty"`            // Group ID of the owner (optional)
	NumLinks           uint16              `msgpack:"numLinks,omitempty"`           // Number of hard links to the directory (optional)
	Children           []ChildEntry        `msgpack:"children,omitempty"`           // List of child entries' serialized checksums (files and subdirectories)
	ExtendedAttributes []ExtendedAttribute `msgpack:"extendedAttributes,omitempty"` // Extended attributes (xattrs) (optional)
	CustomMetadata     []CustomMetadata    `msgpack:"customMetadata,omitempty"`     // Custom key-value metadata defined by the user (optional)
	Tags               []string            `msgpack:"tags,omitempty"`               // List of tags associated with the directory (optional)
	ParentPath         string              `msgpack:"parentPath,omitempty"`         // Path to the parent directory (optional)
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
		Name:               record.Stat.Name(),
		Type:               record.Type,
		Size:               record.Stat.Size(),
		Permissions:        record.Stat.Mode(),
		ModTime:            record.Stat.ModTime(),
		DeviceID:           record.Stat.Dev(),
		InodeID:            record.Stat.Ino(),
		UserID:             record.Stat.Uid(),
		GroupID:            record.Stat.Gid(),
		NumLinks:           record.Stat.Nlink(),
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

func (d *DirEntry) AddChild(checksum [32]byte, record importer.ScanRecord) {
	d.Children = append(d.Children, ChildEntry{
		Checksum:           checksum,
		FileInfo:           record.Stat,
		ExtendedAttributes: record.ExtendedAttributes,
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
	return &objects.FileInfo{
		Lname:    d.Name,
		Lsize:    d.Size,
		Lmode:    d.Permissions,
		LmodTime: d.ModTime,
		Ldev:     d.DeviceID,
		Lino:     d.InodeID,
		Luid:     d.UserID,
		Lgid:     d.GroupID,
		Lnlink:   d.NumLinks,
	}
}
