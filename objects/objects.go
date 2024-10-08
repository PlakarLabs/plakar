package objects

import (
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
)

type Object struct {
	Checksum    [32]byte
	Chunks      [][32]byte
	ContentType string
}

type Chunk struct {
	Checksum [32]byte
	Length   uint32
}

type FileInfo struct {
	Lname    string      `json:"Name" msgpack:"Name"`
	Lsize    int64       `json:"Size" msgpack:"Size"`
	Lmode    fs.FileMode `json:"Mode" msgpack:"Mode"`
	LmodTime time.Time   `json:"ModTime" msgpack:"ModTime"`
	Ldev     uint64      `json:"Dev" msgpack:"Dev"`
	Lino     uint64      `json:"Ino" msgpack:"Ino"`
	Luid     uint64      `json:"Uid" msgpack:"Uid"`
	Lgid     uint64      `json:"Gid" msgpack:"Gid"`
	Lnlink   uint16      `json:"Nlink" msgpack:"Nlink"`
}

func (f FileInfo) Name() string {
	return f.Lname
}

func (f FileInfo) Size() int64 {
	return f.Lsize
}

func (f FileInfo) Mode() os.FileMode {
	return f.Lmode
}

func (f FileInfo) ModTime() time.Time {
	return f.LmodTime
}

func (f FileInfo) Dev() uint64 {
	return f.Ldev
}

func (f FileInfo) Ino() uint64 {
	return f.Lino
}

func (f FileInfo) Uid() uint64 {
	return f.Luid
}

func (f FileInfo) Gid() uint64 {
	return f.Lgid
}

func (f FileInfo) IsDir() bool {
	return f.Lmode.IsDir()
}

func (f FileInfo) Nlink() uint16 {
	return f.Lnlink
}

func (f FileInfo) Sys() any {
	return nil
}

func FileInfoFromStat(stat os.FileInfo) FileInfo {
	return FileInfo{
		Lname:    stat.Name(),
		Lsize:    stat.Size(),
		Lmode:    stat.Mode(),
		LmodTime: stat.ModTime(),
		Ldev:     uint64(stat.Sys().(*syscall.Stat_t).Dev),
		Lino:     uint64(stat.Sys().(*syscall.Stat_t).Ino),
		Luid:     uint64(stat.Sys().(*syscall.Stat_t).Uid),
		Lgid:     uint64(stat.Sys().(*syscall.Stat_t).Gid),
		Lnlink:   uint16(stat.Sys().(*syscall.Stat_t).Nlink),
	}
}

func NewFileInfo(name string, size int64, mode os.FileMode, modTime time.Time, dev uint64, ino uint64, uid uint64, gid uint64, nlink uint16) FileInfo {
	return FileInfo{
		Lname:    name,
		Lsize:    size,
		Lmode:    mode,
		LmodTime: modTime,
		Ldev:     dev,
		Lino:     ino,
		Luid:     uid,
		Lgid:     gid,
		Lnlink:   1,
	}
}

func (fileinfo *FileInfo) HumanSize() string {
	return humanize.Bytes(uint64(fileinfo.Size()))
}

///

/*
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
	IsHidden    bool `json:"isHidden,omitempty"`    // Hidden file attribute (Windows, Linux)
	IsSystem    bool `json:"isSystem,omitempty"`    // System file attribute (Windows)
	IsReadonly  bool `json:"isReadonly,omitempty"`  // Read-only attribute
	IsArchive   bool `json:"isArchive,omitempty"`   // Archive attribute (Windows)
	IsTemporary bool `json:"isTemporary,omitempty"` // Temporary file (Windows)
}

// FileEntry represents the comprehensive structure for a file entry, capturing all relevant metadata
type FileEntry struct {
	Name               string            `json:"name"`                         // Name of the file
	Type               ObjectType        `json:"type"`                         // Type of entry (file, directory, symlink, etc.)
	Size               int64             `json:"size,omitempty"`               // Size of the file in bytes (optional for directories)
	Permissions        os.FileMode       `json:"permissions"`                  // File permissions (read/write/execute)
	ModTime            time.Time         `json:"modTime"`                      // Modification time of the file or directory
	AccessTime         time.Time         `json:"accessTime,omitempty"`         // Last access time (if available)
	ChangeTime         time.Time         `json:"changeTime,omitempty"`         // Last change time (if available)
	CreationTime       time.Time         `json:"creationTime,omitempty"`       // Creation time (if available)
	DeviceID           uint64            `json:"deviceID,omitempty"`           // Device ID for special files (block/character devices)
	NumLinks           uint64            `json:"numLinks,omitempty"`           // Number of hard links to the file (optional)
	UserID             uint32            `json:"userID,omitempty"`             // User ID of the owner (optional)
	GroupID            uint32            `json:"groupID,omitempty"`            // Group ID of the owner (optional)
	Checksum           string            `json:"checksum,omitempty"`           // Checksum of the file contents (SHA-256, etc.)
	ExtendedAttributes map[string]string `json:"extendedAttributes,omitempty"` // Extended attributes (xattrs) (optional)
	FileAttributes     FileAttributes    `json:"fileAttributes,omitempty"`     // Platform-specific attributes (e.g., hidden, system, etc.)
	SymlinkTarget      string            `json:"symlinkTarget,omitempty"`      // Target path if the entry is a symbolic link (optional)
	Version            int               `json:"version"`                      // Version number of the file entry structure for compatibility
	CustomMetadata     map[string]string `json:"customMetadata,omitempty"`     // Custom key-value metadata defined by the user (optional)
	Tags               []string          `json:"tags,omitempty"`               // List of tags associated with the file or directory (optional)
	ParentPath         string            `json:"parentPath,omitempty"`         // Path to the parent directory (optional)
}

// DirEntry represents the comprehensive structure for a directory entry
type DirEntry struct {
	Name               string            `json:"name"`                         // Name of the directory
	Type               ObjectType        `json:"type"`                         // Type of entry (directory)
	Permissions        os.FileMode       `json:"permissions"`                  // Directory permissions (read/write/execute)
	ModTime            time.Time         `json:"modTime"`                      // Modification time of the directory
	AccessTime         time.Time         `json:"accessTime,omitempty"`         // Last access time (if available)
	ChangeTime         time.Time         `json:"changeTime,omitempty"`         // Last change time (if available)
	CreationTime       time.Time         `json:"creationTime,omitempty"`       // Creation time (if available)
	UserID             uint32            `json:"userID,omitempty"`             // User ID of the owner (optional)
	GroupID            uint32            `json:"groupID,omitempty"`            // Group ID of the owner (optional)
	NumLinks           uint64            `json:"numLinks,omitempty"`           // Number of hard links to the directory (optional)
	Checksum           string            `json:"checksum,omitempty"`           // Checksum of the directory contents (optional)
	Children           []string          `json:"children,omitempty"`           // List of child entries' serialized checksums (files and subdirectories)
	ExtendedAttributes map[string]string `json:"extendedAttributes,omitempty"` // Extended attributes (xattrs) (optional)
	Version            int               `json:"version"`                      // Version number of the directory entry structure
	CustomMetadata     map[string]string `json:"customMetadata,omitempty"`     // Custom key-value metadata defined by the user (optional)
	Tags               []string          `json:"tags,omitempty"`               // List of tags associated with the directory (optional)
	ParentPath         string            `json:"parentPath,omitempty"`         // Path to the parent directory (optional)
}
*/
