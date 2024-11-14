package vfs

import (
	"sort"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/vmihailenco/msgpack/v5"
)

type MIMEStats struct {
	Min   uint64  `msgpack:"min,omitempty"`
	Max   uint64  `msgpack:"max,omitempty"`
	Avg   float64 `msgpack:"avg,omitempty"`
	Total uint64  `msgpack:"total,omitempty"`
}

type AggregatedStats struct {
	Files       uint64 `msgpack:"files,omitempty"`       // Total number of files in the directory
	Directories uint64 `msgpack:"directories,omitempty"` // Total number of subdirectories in the directory
	Symlinks    uint64 `msgpack:"symlinks,omitempty"`    // Total number of symlinks in the directory
	Devices     uint64 `msgpack:"devices,omitempty"`     // Total number of devices in the directory
	Pipes       uint64 `msgpack:"pipes,omitempty"`       // Total number of pipes in the directory
	Sockets     uint64 `msgpack:"sockets,omitempty"`     // Total number of sockets in the directory

	MinSize   uint64  `msgpack:"minSize,omitempty"`   // Smallest file in the directory
	MaxSize   uint64  `msgpack:"maxSize,omitempty"`   // Largest file in the directory
	AvgSize   float64 `msgpack:"avgSize,omitempty"`   // Average size of the files in the directory
	TotalSize uint64  `msgpack:"totalSize,omitempty"` // Total size of all files in the directory

	TotalObjects uint64 `msgpack:"totalObjects,omitempty"` // Total number of objects in the directory
	TotalChunks  uint64 `msgpack:"totalChunks,omitempty"`  // Total number of chunks across all files

	MinModTime int64 `msgpack:"minModTime,omitempty"` // Oldest modification time of the directory
	MaxModTime int64 `msgpack:"maxModTime,omitempty"` // Last modification time of the directory
	AvgModTime int64 `msgpack:"avgModTime,omitempty"` // Average age of the files in the directory

	MinDepth uint64  `msgpack:"minDepth,omitempty"` // Minimum depth of the directory
	MaxDepth uint64  `msgpack:"maxDepth,omitempty"` // Maximum depth of the directory
	AvgDepth float64 `msgpack:"avgDepth,omitempty"` // Average depth of the directory

	MinErrors uint64  `msgpack:"minErrors,omitempty"` // Minimum number of errors encountered in a file
	MaxErrors uint64  `msgpack:"maxErrors,omitempty"` // Maximum number of errors encountered in a file
	AvgErrors float64 `msgpack:"avgErrors,omitempty"` // Average number of errors encountered in a file

	MinEntropy            float64 `msgpack:"minEntropy,omitempty"`            // Minimum entropy of the directory
	MaxEntropy            float64 `msgpack:"maxEntropy,omitempty"`            // Maximum entropy of the directory
	AvgEntropy            float64 `msgpack:"avgEntropy,omitempty"`            // Average entropy of the directory
	TotalHighEntropyFiles uint64  `msgpack:"totalHighEntropyFiles,omitempty"` // Total number of files with high entropy
	TotalLowEntropyFiles  uint64  `msgpack:"totalLowEntropyFiles,omitempty"`  // Total number of files with low entropy

	MIMEText        MIMEStats `msgpack:"MIMEText,omitempty"`        // Stats for text files
	MIMEImage       MIMEStats `msgpack:"MIMEImage,omitempty"`       // Stats for image files
	MIMEAudio       MIMEStats `msgpack:"MIMEAudio,omitempty"`       // Stats for audio files
	MIMEVideo       MIMEStats `msgpack:"MIMEVideo,omitempty"`       // Stats for video files
	MIMEApplication MIMEStats `msgpack:"MIMEApplication,omitempty"` // Stats for application files
	MIMEOther       MIMEStats `msgpack:"MIMEOther,omitempty"`       // Stats for other MIME types
}

type ChildEntry struct {
	Checksum        [32]byte         `msgpack:"checksum"`
	FileInfo        objects.FileInfo `msgpack:"fileInfo"`
	AggregatedStats *AggregatedStats `msgpack:"aggregatedStats,omitempty"`
}

type DirEntry struct {
	Version    uint32              `msgpack:"version"`
	ParentPath string              `msgpack:"parentPath"`
	Type       importer.RecordType `msgpack:"type"`
	FileInfo   objects.FileInfo    `msgpack:"fileInfo"`

	/* Directory specific fields */
	Children        []ChildEntry    `msgpack:"children,omitempty"`
	AggregatedStats AggregatedStats `msgpack:"aggregatedStats"`

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

func (d *DirEntry) Stat() *objects.FileInfo {
	return &d.FileInfo
}
