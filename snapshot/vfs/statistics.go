package vfs

type Statistics struct {
	Files       uint64 `msgpack:"files,omitempty"`
	Directories uint64 `msgpack:"directories,omitempty"`
	TotalSize   uint64 `msgpack:"totalSize,omitempty"`

	/*
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

		MinEntropy            float64 `msgpack:"minEntropy,omitempty"`            // Minimum entropy of the directory
		MaxEntropy            float64 `msgpack:"maxEntropy,omitempty"`            // Maximum entropy of the directory
		AvgEntropy            float64 `msgpack:"avgEntropy,omitempty"`            // Average entropy of the directory
		TotalHighEntropyFiles uint64  `msgpack:"totalHighEntropyFiles,omitempty"` // Total number of files with high entropy
		TotalLowEntropyFiles  uint64  `msgpack:"totalLowEntropyFiles,omitempty"`  // Total number of files with low entropy
	*/
}
