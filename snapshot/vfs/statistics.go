package vfs

type Directory struct {
	Files       uint64 `msgpack:"files,omitempty"`
	Directories uint64 `msgpack:"directories,omitempty"`
	Symlinks    uint64 `msgpack:"symlinks,omitempty"` // Total number of symlinks in the directory
	Devices     uint64 `msgpack:"devices,omitempty"`  // Total number of devices in the directory
	Pipes       uint64 `msgpack:"pipes,omitempty"`    // Total number of pipes in the directory
	Sockets     uint64 `msgpack:"sockets,omitempty"`  // Total number of sockets in the directory

	Objects uint64 `msgpack:"objects,omitempty"` // Total number of objects in the directory
	Chunks  uint64 `msgpack:"chunks,omitempty"`  // Total number of chunks across all files

	MinSize uint64 `msgpack:"minSize,omitempty"` // Smallest file in the directory
	MaxSize uint64 `msgpack:"maxSize,omitempty"` // Largest file in the directory
	AvgSize uint64 `msgpack:"avgSize,omitempty"` // Average size of the directory
	Size    uint64 `msgpack:"size,omitempty"`    // Total size of all files in the directory

	MinModTime int64 `msgpack:"minModTime,omitempty"` // Oldest modification time of the directory
	MaxModTime int64 `msgpack:"maxModTime,omitempty"` // Last modification time of the directory

	MinEntropy float64 `msgpack:"minEntropy,omitempty"` // Minimum entropy of the directory
	MaxEntropy float64 `msgpack:"maxEntropy,omitempty"` // Maximum entropy of the directory
	AvgEntropy float64 `msgpack:"avgEntropy,omitempty"` // Average entropy of the directory
	HiEntropy  uint64  `msgpack:"HiEntropy,omitempty"`  // Total number of files with high entropy
	LoEntropy  uint64  `msgpack:"LoEntropy,omitempty"`  // Total number of files with low entropy
}

type Below struct {
	Files       uint64 `msgpack:"files,omitempty"`
	Directories uint64 `msgpack:"directories,omitempty"`
	Symlinks    uint64 `msgpack:"symlinks,omitempty"` // Total number of symlinks in the directory
	Devices     uint64 `msgpack:"devices,omitempty"`  // Total number of devices in the directory
	Pipes       uint64 `msgpack:"pipes,omitempty"`    // Total number of pipes in the directory
	Sockets     uint64 `msgpack:"sockets,omitempty"`  // Total number of sockets in the directory

	Objects uint64 `msgpack:"objects,omitempty"` // Total number of objects in the directory
	Chunks  uint64 `msgpack:"chunks,omitempty"`  // Total number of chunks across all files

	MinSize uint64 `msgpack:"minSize,omitempty"` // Smallest file in the directory
	MaxSize uint64 `msgpack:"maxSize,omitempty"` // Largest file in the directory
	Size    uint64 `msgpack:"size,omitempty"`    // Total size of all files in the directory

	MinModTime int64 `msgpack:"minModTime,omitempty"` // Oldest modification time of the directory
	MaxModTime int64 `msgpack:"maxModTime,omitempty"` // Last modification time of the directory

	MinEntropy float64 `msgpack:"minEntropy,omitempty"` // Minimum entropy of the directory
	MaxEntropy float64 `msgpack:"maxEntropy,omitempty"` // Maximum entropy of the directory
	HiEntropy  uint64  `msgpack:"HiEntropy,omitempty"`  // Total number of files with high entropy
	LoEntropy  uint64  `msgpack:"LoEntropy,omitempty"`  // Total number of files with low entropy
}

type Statistics struct {
	Directory Directory `msgpack:"directory,omitempty"` // Directory statistics
	Below     Below     `msgpack:"under,omitempty"`     // Under statistics
}
