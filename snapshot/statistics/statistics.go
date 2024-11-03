package statistics

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type Statistics struct {
	ImporterStart       time.Time
	ImporterDuration    time.Duration
	ImporterRecords     uint64
	ImporterFiles       uint64
	ImporterDirectories uint64
	ImporterSymlinks    uint64
	ImporterDevices     uint64
	ImporterPipes       uint64
	ImporterSockets     uint64
	ImporterLinks       uint64
	ImporterSize        uint64
	ImporterErrors      uint64

	ScannerStart         time.Time
	ScannerDuration      time.Duration
	ScannerProcessedSize uint64

	ChunkerFiles   uint64
	ChunkerChunks  uint64
	ChunkerObjects uint64
	ChunkerSize    uint64
	ChunkerErrors  uint64

	ChunksCount         uint64
	ChunksSize          uint64
	ChunksTransferCount uint64
	ChunksTransferSize  uint64

	ObjectsCount         uint64
	ObjectsSize          uint64
	ObjectsTransferCount uint64
	ObjectsTransferSize  uint64

	DataCount         uint64
	DataSize          uint64
	DataTransferCount uint64
	DataTransferSize  uint64

	VFSFilesCount         uint64
	VFSFilesSize          uint64
	VFSFilesTransferCount uint64
	VFSFilesTransferSize  uint64

	VFSDirectoriesCount         uint64
	VFSDirectoriesSize          uint64
	VFSDirectoriesTransferCount uint64
	VFSDirectoriesTransferSize  uint64

	PackfilesCount         uint64
	PackfilesSize          uint64
	PackfilesTransferCount uint64
	PackfilesTransferSize  uint64
}

func New() *Statistics {
	return &Statistics{}
}

func FromBytes(data []byte) (*Statistics, error) {
	s := &Statistics{}
	err := msgpack.Unmarshal(data, s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Statistics) Serialize() ([]byte, error) {
	return msgpack.Marshal(s)
}
