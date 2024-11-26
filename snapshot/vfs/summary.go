package vfs

import (
	"io/fs"
	"os"
	"strings"

	"github.com/PlakarKorp/plakar/snapshot/importer"
)

type FileSummary struct {
	Type        importer.RecordType `msgpack:"type"`
	Size        uint64              `msgpack:"size"`
	Objects     uint64              `msgpack:"objects"`
	Chunks      uint64              `msgpack:"chunks"`
	Mode        fs.FileMode         `msgpack:"mode"`
	ModTime     int64               `msgpack:"modTime"`
	ContentType string              `msgpack:"contentType"`
	Entropy     float64             `msgpack:"entropy"`
}

type Directory struct {
	Directories uint64 `msgpack:"directories,omitempty"`
	Files       uint64 `msgpack:"files,omitempty"`
	Symlinks    uint64 `msgpack:"symlinks,omitempty"` // Total number of symlinks in the directory
	Devices     uint64 `msgpack:"devices,omitempty"`  // Total number of devices in the directory
	Pipes       uint64 `msgpack:"pipes,omitempty"`    // Total number of pipes in the directory
	Sockets     uint64 `msgpack:"sockets,omitempty"`  // Total number of sockets in the directory

	Setuid uint64 `msgpack:"setuid,omitempty"` // Total number of setuid files in the directory
	Setgid uint64 `msgpack:"setgid,omitempty"` // Total number of setgid files in the directory
	Sticky uint64 `msgpack:"sticky,omitempty"` // Total number of sticky files in the directory

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
	SumEntropy float64 `msgpack:"sumEntropy,omitempty"` // Sum of entropy of the directory
	AvgEntropy float64 `msgpack:"avgEntropy,omitempty"` // Average entropy of the directory
	HiEntropy  uint64  `msgpack:"HiEntropy,omitempty"`  // Total number of files with high entropy
	LoEntropy  uint64  `msgpack:"LoEntropy,omitempty"`  // Total number of files with low entropy

	MIMEAudio       uint64 `msgpack:"MIMEAudio,omitempty"`       // Total number of audio files in the directory
	MIMEVideo       uint64 `msgpack:"MIMEVideo,omitempty"`       // Total number of video files in the directory
	MIMEImage       uint64 `msgpack:"MIMEImage,omitempty"`       // Total number of image files in the directory
	MIMEText        uint64 `msgpack:"MIMEText,omitempty"`        // Total number of text files in the directory
	MIMEApplication uint64 `msgpack:"MIMEApplication,omitempty"` // Total number of text files in the directory
	MIMEOther       uint64 `msgpack:"MIMEOther,omitempty"`       // Total number of other files in the directory

	Errors uint64 `msgpack:"errors,omitempty"` // Total number of errors in the directory
}

type Below struct {
	Directories uint64 `msgpack:"directories,omitempty"`
	Files       uint64 `msgpack:"files,omitempty"`
	Symlinks    uint64 `msgpack:"symlinks,omitempty"` // Total number of symlinks in the directory
	Devices     uint64 `msgpack:"devices,omitempty"`  // Total number of devices in the directory
	Pipes       uint64 `msgpack:"pipes,omitempty"`    // Total number of pipes in the directory
	Sockets     uint64 `msgpack:"sockets,omitempty"`  // Total number of sockets in the directory

	Setuid uint64 `msgpack:"setuid,omitempty"` // Total number of setuid files in the directory
	Setgid uint64 `msgpack:"setgid,omitempty"` // Total number of setgid files in the directory
	Sticky uint64 `msgpack:"sticky,omitempty"` // Total number of sticky files in the directory

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

	MIMEAudio       uint64 `msgpack:"MIMEAudio,omitempty"`       // Total number of audio files in the directory
	MIMEVideo       uint64 `msgpack:"MIMEVideo,omitempty"`       // Total number of video files in the directory
	MIMEImage       uint64 `msgpack:"MIMEImage,omitempty"`       // Total number of image files in the directory
	MIMEText        uint64 `msgpack:"MIMEText,omitempty"`        // Total number of text files in the directory
	MIMEApplication uint64 `msgpack:"MIMEApplication,omitempty"` // Total number of text files in the directory
	MIMEOther       uint64 `msgpack:"MIMEOther,omitempty"`       // Total number of other files in the directory

	Errors uint64 `msgpack:"errors,omitempty"` // Total number of errors in the directory
}

type Summary struct {
	Directory Directory `msgpack:"directory,omitempty"` // Directory statistics
	Below     Below     `msgpack:"under,omitempty"`     // Under statistics
}

func (s *Summary) UpdateBelow(below *Summary) {
	s.Below.Files += below.Below.Files + below.Directory.Files
	s.Below.Directories += below.Below.Directories + below.Directory.Directories
	s.Below.Symlinks += below.Below.Symlinks + below.Directory.Symlinks
	s.Below.Devices += below.Below.Devices + below.Directory.Devices
	s.Below.Pipes += below.Below.Pipes + below.Directory.Pipes
	s.Below.Sockets += below.Below.Sockets + below.Directory.Sockets

	s.Below.Setuid += below.Below.Setuid + below.Directory.Setuid
	s.Below.Setgid += below.Below.Setgid + below.Directory.Setgid
	s.Below.Sticky += below.Below.Sticky + below.Directory.Sticky

	s.Below.Objects += below.Below.Objects + below.Directory.Objects
	s.Below.Chunks += below.Below.Chunks + below.Directory.Chunks

	if s.Below.MinSize == 0 || below.Below.MinSize < s.Below.MinSize {
		s.Below.MinSize = below.Below.MinSize
	}
	if s.Below.MinSize == 0 || below.Directory.MinSize < s.Below.MinSize {
		s.Below.MinSize = below.Directory.MinSize
	}
	if s.Below.MaxSize == 0 || below.Below.MaxSize > s.Below.MaxSize {
		s.Below.MaxSize = below.Below.MaxSize
	}
	if s.Below.MaxSize == 0 || below.Directory.MaxSize > s.Below.MaxSize {
		s.Below.MaxSize = below.Directory.MaxSize
	}
	s.Below.Size += below.Below.Size + below.Directory.Size

	if s.Below.MinModTime == 0 || below.Below.MinModTime < s.Below.MinModTime {
		s.Below.MinModTime = below.Below.MinModTime
	}
	if s.Below.MinModTime == 0 || below.Directory.MinModTime < s.Below.MinModTime {
		s.Below.MinModTime = below.Directory.MinModTime
	}
	if s.Below.MaxModTime == 0 || below.Below.MaxModTime > s.Below.MaxModTime {
		s.Below.MaxModTime = below.Below.MaxModTime
	}
	if s.Below.MaxModTime == 0 || below.Directory.MaxModTime > s.Below.MaxModTime {
		s.Below.MaxModTime = below.Directory.MaxModTime
	}

	if s.Below.MinEntropy == 0 || below.Below.MinEntropy < s.Below.MinEntropy {
		s.Below.MinEntropy = below.Below.MinEntropy
	}
	if s.Below.MinEntropy == 0 || below.Directory.MinEntropy < s.Below.MinEntropy {
		s.Below.MinEntropy = below.Directory.MinEntropy
	}
	if s.Below.MaxEntropy == 0 || below.Below.MaxEntropy > s.Below.MaxEntropy {
		s.Below.MaxEntropy = below.Below.MaxEntropy
	}
	if s.Below.MaxEntropy == 0 || below.Directory.MaxEntropy > s.Below.MaxEntropy {
		s.Below.MaxEntropy = below.Directory.MaxEntropy
	}
	s.Below.HiEntropy += below.Below.HiEntropy + below.Directory.HiEntropy
	s.Below.LoEntropy += below.Below.LoEntropy + below.Directory.LoEntropy

	s.Below.MIMEAudio += below.Directory.MIMEAudio + below.Below.MIMEAudio
	s.Below.MIMEVideo += below.Directory.MIMEVideo + below.Below.MIMEVideo
	s.Below.MIMEImage += below.Directory.MIMEImage + below.Below.MIMEImage
	s.Below.MIMEText += below.Directory.MIMEText + below.Below.MIMEText
	s.Below.MIMEApplication += below.Directory.MIMEApplication + below.Below.MIMEApplication
	s.Below.MIMEOther += below.Directory.MIMEOther + below.Below.MIMEOther
	s.Below.Errors += below.Directory.Errors + below.Below.Errors
}

func (s *Summary) UpdateWithFileSummary(fileSummary *FileSummary) {
	switch fileSummary.Type {
	case importer.RecordTypeFile:
		s.Directory.Files++
	case importer.RecordTypeDirectory:
		s.Directory.Directories++
	case importer.RecordTypeSymlink:
		s.Directory.Symlinks++
	case importer.RecordTypeDevice:
		s.Directory.Devices++
	case importer.RecordTypePipe:
		s.Directory.Pipes++
	case importer.RecordTypeSocket:
		s.Directory.Sockets++
	default:
		panic("unexpected record type")
	}

	if fileSummary.Mode&os.ModeSetuid != 0 {
		s.Directory.Setuid++
	}
	if fileSummary.Mode&os.ModeSetgid != 0 {
		s.Directory.Setgid++
	}
	if fileSummary.Mode&os.ModeSticky != 0 {
		s.Directory.Sticky++
	}

	if fileSummary.Objects > 0 {
		s.Directory.Objects += fileSummary.Objects
		s.Directory.Chunks += fileSummary.Chunks
	}

	if fileSummary.ModTime < s.Directory.MinModTime || s.Directory.MinModTime == 0 {
		s.Directory.MinModTime = fileSummary.ModTime
	}
	if fileSummary.ModTime > s.Directory.MaxModTime || s.Directory.MaxModTime == 0 {
		s.Directory.MaxModTime = fileSummary.ModTime
	}

	if fileSummary.Size < s.Directory.MinSize || s.Directory.MinSize == 0 {
		s.Directory.MinSize = fileSummary.Size
	}
	if fileSummary.Size > s.Directory.MaxSize || s.Directory.MaxSize == 0 {
		s.Directory.MaxSize = fileSummary.Size
	}

	if fileSummary.Entropy < s.Directory.MinEntropy || s.Directory.MinEntropy == 0 {
		s.Directory.MinEntropy = fileSummary.Entropy
	}
	if fileSummary.Entropy > s.Directory.MaxEntropy || s.Directory.MaxEntropy == 0 {
		s.Directory.MaxEntropy = fileSummary.Entropy
	}

	if fileSummary.Entropy <= 2.0 {
		s.Directory.LoEntropy++
	} else if fileSummary.Entropy >= 7.0 {
		s.Directory.HiEntropy++
	}

	if fileSummary.ContentType != "" {
		if strings.HasPrefix(fileSummary.ContentType, "text/") {
			s.Directory.MIMEText++
		} else if strings.HasPrefix(fileSummary.ContentType, "image/") {
			s.Directory.MIMEImage++
		} else if strings.HasPrefix(fileSummary.ContentType, "audio/") {
			s.Directory.MIMEAudio++
		} else if strings.HasPrefix(fileSummary.ContentType, "video/") {
			s.Directory.MIMEVideo++
		} else if strings.HasPrefix(fileSummary.ContentType, "application/") {
			s.Directory.MIMEApplication++
		} else {
			s.Directory.MIMEOther++
		}
	}
	s.Directory.Size += fileSummary.Size
	s.Directory.SumEntropy += fileSummary.Entropy
}

func (s *Summary) UpdateAverages() {
	if s.Directory.Files > 0 {
		s.Directory.AvgSize = s.Directory.Size / s.Directory.Files
		s.Directory.AvgEntropy = s.Directory.SumEntropy / float64(s.Directory.Files)
	}
}
