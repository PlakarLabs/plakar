package vfs

import (
	"io/fs"
	"os"
	"strings"

	"github.com/PlakarKorp/plakar/snapshot/importer"
)

type FileSummary struct {
	Type        importer.RecordType `msgpack:"type" json:"type"`
	Size        uint64              `msgpack:"size" json:"size"`
	Objects     uint64              `msgpack:"objects" json:"objects"`
	Chunks      uint64              `msgpack:"chunks" json:"chunks"`
	Mode        fs.FileMode         `msgpack:"mode" json:"mode"`
	ModTime     int64               `msgpack:"mod_time" json:"mod_time"`
	ContentType string              `msgpack:"content_type" json:"content_type"`
	Entropy     float64             `msgpack:"entropy" json:"entropy"`
}

type Directory struct {
	Directories uint64 `msgpack:"directories,omitempty" json:"directories,omitempty"`
	Files       uint64 `msgpack:"files,omitempty" json:"files,omitempty"`
	Symlinks    uint64 `msgpack:"symlinks,omitempty" json:"symlinks,omitempty"`
	Devices     uint64 `msgpack:"devices,omitempty" json:"devices,omitempty"`
	Pipes       uint64 `msgpack:"pipes,omitempty" json:"pipes,omitempty"`
	Sockets     uint64 `msgpack:"sockets,omitempty" json:"sockets,omitempty"`

	Children uint64 `msgpack:"children,omitempty" json:"children,omitempty"`

	Setuid uint64 `msgpack:"setuid,omitempty" json:"setuid,omitempty"`
	Setgid uint64 `msgpack:"setgid,omitempty" json:"setgid,omitempty"`
	Sticky uint64 `msgpack:"sticky,omitempty" json:"sticky,omitempty"`

	Objects uint64 `msgpack:"objects,omitempty" json:"objects,omitempty"`
	Chunks  uint64 `msgpack:"chunks,omitempty" json:"chunks,omitempty"`

	MinSize uint64 `msgpack:"min_size,omitempty" json:"min_size,omitempty"`
	MaxSize uint64 `msgpack:"max_size,omitempty" json:"max_size,omitempty"`
	AvgSize uint64 `msgpack:"avg_size,omitempty" json:"avg_size,omitempty"`
	Size    uint64 `msgpack:"size,omitempty" json:"size,omitempty"`

	MinModTime int64 `msgpack:"min_mod_time,omitempty" json:"min_mod_time,omitempty"`
	MaxModTime int64 `msgpack:"max_mod_time,omitempty" json:"max_mod_time,omitempty"`

	MinEntropy float64 `msgpack:"min_entropy,omitempty" json:"min_entropy,omitempty"`
	MaxEntropy float64 `msgpack:"max_entropy,omitempty" json:"max_entropy,omitempty"`
	SumEntropy float64 `msgpack:"sum_entropy,omitempty" json:"sum_entropy,omitempty"`
	AvgEntropy float64 `msgpack:"avg_entropy,omitempty" json:"avg_entropy,omitempty"`
	HiEntropy  uint64  `msgpack:"hi_entropy,omitempty" json:"hi_entropy,omitempty"`
	LoEntropy  uint64  `msgpack:"lo_entropy,omitempty" json:"lo_entropy,omitempty"`

	MIMEAudio       uint64 `msgpack:"MIMEAudio,omitempty" json:"MIMEAudio,omitempty"`
	MIMEVideo       uint64 `msgpack:"MIMEVideo,omitempty" json:"MIMEVideo,omitempty"`
	MIMEImage       uint64 `msgpack:"MIMEImage,omitempty" json:"MIMEImage,omitempty"`
	MIMEText        uint64 `msgpack:"MIMEText,omitempty" json:"MIMEText,omitempty"`
	MIMEApplication uint64 `msgpack:"MIMEApplication,omitempty" json:"MIMEApplication,omitempty"`
	MIMEOther       uint64 `msgpack:"MIMEOther,omitempty" json:"MIMEOther,omitempty"`

	Errors uint64 `msgpack:"errors,omitempty" json:"errors,omitempty"`
}

type Below struct {
	Directories uint64 `msgpack:"directories,omitempty" json:"directories,omitempty"`
	Files       uint64 `msgpack:"files,omitempty" json:"files,omitempty"`
	Symlinks    uint64 `msgpack:"symlinks,omitempty" json:"symlinks,omitempty"`
	Devices     uint64 `msgpack:"devices,omitempty" json:"devices,omitempty"`
	Pipes       uint64 `msgpack:"pipes,omitempty" json:"pipes,omitempty"`
	Sockets     uint64 `msgpack:"sockets,omitempty" json:"sockets,omitempty"`

	Children uint64 `msgpack:"children,omitempty" json:"children,omitempty"`

	Setuid uint64 `msgpack:"setuid,omitempty" json:"setuid,omitempty"`
	Setgid uint64 `msgpack:"setgid,omitempty" json:"setgid,omitempty"`
	Sticky uint64 `msgpack:"sticky,omitempty" json:"sticky,omitempty"`

	Objects uint64 `msgpack:"objects,omitempty" json:"objects,omitempty"`
	Chunks  uint64 `msgpack:"chunks,omitempty" json:"chunks,omitempty"`

	MinSize uint64 `msgpack:"min_size,omitempty" json:"min_size,omitempty"`
	MaxSize uint64 `msgpack:"max_size,omitempty" json:"max_size,omitempty"`
	Size    uint64 `msgpack:"size,omitempty" json:"size,omitempty"`

	MinModTime int64 `msgpack:"min_mod_time,omitempty" json:"min_mod_time,omitempty"`
	MaxModTime int64 `msgpack:"max_mod_time,omitempty" json:"man_mod_time,omitempty"`

	MinEntropy float64 `msgpack:"min_entropy,omitempty" json:"min_entropy,omitempty"`
	MaxEntropy float64 `msgpack:"max_entropy,omitempty" json:"max_entropy,omitempty"`
	HiEntropy  uint64  `msgpack:"hi_entropy,omitempty" json:"hi_entropy,omitempty"`
	LoEntropy  uint64  `msgpack:"lo_entropy,omitempty" json:"lo_entropy,omitempty"`

	MIMEAudio       uint64 `msgpack:"MIMEAudio,omitempty" json:"MIMEAudio,omitempty"`
	MIMEVideo       uint64 `msgpack:"MIMEVideo,omitempty" json:"MIMEVideo,omitempty"`
	MIMEImage       uint64 `msgpack:"MIMEImage,omitempty" json:"MIMEImage,omitempty"`
	MIMEText        uint64 `msgpack:"MIMEText,omitempty" json:"MIMEText,omitempty"`
	MIMEApplication uint64 `msgpack:"MIMEApplication,omitempty" json:"MIMEApplication,omitempty"`
	MIMEOther       uint64 `msgpack:"MIMEOther,omitempty" json:"MIMEOther,omitempty"`

	Errors uint64 `msgpack:"errors,omitempty"`
}

type Summary struct {
	Directory Directory `msgpack:"directory,omitempty" json:"directory,omitempty"`
	Below     Below     `msgpack:"below,omitempty" json:"below,omitempty"`
}

func (s *Summary) UpdateBelow(below *Summary) {
	s.Below.Files += below.Below.Files + below.Directory.Files
	s.Below.Directories += below.Below.Directories + below.Directory.Directories
	s.Below.Symlinks += below.Below.Symlinks + below.Directory.Symlinks
	s.Below.Devices += below.Below.Devices + below.Directory.Devices
	s.Below.Pipes += below.Below.Pipes + below.Directory.Pipes
	s.Below.Sockets += below.Below.Sockets + below.Directory.Sockets

	s.Below.Children += below.Below.Children + below.Directory.Children

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
