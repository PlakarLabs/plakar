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
	}
}

func NewFileInfo(name string, size int64, mode os.FileMode, modTime time.Time, dev uint64, ino uint64, uid uint64, gid uint64) FileInfo {
	return FileInfo{
		Lname:    name,
		Lsize:    size,
		Lmode:    mode,
		LmodTime: modTime,
		Ldev:     dev,
		Lino:     ino,
		Luid:     uid,
		Lgid:     gid,
	}
}

func (fileinfo *FileInfo) HumanSize() string {
	return humanize.Bytes(uint64(fileinfo.Size()))
}
