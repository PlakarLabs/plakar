package objects

import (
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/vmihailenco/msgpack/v5"
)

type Directory struct {
	Checksum [32]byte
	FileInfo FileInfo
}

func NewDirectory(checksum [32]byte, fileInfo FileInfo) *Directory {
	return &Directory{
		Checksum: checksum,
		FileInfo: fileInfo,
	}
}

func NewDirectoryFromBytes(serialized []byte) (*Directory, error) {
	var d Directory
	if err := msgpack.Unmarshal(serialized, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (d *Directory) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(d)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

type File struct {
	Checksum [32]byte
	FileInfo FileInfo
}

func NewFile(checksum [32]byte, fileInfo FileInfo) *File {
	return &File{
		Checksum: checksum,
		FileInfo: fileInfo,
	}
}

func NewFileFromBytes(serialized []byte) (*File, error) {
	var f File
	if err := msgpack.Unmarshal(serialized, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func (f *File) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(f)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

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
