package objects

import (
	"encoding/base64"
	"encoding/json"
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/vmihailenco/msgpack/v5"
)

type CustomMetadata struct {
	Key   string `msgpack:"key"`
	Value []byte `msgpack:"value"`
}

type Object struct {
	Checksum       [32]byte         `msgpack:"checksum"`
	Chunks         []Chunk          `msgpack:"chunks"`
	ContentType    string           `msgpack:"contentType,omitempty"`
	CustomMetadata []CustomMetadata `msgpack:"customMetadata,omitempty"`
	Tags           []string         `msgpack:"tags,omitempty"`
	Entropy        float64          `msgpack:"entropy,omitempty"`
}

func (m Object) MarshalJSON() ([]byte, error) {
	type Alias Object // Create an alias to avoid recursion
	return json.Marshal(&struct {
		Checksum string `json:"Checksum"`
		*Alias
	}{
		Checksum: base64.RawURLEncoding.EncodeToString(m.Checksum[:]),
		Alias:    (*Alias)(&m),
	})
}

func NewObject() *Object {
	return &Object{
		CustomMetadata: make([]CustomMetadata, 0),
	}
}

func NewObjectFromBytes(serialized []byte) (*Object, error) {
	var o Object
	if err := msgpack.Unmarshal(serialized, &o); err != nil {
		return nil, err
	}
	if o.CustomMetadata == nil {
		o.CustomMetadata = make([]CustomMetadata, 0)
	}
	if o.Tags == nil {
		o.Tags = make([]string, 0)
	}
	return &o, nil
}

func (o *Object) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(o)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

type Chunk struct {
	Checksum [32]byte `msgpack:"checksum"`
	Length   uint32   `msgpack:"length"`
	Entropy  float64  `msgpack:"entropy"`
}

func (m Chunk) MarshalJSON() ([]byte, error) {
	type Alias Chunk // Create an alias to avoid recursion
	return json.Marshal(&struct {
		Checksum string `json:"Checksum"`
		*Alias
	}{
		Checksum: base64.RawURLEncoding.EncodeToString(m.Checksum[:]),
		Alias:    (*Alias)(&m),
	})
}

type FileInfo struct {
	Lname    string      `json:"Name" msgpack:"name"`
	Lsize    int64       `json:"Size" msgpack:"size"`
	Lmode    fs.FileMode `json:"Mode" msgpack:"mode"`
	LmodTime time.Time   `json:"ModTime" msgpack:"modTime"`
	Ldev     uint64      `json:"Dev" msgpack:"dev"`
	Lino     uint64      `json:"Ino" msgpack:"ino"`
	Luid     uint64      `json:"Uid" msgpack:"uid"`
	Lgid     uint64      `json:"Gid" msgpack:"gid"`
	Lnlink   uint16      `json:"Nlink" msgpack:"nlink"`
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
	Ldev := uint64(0)
	Lino := uint64(0)
	Luid := uint64(0)
	Lgid := uint64(0)
	Lnlink := uint16(0)

	if _, ok := stat.Sys().(*syscall.Stat_t); ok {
		Ldev = uint64(stat.Sys().(*syscall.Stat_t).Dev)
		Lino = uint64(stat.Sys().(*syscall.Stat_t).Ino)
		Luid = uint64(stat.Sys().(*syscall.Stat_t).Uid)
		Lgid = uint64(stat.Sys().(*syscall.Stat_t).Gid)
		Lnlink = uint16(stat.Sys().(*syscall.Stat_t).Nlink)
	}

	return FileInfo{
		Lname:    stat.Name(),
		Lsize:    stat.Size(),
		Lmode:    stat.Mode(),
		LmodTime: stat.ModTime(),
		Ldev:     Ldev,
		Lino:     Lino,
		Luid:     Luid,
		Lgid:     Lgid,
		Lnlink:   Lnlink,
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

func (fileinfo *FileInfo) Equal(fi *FileInfo) bool {
	return fileinfo.Lname == fi.Lname &&
		fileinfo.Lsize == fi.Lsize &&
		fileinfo.Lmode == fi.Lmode &&
		fileinfo.LmodTime == fi.LmodTime &&
		fileinfo.Ldev == fi.Ldev &&
		fileinfo.Lino == fi.Lino &&
		fileinfo.Luid == fi.Luid &&
		fileinfo.Lgid == fi.Lgid &&
		fileinfo.Lnlink == fi.Lnlink
}
