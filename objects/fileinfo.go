package objects

import (
	"errors"
	"io/fs"
	"os"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
)

type FileInfo struct {
	Lname      string      `json:"Name" msgpack:"name"`
	Lsize      int64       `json:"Size" msgpack:"size"`
	Lmode      fs.FileMode `json:"Mode" msgpack:"mode"`
	LmodTime   time.Time   `json:"ModTime" msgpack:"modTime"`
	Ldev       uint64      `json:"Dev" msgpack:"dev"`
	Lino       uint64      `json:"Ino" msgpack:"ino"`
	Luid       uint64      `json:"Uid" msgpack:"uid"`
	Lgid       uint64      `json:"Gid" msgpack:"gid"`
	Lnlink     uint16      `json:"Nlink" msgpack:"nlink"`
	Lusername  string      `json:"Username" msgpack:"username"`
	Lgroupname string      `json:"Groupname" msgpack:"groupname"`
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

func (f FileInfo) Username() string {
	return f.Lusername
}

func (f FileInfo) Groupname() string {
	return f.Lgroupname
}

func FileInfoFromStat(stat fs.FileInfo) FileInfo {
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

func ParseFileInfoSortKeys(sortKeysStr string) ([]string, error) {
	if sortKeysStr == "" {
		return nil, nil
	}
	keys := strings.Split(sortKeysStr, ",")
	uniqueKeys := make(map[string]bool)
	validKeys := []string{}

	headerType := reflect.TypeOf(FileInfo{})

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if uniqueKeys[key] {
			return nil, errors.New("duplicate sort key: " + key)
		}
		uniqueKeys[key] = true

		if _, found := headerType.FieldByName(key); !found {
			return nil, errors.New("invalid sort key: " + key)
		}
		validKeys = append(validKeys, key)
	}

	return validKeys, nil
}

func SortFileInfos(infos []FileInfo, sortKeys []string, reversed bool) error {
	var err error
	sort.Slice(infos, func(i, j int) bool {
		for _, key := range sortKeys {
			switch key {
			case "ModTime":
				if !infos[i].ModTime().Equal(infos[j].ModTime()) {
					return infos[i].ModTime().Before(infos[j].ModTime())
				}
			case "Name":
				if infos[i].Name() != infos[j].Name() {
					return infos[i].Name() < infos[j].Name()
				}
			case "Size":
				if infos[i].Size() != infos[j].Size() {
					return infos[i].Size() < infos[j].Size()
				}
			case "Mode":
				if infos[i].Mode() != infos[j].Mode() {
					return infos[i].Mode() < infos[j].Mode()
				}
			case "Dev":
				if infos[i].Dev() != infos[j].Dev() {
					return infos[i].Dev() < infos[j].Dev()
				}
			case "Ino":
				if infos[i].Ino() != infos[j].Ino() {
					return infos[i].Ino() < infos[j].Ino()
				}
			case "Uid":
				if infos[i].Uid() != infos[j].Uid() {
					return infos[i].Uid() < infos[j].Uid()
				}
			case "Gid":
				if infos[i].Gid() != infos[j].Gid() {
					return infos[i].Gid() < infos[j].Gid()
				}
			case "Nlink":
				if infos[i].Nlink() != infos[j].Nlink() {
					return infos[i].Nlink() < infos[j].Nlink()
				}
			case "Username":
				if infos[i].Username() != infos[j].Username() {
					return infos[i].Username() < infos[j].Username()
				}
			case "Groupname":
				if infos[i].Groupname() != infos[j].Groupname() {
					return infos[i].Groupname() < infos[j].Groupname()
				}
			default:
				err = errors.New("invalid sort key: " + key)
				return false
			}
		}
		return false
	})

	if err == nil && reversed {
		for i, j := 0, len(infos)-1; i < j; i, j = i+1, j-1 {
			infos[i], infos[j] = infos[j], infos[i]
		}
	}
	return err
}
