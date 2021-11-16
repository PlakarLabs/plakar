package filesystem

import (
	"os"
	"syscall"

	"github.com/dustin/go-humanize"
)

func FileinfoFromStat(stat os.FileInfo) Fileinfo {
	return Fileinfo{
		Name:    stat.Name(),
		Size:    stat.Size(),
		Mode:    stat.Mode(),
		ModTime: stat.ModTime(),
		Dev:     uint64(stat.Sys().(*syscall.Stat_t).Dev),
		Ino:     uint64(stat.Sys().(*syscall.Stat_t).Ino),
		Uid:     uint64(stat.Sys().(*syscall.Stat_t).Uid),
		Gid:     uint64(stat.Sys().(*syscall.Stat_t).Gid),
	}
}

func (fileinfo *Fileinfo) HumanSize() string {
	return humanize.Bytes(uint64(fileinfo.Size))
}
