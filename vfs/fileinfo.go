/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package vfs

import (
	"os"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	_ "github.com/vmihailenco/msgpack/v5"
)

type FileInfo struct {
	Lname    string      `json:"Name" msgpack:"Name"`
	Lsize    int64       `json:"Size" msgpack:"Size"`
	Lmode    os.FileMode `json:"Mode" msgpack:"Mode"`
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
