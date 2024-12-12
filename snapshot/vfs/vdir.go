package vfs

import (
	"errors"
	"io/fs"
)

type VDirectory struct {
	vfs      *Filesystem
	vfsEntry *DirEntry
}

func NewVDirectory(fs *Filesystem, entry *DirEntry) *VDirectory {
	return &VDirectory{
		vfs:      fs,
		vfsEntry: entry,
	}
}

func (vd *VDirectory) Read(p []byte) (int, error) {
	return 0, fs.ErrInvalid
}

func (vd *VDirectory) Seek(offset int64, whence int) (int64, error) {
	return 0, fs.ErrInvalid
}

func (vd *VDirectory) Stat() (fs.FileInfo, error) {
	return vd.vfsEntry.FileInfo, nil
}

func (vd *VDirectory) Close() error {
	return nil
}

func (vd *VDirectory) Name() string {
	return vd.vfsEntry.Name()
}

func (vd *VDirectory) IsDir() bool {
	return true
}

func (vd *VDirectory) Type() fs.FileMode {
	return fs.ModeDir
}

func (vd *VDirectory) Info() (fs.FileInfo, error) {
	// Return fs.FileInfo for the entry
	return vd.vfsEntry.FileInfo, nil
}

func (vf *VDirectory) ReadDir(count int) ([]fs.DirEntry, error) {
	return nil, errors.New("not a implemented yet")
}
