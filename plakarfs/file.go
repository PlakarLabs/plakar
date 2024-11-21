package plakarfs

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/anacrolix/fuse"
)

// File implements both Node and Handle for the hello file.
type File struct {
	parent   *Dir
	name     string
	fullpath string
	repo     *repository.Repository
	vfs      *vfs.Filesystem
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.repo = f.parent.repo
	f.vfs = f.parent.vfs
	f.fullpath = f.parent.fullpath + "/" + f.name

	f.fullpath = filepath.Clean(f.fullpath)

	fi, err := f.vfs.Stat(f.fullpath)
	if err != nil {
		return err
	}
	if fi, ok := fi.(*vfs.FileEntry); !ok {
		panic(fmt.Sprintf("unexpected type %T", fi))
	} else {
		a.Rdev = uint32(fi.Stat().Dev())
		a.Inode = fi.Stat().Ino()
		a.Mode = fi.Stat().Mode()
		a.Uid = uint32(fi.Stat().Uid())
		a.Gid = uint32(fi.Stat().Gid())
		a.Ctime = fi.Stat().ModTime()
		a.Mtime = fi.Stat().ModTime()
		a.Size = uint64(fi.Stat().Size())
		a.Nlink = uint32(fi.Stat().Nlink())
	}
	return nil
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	rd, err := f.parent.snap.NewReader(f.fullpath)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}
