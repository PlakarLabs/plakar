package plakarfs

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"syscall"

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

	entry, err := f.vfs.GetEntry(f.fullpath)
	if err != nil {
		return syscall.ENOENT
	}

	if entry.Stat().IsDir() {
		panic(fmt.Sprintf("unexpected type %T", entry))
	}

	a.Rdev = uint32(entry.Stat().Dev())
	a.Inode = entry.Stat().Ino()
	a.Mode = entry.Stat().Mode()
	a.Uid = uint32(entry.Stat().Uid())
	a.Gid = uint32(entry.Stat().Gid())
	a.Ctime = entry.Stat().ModTime()
	a.Mtime = entry.Stat().ModTime()
	a.Size = uint64(entry.Stat().Size())
	a.Nlink = uint32(entry.Stat().Nlink())
	return nil
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	rd, err := f.parent.snap.NewReader(f.fullpath)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}
