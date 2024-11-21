package plakarfs

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"syscall"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
)

type Dir struct {
	parent   *Dir
	name     string
	fullpath string
	repo     *repository.Repository
	snap     *snapshot.Snapshot
	vfs      *vfs.Filesystem
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	if d.name == "/" {
		d.fullpath = d.name
		a.Inode = 1
		a.Mode = os.ModeDir | 0o700
		a.Uid = uint32(os.Geteuid())
		a.Gid = uint32(os.Getgid())
	} else if d.parent.name == "/" {
		snapshotID, err := hex.DecodeString(d.name)
		if err != nil {
			return err
		}
		if len(snapshotID) != 32 {
			return fmt.Errorf("invalid snapshot id length %d", len(snapshotID))
		}
		snap, err := snapshot.Load(d.repo, objects.Checksum(snapshotID))
		if err != nil {
			return err
		}
		snapfs, err := snap.Filesystem()
		if err != nil {
			return err
		}

		d.snap = snap
		d.repo = d.parent.repo
		d.vfs = snapfs
		d.fullpath = "/"

		a.Inode = rand.Uint64()
		a.Mode = os.ModeDir | 0o700
		a.Uid = uint32(os.Geteuid())
		a.Gid = uint32(os.Getgid())
		a.Ctime = snap.Header.CreationTime
		a.Mtime = snap.Header.CreationTime
		a.Atime = snap.Header.CreationTime
		a.Size = snap.Header.Summary.Directory.Size + snap.Header.Summary.Below.Size
	} else {
		d.snap = d.parent.snap
		d.repo = d.parent.repo
		d.vfs = d.parent.vfs
		d.fullpath = d.parent.fullpath + "/" + d.name

		d.fullpath = filepath.Clean(d.fullpath)

		fi, err := d.vfs.Stat(d.fullpath)
		if err != nil {
			return syscall.ENOENT
		}

		if fi, ok := fi.(*vfs.DirEntry); !ok {
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
		}
	}
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if d.name == "/" {
		return &Dir{parent: d, name: name, repo: d.repo}, nil
	} else if d.parent.name == "/" {
		return &Dir{parent: d, name: name}, nil
	} else {
		cleanpath := filepath.Clean(d.fullpath + "/" + name)
		fi, err := d.vfs.Stat(cleanpath)
		if err != nil {
			return nil, err
		}
		if _, isDir := fi.(*vfs.DirEntry); isDir {
			return &Dir{parent: d, name: name}, nil
		} else if _, isFile := fi.(*vfs.FileEntry); isFile {
			return &File{parent: d, name: name}, nil
		} else {
			return nil, fmt.Errorf("unknown file type")
		}
	}
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if d.name == "/" {

		d.repo.RebuildState()

		snapshotIDs, err := d.repo.GetSnapshots()
		if err != nil {
			return nil, err
		}
		dirDirs := make([]fuse.Dirent, 0)
		for idx, snapshotID := range snapshotIDs {
			dirDirs = append(dirDirs, fuse.Dirent{
				Inode: uint64(idx),
				Name:  fmt.Sprintf("%x", snapshotID),
				Type:  fuse.DT_Dir,
			})
		}
		return dirDirs, nil
	}

	children, err := d.vfs.Children(d.fullpath)
	if err != nil {
		return nil, err
	}

	dirDirs := make([]fuse.Dirent, 0)
	for child := range children {
		cleanpath := filepath.Clean(d.fullpath + "/" + child)
		fi, err := d.vfs.Stat(cleanpath)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		var dirEnt fuse.Dirent
		switch fi := fi.(type) {
		case *vfs.DirEntry:
			dirEnt.Inode = fi.Stat().Ino()
			dirEnt.Name = child
			dirEnt.Type = fuse.DT_Dir
		case *vfs.FileEntry:
			dirEnt.Inode = fi.Stat().Ino()
			dirEnt.Name = child
			dirEnt.Type = fuse.DT_File
		default:
			return nil, fmt.Errorf("unknown file type")
		}

		dirDirs = append(dirDirs, dirEnt)
	}
	return dirDirs, nil
}
