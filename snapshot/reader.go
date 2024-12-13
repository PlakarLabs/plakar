package snapshot

import (
	"io"
	"os"
	"path"

	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

func (snapshot *Snapshot) NewReader(pathname string) (io.ReadCloser, error) {
	return NewReader(snapshot, pathname)
}

func NewReader(snap *Snapshot, pathname string) (io.ReadCloser, error) {
	pathname = path.Clean(pathname)

	fs, err := vfs.NewFilesystem(snap.Repository(), snap.Header.Root)
	if err != nil {
		return nil, err
	}

	st, err := fs.Stat(pathname)
	if err != nil {
		return nil, err
	} else if _, isDir := st.(*vfs.DirEntry); isDir {
		return nil, os.ErrInvalid
	}
	return fs.Open(pathname)
}
