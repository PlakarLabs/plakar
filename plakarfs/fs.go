package plakarfs

import (
	"github.com/PlakarKorp/plakar/repository"
	"github.com/anacrolix/fuse/fs"
)

type FS struct {
	repo *repository.Repository
}

func NewFS(repo *repository.Repository, mountpoint string) *FS {
	fs := &FS{
		repo: repo,
	}
	return fs
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{name: "/", repo: f.repo}, nil
}
