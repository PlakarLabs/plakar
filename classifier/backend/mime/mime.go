package mime

import (
	"github.com/PlakarKorp/plakar/classifier"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

type Classifier struct {
}

func NewClassifier() *Classifier {
	return &Classifier{}
}

func (c *Classifier) Directory(dirEntry *vfs.DirEntry) (*classifier.Result, error) {
	return nil, nil
}

func (c *Classifier) File(fileEntry *vfs.FileEntry) (*classifier.Result, error) {
	return nil, nil
}
