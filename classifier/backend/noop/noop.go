package mime

import (
	"github.com/PlakarKorp/plakar/classifier"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

const NAME = "noop"

func init() {
	classifier.Register(NAME, NewClassifier)
}

type Classifier struct {
}

type Processor struct {
	classifier classifier.Backend
	pathname   string
}

func NewClassifier() classifier.Backend {
	return &Classifier{}
}

func (c *Classifier) Processor(backend classifier.Backend, pathname string) classifier.ProcessorBackend {
	return &Processor{
		classifier: backend,
		pathname:   pathname,
	}
}

func (c *Classifier) Close() error {
	return nil
}

func (p *Processor) Name() string {
	return NAME
}

func (p *Processor) Directory(dirEntry *vfs.DirEntry) []string {
	return []string{}
}

func (p *Processor) File(fileEntry *vfs.FileEntry) []string {
	return []string{}
}

func (p *Processor) Write(data []byte) bool {
	// return false when the processor should no longer be called until finalized
	return false
}

func (p *Processor) Finalize() []string {
	return []string{}
}
