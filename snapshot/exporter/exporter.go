package exporter

import (
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/PlakarKorp/plakar/objects"
)

type ExporterBackend interface {
	Root() string
	CreateDirectory(pathname string) error
	StoreFile(pathname string, fp io.Reader) error
	SetPermissions(pathname string, fileinfo *objects.FileInfo) error
	Close() error
}

type Exporter struct {
	backend ExporterBackend
}

var muBackends sync.Mutex
var backends map[string]func(location string) (ExporterBackend, error) = make(map[string]func(location string) (ExporterBackend, error))

func Register(name string, backend func(location string) (ExporterBackend, error)) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if _, ok := backends[name]; ok {
		log.Fatalf("backend '%s' registered twice", name)
	}
	backends[name] = backend
}

func Backends() []string {
	muBackends.Lock()
	defer muBackends.Unlock()

	ret := make([]string, 0)
	for backendName := range backends {
		ret = append(ret, backendName)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

func NewExporter(location string) (*Exporter, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	var backendName string
	if !strings.HasPrefix(location, "/") {
		if strings.HasPrefix(location, "s3://") {
			backendName = "s3"
		} else if strings.HasPrefix(location, "fs://") {
			backendName = "fs"
		} else {
			if strings.Contains(location, "://") {
				return nil, fmt.Errorf("unsupported importer protocol")
			} else {
				backendName = "fs"
			}
		}
	} else {
		backendName = "fs"
	}

	if backend, exists := backends[backendName]; !exists {
		return nil, fmt.Errorf("backend '%s' does not exist", backendName)
	} else {
		backendInstance, err := backend(location)
		if err != nil {
			return nil, err
		}
		return &Exporter{backend: backendInstance}, nil
	}
}

func (exporter *Exporter) Root() string {
	return exporter.backend.Root()
}

func (exporter *Exporter) CreateDirectory(pathname string) error {
	return exporter.backend.CreateDirectory(pathname)
}

func (exporter *Exporter) StoreFile(pathname string, fp io.Reader) error {
	return exporter.backend.StoreFile(pathname, fp)
}

func (exporter *Exporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	return exporter.backend.SetPermissions(pathname, fileinfo)
}

func (exporter *Exporter) Close() error {
	return exporter.backend.Close()
}
