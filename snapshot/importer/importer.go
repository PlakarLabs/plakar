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

package importer

import (
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/profiler"
)

type ScanResult interface {
	scanResult()
}

type RecordType int8

const (
	RecordTypeFile      RecordType = 0
	RecordTypeDirectory RecordType = 1
	RecordTypeSymlink   RecordType = 2
	RecordTypeDevice    RecordType = 3
	RecordTypePipe      RecordType = 4
	RecordTypeSocket    RecordType = 5
)

type FileAttributes struct {
	IsHidden    bool // Hidden file attribute (Windows, Linux)
	IsSystem    bool // System file attribute (Windows)
	IsReadonly  bool // Read-only attribute
	IsArchive   bool // Archive attribute (Windows)
	IsTemporary bool // Temporary file (Windows)
}

type ChildEntry struct {
	Name string
	Stat objects.FileInfo
}

type ScanRecord struct {
	Type               RecordType
	Pathname           string
	Target             string
	Stat               objects.FileInfo
	Children           []objects.FileInfo
	ExtendedAttributes map[string][]byte
	FileAttributes     []string
}

func (r ScanRecord) scanResult() {}

type ScanError struct {
	Pathname string
	Err      error
}

func (r ScanError) scanResult() {}

type ImporterBackend interface {
	Root() string
	Scan() (<-chan ScanResult, error)
	NewReader(pathname string) (io.ReadCloser, error)
	Close() error
}

type Importer struct {
	backend ImporterBackend
}

var muBackends sync.Mutex
var backends map[string]func(config string) (ImporterBackend, error) = make(map[string]func(config string) (ImporterBackend, error))

func Register(name string, backend func(string) (ImporterBackend, error)) {
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

func NewImporter(location string) (*Importer, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	var backendName string
	if !strings.HasPrefix(location, "/") {
		if strings.HasPrefix(location, "s3://") {
			backendName = "s3"
		} else if strings.HasPrefix(location, "fs://") {
			backendName = "fs"
		} else if strings.HasPrefix(location, "ftp://") {
			backendName = "ftp"
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
		return &Importer{backend: backendInstance}, nil
	}
}

func (importer *Importer) Root() string {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.importer.Root", time.Since(t0))
		logger.Trace("importer", "importer.Root(): %s", time.Since(t0))
	}()

	return importer.backend.Root()
}

func (importer *Importer) Scan() (<-chan ScanResult, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.importer.Scan", time.Since(t0))
		logger.Trace("importer", "importer.Scan(): %s", time.Since(t0))
	}()

	return importer.backend.Scan()
}

func (importer *Importer) NewReader(pathname string) (io.ReadCloser, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.importer.NewReader", time.Since(t0))
		logger.Trace("importer", "importer.NewReader(%s): %s", pathname, time.Since(t0))
	}()

	return importer.backend.NewReader(pathname)
}

func (importer *Importer) Close() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("snapshot.importer.Close", time.Since(t0))
		logger.Trace("importer", "importer.Close(): %s", time.Since(t0))
	}()

	return importer.backend.Close()
}
