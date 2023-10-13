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
	"io/fs"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
)

type ImporterRecord struct {
	Pathname string
	Stat     fs.FileInfo
}

type ImporterBackend interface {
	Begin(config string) error
	Scan() (<-chan ImporterRecord, <-chan error, error)
	Open(pathname string) (io.ReadCloser, error)
	End() error
}

type Importer struct {
	backend ImporterBackend
}

var muBackends sync.Mutex
var backends map[string]func() ImporterBackend = make(map[string]func() ImporterBackend)

func Register(name string, backend func() ImporterBackend) {
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
		} else if strings.HasPrefix(location, "imap://") {
			backendName = "imap"
		} else if strings.HasPrefix(location, "fs://") {
			backendName = "fs"
		} else {
			return nil, fmt.Errorf("unsupported importer method")
		}
	} else {
		backendName = "fs"
	}

	if backend, exists := backends[backendName]; !exists {
		return nil, fmt.Errorf("backend '%s' does not exist", backendName)
	} else {
		provider := &Importer{}
		provider.backend = backend()
		return provider, nil
	}
}

func (importer *Importer) Begin(config string) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.importer.Begin", time.Since(t0))
		logger.Trace("vfs", "importer.Begin(%s): %s", config, time.Since(t0))
	}()

	return importer.backend.Begin(config)
}

func (importer *Importer) Scan() (<-chan ImporterRecord, <-chan error, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.importer.Scan", time.Since(t0))
		logger.Trace("vfs", "importer.Scan(): %s", time.Since(t0))
	}()

	return importer.backend.Scan()
}

func (importer *Importer) Open(pathname string) (io.ReadCloser, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.importer.Open", time.Since(t0))
		logger.Trace("vfs", "importer.Open(%s): %s", pathname, time.Since(t0))
	}()

	return importer.backend.Open(pathname)
}

func (importer *Importer) End() error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("vfs.importer.End", time.Since(t0))
		logger.Trace("vfs", "importer.End(): %s", time.Since(t0))
	}()

	return importer.backend.End()
}
