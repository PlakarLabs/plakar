/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
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

package classifier

import (
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

const VERSION string = "0.6.0"

type Result struct {
	Name        string
	Probability float64
}

type Backend interface {
	Directory(dirEntry *vfs.DirEntry) (*Result, error)
	File(fileEntry *vfs.FileEntry) (*Result, error)
}

var muBackends sync.Mutex
var backends map[string]func() Backend = make(map[string]func() Backend)

type Classifier struct {
	backend Backend
	context *context.Context
}

func NewClassifier(ctx *context.Context, name string) (*Classifier, error) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if backend, exists := backends[name]; !exists {
		return nil, fmt.Errorf("backend '%s' does not exist", name)
	} else {
		cf := &Classifier{}
		cf.context = ctx
		cf.backend = backend()
		return cf, nil
	}
}

func Register(name string, backend func() Backend) {
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

func (cf *Classifier) Directory(dirEntry *vfs.DirEntry) (*Result, error) {
	return cf.backend.Directory(dirEntry)
}

func (cf *Classifier) File(fileEntry *vfs.FileEntry) (*Result, error) {
	return cf.backend.File(fileEntry)
}

func (cf *Classifier) Close() error {
	return nil
}
