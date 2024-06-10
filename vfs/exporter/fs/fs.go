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

package fs

import (
	"fmt"
	"io"
	"strings"

	"github.com/PlakarLabs/plakar/vfs/exporter"
)

type FSExporter struct {
	exporter.ExporterBackend
	config  string
	rootDir string
}

func init() {
	exporter.Register("fs", NewFSExporter)
}

func NewFSExporter() exporter.ExporterBackend {
	return &FSExporter{}
}

func (p *FSExporter) Begin(config string) error {
	if strings.HasPrefix(config, "fs://") {
		config = config[4:]
	}
	p.config = config
	p.rootDir = config // duplicate for now, config might change later
	return nil
}

func (p *FSExporter) Root() string {
	return p.rootDir
}

func (p *FSExporter) Store(pathname string, fp io.ReadCloser) error {
	fmt.Println(pathname, fp)
	return nil
}

func (p *FSExporter) End() error {
	return nil
}
