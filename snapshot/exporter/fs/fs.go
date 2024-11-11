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
	"io"
	"os"
	"strings"

	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
)

type FSExporter struct {
	exporter.ExporterBackend
	rootDir string
}

func init() {
	exporter.Register("fs", NewFSExporter)
}

func NewFSExporter(location string) (exporter.ExporterBackend, error) {
	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	return &FSExporter{
		rootDir: location,
	}, nil
}

func (p *FSExporter) Begin(config string) error {
	if strings.HasPrefix(config, "fs://") {
		config = config[4:]
	}
	p.rootDir = config // duplicate for now, config might change later
	return nil
}

func (p *FSExporter) Root() string {
	return p.rootDir
}

func (p *FSExporter) CreateDirectory(pathname string, fileinfo *objects.FileInfo) error {
	os.MkdirAll(pathname, 0700)
	//os.Chmod(pathname, fileinfo.Mode())
	//os.Chown(pathname, int(fileinfo.Uid()), int(fileinfo.Gid()))
	return nil
}

func (p *FSExporter) StoreFile(pathname string, fileinfo *objects.FileInfo, fp io.Reader) error {
	f, err := os.Create(pathname)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, fp); err != nil {
		logger.Warn("copy failure: %s: %s", pathname, err)
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		logger.Warn("sync failure: %s: %s", pathname, err)
	}
	if err := f.Close(); err != nil {
		logger.Warn("close failure: %s: %s", pathname, err)
	}
	//if err := os.Chmod(pathname, fileinfo.Mode()); err != nil {
	//	logger.Warn("chmod failure: %s: %s", pathname, err)
	//}
	//if err := os.Chown(pathname, int(fileinfo.Uid()), int(fileinfo.Gid())); err != nil {
	//	if err == os.ErrPermission {
	//		logger.Warn("chown failure: %s: %s", pathname, err)
	//	}
	//}
	return nil
}

func (p *FSExporter) Close() error {
	return nil
}
