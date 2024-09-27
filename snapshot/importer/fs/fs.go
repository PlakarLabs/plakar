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
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/snapshot/importer"
)

type FSImporter struct {
	importer.ImporterBackend
	rootDir string
}

func init() {
	importer.Register("fs", NewFSImporter)
}

func NewFSImporter(location string) (importer.ImporterBackend, error) {
	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}
	return &FSImporter{
		rootDir: location,
	}, nil
}

func (p *FSImporter) Scan() (<-chan importer.ScanResult, error) {
	c := make(chan importer.ScanResult)
	go func() {
		directory := filepath.Clean(p.rootDir)
		atoms := strings.Split(directory, string(os.PathSeparator))
		for i := 0; i < len(atoms)-1; i++ {
			path := filepath.Clean(fmt.Sprintf("%s%s", string(os.PathSeparator), strings.Join(atoms[0:i+1], string(os.PathSeparator))))

			if runtime.GOOS == "windows" {
				path = path[1:]
			}

			f, err := os.Stat(path)
			if err != nil {
				c <- importer.ScanError{Pathname: path, Err: err}
				return
			}
			fileinfo := objects.FileInfoFromStat(f)
			c <- importer.ScanRecord{Pathname: filepath.ToSlash(path), Stat: fileinfo}
		}

		filepath.WalkDir(directory, func(path string, di fs.DirEntry, err error) error {
			if err != nil {
				c <- importer.ScanError{Pathname: path, Err: err}
				return nil
			}

			info, err := di.Info()
			if err != nil {
				c <- importer.ScanError{Pathname: path, Err: err}
				return nil
			}

			fileinfo := objects.FileInfoFromStat(info)
			c <- importer.ScanRecord{Pathname: filepath.ToSlash(path), Stat: fileinfo}

			if !fileinfo.Mode().IsDir() && !fileinfo.Mode().IsRegular() {
				lstat, err := os.Lstat(path)
				if err != nil {
					c <- importer.ScanError{Pathname: path, Err: err}
					return nil
				}

				lfileinfo := objects.FileInfoFromStat(lstat)
				if lfileinfo.Mode()&os.ModeSymlink != 0 {
					originFile, err := os.Readlink(lfileinfo.Name())
					if err != nil {
						c <- importer.ScanError{Pathname: path, Err: err}
						return nil
					}
					c <- importer.ScanLink{Pathname: filepath.ToSlash(path), Target: originFile, Stat: lfileinfo}
				}
			}
			return nil
		})
		close(c)
	}()
	return c, nil
}

func (p *FSImporter) NewReader(pathname string) (io.ReadCloser, error) {
	return os.Open(pathname)
}

func (p *FSImporter) Close() error {
	return nil
}

func (p *FSImporter) Root() string {
	return p.rootDir
}
