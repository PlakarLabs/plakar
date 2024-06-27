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

	"github.com/PlakarLabs/plakar/vfs"
	"github.com/PlakarLabs/plakar/vfs/importer"
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

func (p *FSImporter) Scan() (<-chan importer.ImporterRecord, <-chan error, error) {
	c := make(chan importer.ImporterRecord)
	cerr := make(chan error)
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
				cerr <- err
				close(cerr)
				return
			}
			fileinfo := vfs.FileInfoFromStat(f)
			c <- importer.ImporterRecord{Pathname: filepath.ToSlash(path), Stat: fileinfo}
		}

		err := filepath.WalkDir(directory, func(path string, di fs.DirEntry, err error) error {
			if err != nil {
				cerr <- err
				return nil
			}

			info, err := di.Info()
			if err != nil {
				cerr <- err
				return nil
			}

			fileinfo := vfs.FileInfoFromStat(info)
			c <- importer.ImporterRecord{Pathname: filepath.ToSlash(path), Stat: fileinfo}

			if !fileinfo.Mode().IsDir() && !fileinfo.Mode().IsRegular() {
				lstat, err := os.Lstat(path)
				if err != nil {
					cerr <- err
					return nil
				}

				lfileinfo := vfs.FileInfoFromStat(lstat)
				if lfileinfo.Mode()&os.ModeSymlink != 0 {
					originFile, err := os.Readlink(lfileinfo.Name())
					if err != nil {
						cerr <- err
						return nil
					}
					_ = originFile

					c <- importer.ImporterRecord{Pathname: filepath.ToSlash(path), Stat: lfileinfo}

					// need to figure out how to notidy fakefs
					// that a pathname actually link to another
				}
			}
			return nil
		})
		if err != nil {
			cerr <- err
		}
		close(cerr)
		close(c)
	}()
	return c, cerr, nil
}

func (p *FSImporter) NewReader(pathname string) (io.ReadCloser, error) {
	return os.Open(pathname)
}

func (p *FSImporter) Close() error {
	return nil
}
