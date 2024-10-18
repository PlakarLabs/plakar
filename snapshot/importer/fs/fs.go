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
	"github.com/pkg/xattr"
)

type FSImporter struct {
	importer.ImporterBackend
	rootDir string
}

func init() {
	importer.Register("fs", NewFSImporter)
}

func getExtendedAttributes(path string) (map[string][]byte, error) {
	attrs := make(map[string][]byte)

	// Get the list of attribute names
	attributes, err := xattr.List(path)
	if err != nil {
		return nil, fmt.Errorf("failed to list extended attributes for %s: %w", path, err)
	}

	// Iterate over each attribute and retrieve its value
	for _, attr := range attributes {
		value, err := xattr.Get(path, attr)
		if err != nil {
			// Log the error and continue instead of failing
			if os.IsPermission(err) {
				fmt.Printf("permission denied for attribute %s on %s\n", attr, path)
				continue
			}
			return nil, fmt.Errorf("failed to get value for attribute %s: %w", attr, err)
		}
		attrs[attr] = value
	}

	return attrs, nil
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

			var recordType importer.RecordType
			switch mode := fileinfo.Mode(); {
			case mode.IsRegular():
				recordType = importer.RecordTypeFile
			case mode.IsDir():
				recordType = importer.RecordTypeDirectory
			case mode&os.ModeSymlink != 0:
				recordType = importer.RecordTypeSymlink
			case mode&os.ModeDevice != 0:
				recordType = importer.RecordTypeDevice
			case mode&os.ModeNamedPipe != 0:
				recordType = importer.RecordTypePipe
			case mode&os.ModeSocket != 0:
				recordType = importer.RecordTypeSocket
			default:
				recordType = importer.RecordTypeFile // Default to file if type is unknown
			}
			c <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path), Stat: fileinfo}
		}

		filepath.WalkDir(directory, func(path string, di fs.DirEntry, err error) error {
			if err != nil {
				c <- importer.ScanError{Pathname: path, Err: err}
				return nil
			}

			path, err = filepath.Abs(path)
			if err != nil {
				c <- importer.ScanError{Pathname: path, Err: err}
				return nil
			}

			info, err := di.Info()
			if err != nil {
				c <- importer.ScanError{Pathname: path, Err: err}
				return nil
			}

			var recordType importer.RecordType
			switch mode := info.Mode(); {
			case mode.IsRegular():
				recordType = importer.RecordTypeFile
			case mode.IsDir():
				recordType = importer.RecordTypeDirectory
			case mode&os.ModeSymlink != 0:
				recordType = importer.RecordTypeSymlink
			case mode&os.ModeDevice != 0:
				recordType = importer.RecordTypeDevice
			case mode&os.ModeNamedPipe != 0:
				recordType = importer.RecordTypePipe
			case mode&os.ModeSocket != 0:
				recordType = importer.RecordTypeSocket
			default:
				recordType = importer.RecordTypeFile // Default to file if type is unknown
			}

			extendedAttributes, err := getExtendedAttributes(path)
			if err != nil {
				c <- importer.ScanError{Pathname: path, Err: err}
			}

			fileinfo := objects.FileInfoFromStat(info)
			c <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path), Stat: fileinfo, ExtendedAttributes: extendedAttributes}

			if !fileinfo.Mode().IsDir() && !fileinfo.Mode().IsRegular() {
				lstat, err := os.Lstat(path)
				if err != nil {
					c <- importer.ScanError{Pathname: path, Err: err}
					return nil
				}

				lfileinfo := objects.FileInfoFromStat(lstat)
				if lfileinfo.Mode()&os.ModeSymlink != 0 {
					originFile, err := os.Readlink(path)
					if err != nil {
						c <- importer.ScanError{Pathname: path, Err: err}
						return nil
					}
					c <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path), Target: originFile, Stat: lfileinfo, ExtendedAttributes: extendedAttributes}
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
