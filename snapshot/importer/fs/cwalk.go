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
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/iafan/cwalk"
)

type ScanEntry struct {
	Pathname string
	Info     fs.FileInfo
}

func cwalk_worker(rootDir string, jobs <-chan ScanEntry, results chan<- importer.ScanResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for path := range jobs {
		info := path.Info

		// Use fs.DirEntry.Type() to avoid another stat call when possible
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

		// Get extended attributes (if applicable)
		extendedAttributes, err := getExtendedAttributes(path.Pathname)
		if err != nil {
			results <- importer.ScanError{Pathname: path.Pathname, Err: err}
			continue
		}

		fileinfo := objects.FileInfoFromStat(info)

		var username string
		var groupname string

		u, err := user.LookupId(fmt.Sprintf("%d", fileinfo.Uid()))
		if err == nil {
			username = u.Username
		}

		g, err := user.LookupGroupId(fmt.Sprintf("%d", fileinfo.Gid()))
		if err == nil {
			groupname = g.Name
		}

		if fileinfo.Mode().IsDir() {
			entries, err := os.ReadDir(path.Pathname)
			if err != nil {
				results <- importer.ScanError{Pathname: path.Pathname, Err: err}
				continue
			}
			var children []objects.FileInfo
			prefix := rootDir
			if rootDir != "/" {
				prefix = prefix + "/"
			}
			for _, child := range entries {
				info, err := child.Info()
				if err != nil {
					results <- importer.ScanError{Pathname: path.Pathname, Err: err}
					continue
				}

				fullpath := filepath.Join(path.Pathname, child.Name())
				if !info.IsDir() {
					if !strings.HasPrefix(fullpath, prefix) {
						continue
					}
				} else {
					if len(fullpath) < len(prefix) {
						if !strings.HasPrefix(prefix, fullpath) {
							continue
						}
					} else {
						if !strings.HasPrefix(fullpath, prefix) {
							continue
						}
					}
				}
				children = append(children, objects.FileInfoFromStat(info))
			}
			results <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path.Pathname), User: username, Group: groupname, Stat: fileinfo, ExtendedAttributes: extendedAttributes, Children: children}
		} else {
			results <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path.Pathname), User: username, Group: groupname, Stat: fileinfo, ExtendedAttributes: extendedAttributes}
		}

		// Handle symlinks separately
		if fileinfo.Mode()&os.ModeSymlink != 0 {
			originFile, err := os.Readlink(path.Pathname)
			if err != nil {
				results <- importer.ScanError{Pathname: path.Pathname, Err: err}
				continue
			}
			results <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path.Pathname), Target: originFile, Stat: fileinfo, ExtendedAttributes: extendedAttributes}
		}
	}
}

// Add prefix directories for the root path (leading up to the full rootDir path)
func cwalk_addPrefixDirectories(rootDir string, jobs chan<- ScanEntry, results chan<- importer.ScanResult) {
	// Clean the directory and split the path into components
	directory := filepath.Clean(rootDir)
	atoms := strings.Split(directory, string(os.PathSeparator))

	for i := 0; i < len(atoms)-1; i++ {
		path := filepath.Join(atoms[0 : i+1]...)

		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		// Check if the directory exists
		if info, err := os.Stat(path); err != nil {
			results <- importer.ScanError{Pathname: path, Err: err}
			continue
		} else {

			// Send the directory to the jobs channel for processing
			jobs <- ScanEntry{Pathname: path, Info: info}
		}
	}
}

// Scan directory with worker pool using cwalk
func cwalk_walker(rootDir string, numWorkers int) (<-chan importer.ScanResult, error) {
	results := make(chan importer.ScanResult, 1000) // Larger buffer for results
	jobs := make(chan ScanEntry, 1000)              // Buffered channel to feed paths to workers
	var wg sync.WaitGroup

	// Launch worker pool
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go cwalk_worker(rootDir, jobs, results, &wg)
	}

	// Start walking the directory and sending file paths to workers using cwalk
	go func() {
		defer close(jobs)

		// Add prefix directories first
		cwalk_addPrefixDirectories(rootDir, jobs, results)

		err := cwalk.Walk(rootDir, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				results <- importer.ScanError{Pathname: path, Err: err}
				return nil
			}

			path = filepath.Join(rootDir, path)

			// Send each path to the job queue
			jobs <- ScanEntry{Pathname: path, Info: info}
			return nil
		})

		if err != nil {
			results <- importer.ScanError{Pathname: rootDir, Err: err}
		}
	}()

	// Close the results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	return results, nil
}
