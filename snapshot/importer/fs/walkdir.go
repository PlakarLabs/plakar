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
)

// Worker pool to handle file scanning in parallel
func walkDir_worker(rootDir string, jobs <-chan string, results chan<- importer.ScanResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for path := range jobs {
		info, err := os.Lstat(path) // Use Lstat to handle symlinks properly
		if err != nil {
			results <- importer.ScanError{Pathname: path, Err: err}
			continue
		}

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
		extendedAttributes, err := getExtendedAttributes(path)
		if err != nil {
			results <- importer.ScanError{Pathname: path, Err: err}
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
			entries, err := os.ReadDir(path)
			if err != nil {
				results <- importer.ScanError{Pathname: path, Err: err}
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
					results <- importer.ScanError{Pathname: path, Err: err}
					continue
				}

				fullpath := filepath.Join(path, child.Name())
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
			results <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path), User: username, Group: groupname, FileInfo: fileinfo, ExtendedAttributes: extendedAttributes, Children: children}
		} else {
			results <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path), User: username, Group: groupname, FileInfo: fileinfo, ExtendedAttributes: extendedAttributes}
		}

		// Handle symlinks separately
		if fileinfo.Mode()&os.ModeSymlink != 0 {
			originFile, err := os.Readlink(path)
			if err != nil {
				results <- importer.ScanError{Pathname: path, Err: err}
				continue
			}
			results <- importer.ScanRecord{Type: recordType, Pathname: filepath.ToSlash(path), User: username, Group: groupname, Target: originFile, FileInfo: fileinfo, ExtendedAttributes: extendedAttributes}
		}
	}
}

func walkDir_addPrefixDirectories(rootDir string, jobs chan<- string, results chan<- importer.ScanResult) {
	// Clean the directory and split the path into components
	directory := filepath.Clean(rootDir)
	atoms := strings.Split(directory, string(os.PathSeparator))

	for i := 0; i < len(atoms)-1; i++ {
		path := filepath.Join(atoms[0 : i+1]...)

		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}

		// Check if the directory exists
		if _, err := os.Stat(path); err != nil {
			results <- importer.ScanError{Pathname: path, Err: err}
			continue
		}

		// Send the directory to the jobs channel for processing
		jobs <- path
	}
}

func walkDir_walker(rootDir string, numWorkers int) (<-chan importer.ScanResult, error) {
	results := make(chan importer.ScanResult, 1000) // Larger buffer for results
	jobs := make(chan string, 1000)                 // Buffered channel to feed paths to workers
	var wg sync.WaitGroup

	// Launch worker pool
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go walkDir_worker(rootDir, jobs, results, &wg)
	}

	// Start walking the directory and sending file paths to workers
	go func() {
		defer close(jobs)

		// Add prefix directories first
		walkDir_addPrefixDirectories(rootDir, jobs, results)

		err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				results <- importer.ScanError{Pathname: path, Err: err}
				return nil
			}

			// If d is a directory, send its path directly to the job queue
			jobs <- path
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
