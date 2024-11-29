package vfs

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository"
)

const VERSION = 001

type FSEntry interface {
	fsEntry()
	Size() int64
}

type ExtendedAttribute struct {
	Name  string `msgpack:"name"`
	Value []byte `msgpack:"value"`
}

type CustomMetadata struct {
	Key   string `msgpack:"key"`
	Value []byte `msgpack:"value"`
}

type AlternateDataStream struct {
	Name    string `msgpack:"name"`
	Content []byte `msgpack:"content"`
}

type Filesystem struct {
	repo      *repository.Repository
	root      [32]byte
	rootEntry *DirEntry
}

func NewFilesystem(repo *repository.Repository, root [32]byte) (*Filesystem, error) {
	rd, _, err := repo.GetBlob(packfile.TYPE_DIRECTORY, root)
	if err != nil {
		return nil, err
	}

	blob, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	dirEntry, err := DirEntryFromBytes(blob)
	if err != nil {
		return nil, err
	}

	return &Filesystem{
		repo:      repo,
		root:      root,
		rootEntry: dirEntry,
	}, nil
}

func (fsc *Filesystem) directoriesRecursive(checksum [32]byte, out chan string) {
	currentEntry := fsc.rootEntry
	baseDir := "/"
	if fsc.root != checksum {
		rd, _, err := fsc.repo.GetBlob(packfile.TYPE_DIRECTORY, checksum)
		if err != nil {
			fmt.Println("packfile blob not found for directory")
			return
		}

		blob, err := io.ReadAll(rd)
		if err != nil {
			fmt.Println("could not read packfile blob for directory")
			return
		}

		currentEntry, err = DirEntryFromBytes(blob)
		if err != nil {
			fmt.Println("error decoding directory entry")
			return
		}
		baseDir = filepath.Join("/", currentEntry.ParentPath, currentEntry.Stat().Name())
	}

	children, err := fsc.ChildrenIter(currentEntry)
	if err != nil {
		fmt.Println("error getting children iterator")
		return
	}
	for child := range children {
		if exists := fsc.repo.BlobExists(packfile.TYPE_DIRECTORY, child.Checksum()); !exists {
			continue
		}
		out <- filepath.Join(baseDir, child.Stat().Name())
		fsc.directoriesRecursive(child.Checksum(), out)
	}
}

func (fsc *Filesystem) Directories() <-chan string {
	ch := make(chan string)
	go func() {
		fsc.directoriesRecursive(fsc.root, ch)
		close(ch)
	}()
	return ch
}

func (fsc *Filesystem) filesRecursive(checksum [32]byte, out chan string) {
	currentEntry := fsc.rootEntry
	baseDir := "/"
	if fsc.root != checksum {
		rd, _, err := fsc.repo.GetBlob(packfile.TYPE_DIRECTORY, checksum)
		if err != nil {
			return
		}

		blob, err := io.ReadAll(rd)
		if err != nil {
			return
		}

		currentEntry, err = DirEntryFromBytes(blob)
		if err != nil {
			return
		}
		baseDir = filepath.Join(currentEntry.ParentPath, currentEntry.Stat().Name())
	}

	children, err := fsc.ChildrenIter(currentEntry)
	if err != nil {
		fmt.Println("error getting children iterator")
		return
	}
	for child := range children {
		if exists := fsc.repo.BlobExists(packfile.TYPE_FILE, child.Checksum()); !exists {
			if exists := fsc.repo.BlobExists(packfile.TYPE_DIRECTORY, child.Checksum()); !exists {
				return
			}
			fsc.filesRecursive(child.Checksum(), out)
		} else {
			out <- filepath.Join(baseDir, child.Stat().Name())
		}
	}
}

func (fsc *Filesystem) Files() <-chan string {
	ch := make(chan string)
	go func() {
		fsc.filesRecursive(fsc.root, ch)
		close(ch)
	}()
	return ch
}

func (fsc *Filesystem) pathnamesRecursive(checksum [32]byte, out chan string) {
	currentEntry := fsc.rootEntry
	baseDir := "/"
	if fsc.root != checksum {
		rd, _, err := fsc.repo.GetBlob(packfile.TYPE_DIRECTORY, checksum)
		if err != nil {
			return
		}

		blob, err := io.ReadAll(rd)
		if err != nil {
			return
		}

		currentEntry, err = DirEntryFromBytes(blob)
		if err != nil {
			return
		}
	}
	baseDir = filepath.Join("/", currentEntry.ParentPath, currentEntry.Stat().Name())
	out <- baseDir

	children, err := fsc.ChildrenIter(currentEntry)
	if err != nil {
		fmt.Println("error getting children iterator")
		return
	}
	for child := range children {
		if exists := fsc.repo.BlobExists(packfile.TYPE_FILE, child.Checksum()); !exists {
			if exists := fsc.repo.BlobExists(packfile.TYPE_DIRECTORY, child.Checksum()); !exists {
				return
			}
			fsc.pathnamesRecursive(child.Checksum(), out)
		} else {
			out <- filepath.Join(baseDir, child.Stat().Name())
		}
	}
}

func (fsc *Filesystem) Pathnames() <-chan string {
	ch := make(chan string)
	go func() {
		fsc.pathnamesRecursive(fsc.root, ch)
		close(ch)
	}()
	return ch
}

// Helper function to recursively traverse directories and find the path
func (fsc *Filesystem) statRecursive(checksum [32]byte, components []string) (FSEntry, error) {
	if checksum == fsc.root {
		if len(components) == 0 {
			return fsc.rootEntry, nil
		}
	}

	// Check if checksum refers to a file
	if fsc.repo.BlobExists(packfile.TYPE_FILE, checksum) {
		// Retrieve the file metadata
		rd, _, err := fsc.repo.GetBlob(packfile.TYPE_FILE, checksum)
		if err != nil {
			return nil, err
		}

		blob, err := io.ReadAll(rd)
		if err != nil {
			return nil, err
		}

		// Unmarshal the file entry
		fileEntry, err := FileEntryFromBytes(blob)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling file entry: %v", err)
		}

		// If this is the last component, return the file
		if len(components) == 0 {
			return fileEntry, nil
		}

		// If there are still components left, this is an error (files cannot have children)
		return nil, fmt.Errorf("invalid path: %s is a file but more components remain", components[0])
	}

	// Check if checksum refers to a directory
	if fsc.repo.BlobExists(packfile.TYPE_DIRECTORY, checksum) {
		// Retrieve the directory metadata
		rd, _, err := fsc.repo.GetBlob(packfile.TYPE_DIRECTORY, checksum)
		if err != nil {
			return nil, err
		}

		blob, err := io.ReadAll(rd)
		if err != nil {
			return nil, err
		}

		// Unmarshal the directory entry
		dirEntry, err := DirEntryFromBytes(blob)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling directory entry: %v", err)
		}

		// If there are no more components, return the directory
		if len(components) == 0 {
			return dirEntry, nil
		}

		// Look for the next component (file or directory) in the children of the directory
		children, err := fsc.ChildrenIter(dirEntry)
		if err != nil {
			fmt.Println("error getting children iterator")
			return nil, err
		}
		for child := range children {
			if child.Stat().Name() == components[0] {
				// Recursively continue with the child checksum
				return fsc.statRecursive(child.Checksum(), components[1:])
			}
		}

		// If no matching child was found, return an error
		return nil, fmt.Errorf("path not found: %s", components[0])
	}

	// If neither a file nor a directory, return an error
	return nil, fmt.Errorf("path not found or invalid: checksum does not correspond to a file or directory")
}

func (fsc *Filesystem) Stat(path string) (FSEntry, error) {
	// Ensure the path starts with a slash for consistency
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if path == "/" {
		return fsc.rootEntry, nil
	}

	path = filepath.Clean(path)

	// Split the path into components for recursive lookup
	components := strings.Split(path, "/")
	if len(components) == 0 {
		return nil, fmt.Errorf("invalid path: %s", path)
	}

	// Start the recursive lookup from the root
	return fsc.statRecursive(fsc.root, components[1:]) // Skip the initial empty component due to leading '/'
}

func (fsc *Filesystem) Children(path string) (<-chan string, error) {
	fsEntry, err := fsc.Stat(path)
	if err != nil {
		return nil, err
	}
	if fsEntry.(*DirEntry) == nil {
		return nil, fmt.Errorf("path is not a directory")
	}

	ch := make(chan string)
	go func() {
		defer close(ch)

		children, err := fsc.ChildrenIter(fsEntry.(*DirEntry))
		if err != nil {
			fmt.Println("error getting children iterator")
			return
		}
		for child := range children {
			ch <- child.Stat().Name()
		}
	}()
	return ch, nil
}

func (fsc *Filesystem) fileChecksumsRecursive(checksum [32]byte, out chan [32]byte) {
	currentEntry := fsc.rootEntry
	if fsc.root != checksum {
		rd, _, err := fsc.repo.GetBlob(packfile.TYPE_DIRECTORY, checksum)
		if err != nil {
			return
		}

		blob, err := io.ReadAll(rd)
		if err != nil {
			return
		}

		currentEntry, err = DirEntryFromBytes(blob)
		if err != nil {
			return
		}
	}

	children, err := fsc.ChildrenIter(currentEntry)
	if err != nil {
		fmt.Println("error getting children iterator")
		return
	}
	for child := range children {
		if exists := fsc.repo.BlobExists(packfile.TYPE_FILE, child.Checksum()); !exists {
			if exists := fsc.repo.BlobExists(packfile.TYPE_DIRECTORY, child.Checksum()); !exists {
				return
			}
			fsc.fileChecksumsRecursive(child.Checksum(), out)
		} else {
			out <- child.Checksum()
		}
	}
}

func (fsc *Filesystem) FileChecksums() <-chan [32]byte {
	ch := make(chan [32]byte)
	go func() {
		fsc.fileChecksumsRecursive(fsc.root, ch)
		close(ch)
	}()
	return ch
}

func (fsc *Filesystem) directoryChecksumsRecursive(checksum [32]byte, out chan [32]byte) {
	currentEntry := fsc.rootEntry
	if fsc.root != checksum {
		rd, _, err := fsc.repo.GetBlob(packfile.TYPE_DIRECTORY, checksum)
		if err != nil {
			fmt.Println("packfile blob not found for directory")
			return
		}

		blob, err := io.ReadAll(rd)
		if err != nil {
			fmt.Println("could not read packfile blob for directory")
			return
		}

		currentEntry, err = DirEntryFromBytes(blob)
		if err != nil {
			fmt.Println("error decoding directory entry")
			return
		}
	}

	children, err := fsc.ChildrenIter(currentEntry)
	if err != nil {
		fmt.Println("error getting children iterator")
		return
	}
	for child := range children {
		if exists := fsc.repo.BlobExists(packfile.TYPE_DIRECTORY, child.Checksum()); !exists {
			continue
		}
		out <- child.Checksum()
		fsc.directoryChecksumsRecursive(child.Checksum(), out)
	}
}
func (fsc *Filesystem) DirectoryChecksums() <-chan [32]byte {
	ch := make(chan [32]byte)
	go func() {
		fsc.directoryChecksumsRecursive(fsc.root, ch)
		close(ch)
	}()
	return ch
}

func (fsc *Filesystem) ChildrenIter(dir *DirEntry) (<-chan *ChildEntry, error) {
	c := make(chan *ChildEntry)

	go func() {
		defer close(c)

		iter := dir.Children.Head
		for iter != nil {

			rd, _, err := fsc.repo.GetBlob(packfile.TYPE_CHILD, *iter)
			if err != nil {
				return
			}

			childBytes, err := io.ReadAll(rd)
			if err != nil {
				return
			}

			child, err := ChildEntryFromBytes(childBytes)
			if err != nil {
				return
			}

			c <- child

			rd, _, err = fsc.repo.GetBlob(packfile.TYPE_LIST, *iter)
			if err != nil {
				return
			}

			leBytes, err := io.ReadAll(rd)
			if err != nil {
				return
			}

			le, err := objects.ListEntryFromBytes(leBytes)
			if err != nil {
				return
			}

			iter = le.Successor
		}
	}()

	return c, nil
}

func (fsc *Filesystem) ErrorIter(dir *DirEntry) (<-chan *ErrorEntry, error) {
	c := make(chan *ErrorEntry)

	go func() {
		defer close(c)

		iter := dir.Children.Head
		for iter != nil {

			rd, _, err := fsc.repo.GetBlob(packfile.TYPE_ERROR, *iter)
			if err != nil {
				return
			}

			errorBytes, err := io.ReadAll(rd)
			if err != nil {
				return
			}

			errEntry, err := ErrorEntryFromBytes(errorBytes)
			if err != nil {
				return
			}

			c <- errEntry

			rd, _, err = fsc.repo.GetBlob(packfile.TYPE_LIST, *iter)
			if err != nil {
				return
			}

			leBytes, err := io.ReadAll(rd)
			if err != nil {
				return
			}

			le, err := objects.ListEntryFromBytes(leBytes)
			if err != nil {
				return
			}

			iter = le.Successor
		}
	}()

	return c, nil
}
