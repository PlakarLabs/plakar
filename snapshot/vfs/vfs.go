package vfs

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/PlakarLabs/plakar/repository"
)

type Filesystem struct {
	repo      *repository.Repository
	root      [32]byte
	rootEntry *DirEntry
}

func NewFilesystem(repo *repository.Repository, root [32]byte) (*Filesystem, error) {
	packfile, offset, length, exists := repo.State().GetSubpartForDirectory(root)
	if !exists {
		return nil, fmt.Errorf("directory not found")
	}

	blob, err := repo.GetPackfileBlob(packfile, offset, length)
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
		packfile, offset, length, exists := fsc.repo.State().GetSubpartForDirectory(checksum)
		if !exists {
			fmt.Println("packfile not found for directory")
			return
		}

		blob, err := fsc.repo.GetPackfileBlob(packfile, offset, length)
		if err != nil {
			fmt.Println("packfile blob not found for directory")
			return
		}

		currentEntry, err = DirEntryFromBytes(blob)
		if err != nil {
			fmt.Println("error decoding directory entry")
			return
		}
		baseDir = filepath.Join("/", currentEntry.ParentPath, currentEntry.Name)
	}

	for _, child := range currentEntry.Children {
		if exists := fsc.repo.State().DirectoryExists(child.Checksum); !exists {
			continue
		}
		out <- filepath.Join(baseDir, child.FileInfo.Name())
		fsc.directoriesRecursive(child.Checksum, out)
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
		packfile, offset, length, exists := fsc.repo.State().GetSubpartForDirectory(checksum)
		if !exists {
			return
		}
		blob, err := fsc.repo.GetPackfileBlob(packfile, offset, length)
		if err != nil {
			return
		}

		currentEntry, err = DirEntryFromBytes(blob)
		if err != nil {
			return
		}
		baseDir = filepath.Join(currentEntry.ParentPath, currentEntry.Name)
	}

	for _, child := range currentEntry.Children {
		if exists := fsc.repo.State().FileExists(child.Checksum); !exists {
			if exists := fsc.repo.State().DirectoryExists(child.Checksum); !exists {
				return
			}
			fsc.filesRecursive(child.Checksum, out)
		} else {
			out <- filepath.Join(baseDir, child.FileInfo.Name())
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
		packfile, offset, length, exists := fsc.repo.State().GetSubpartForDirectory(checksum)
		if !exists {
			return
		}
		blob, err := fsc.repo.GetPackfileBlob(packfile, offset, length)
		if err != nil {
			return
		}

		currentEntry, err = DirEntryFromBytes(blob)
		if err != nil {
			return
		}
	}
	baseDir = filepath.Join("/", currentEntry.ParentPath, currentEntry.Name)
	out <- baseDir

	for _, child := range currentEntry.Children {
		if exists := fsc.repo.State().FileExists(child.Checksum); !exists {
			if exists := fsc.repo.State().DirectoryExists(child.Checksum); !exists {
				return
			}
			fsc.pathnamesRecursive(child.Checksum, out)
		} else {
			out <- filepath.Join(baseDir, child.FileInfo.Name())
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
	if fsc.repo.State().FileExists(checksum) {
		// Retrieve the file metadata
		packfile, offset, length, exists := fsc.repo.State().GetSubpartForFile(checksum)
		if !exists {
			return nil, fmt.Errorf("file not found")
		}

		blob, err := fsc.repo.GetPackfileBlob(packfile, offset, length)
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
	if fsc.repo.State().DirectoryExists(checksum) {
		// Retrieve the directory metadata
		packfile, offset, length, exists := fsc.repo.State().GetSubpartForDirectory(checksum)
		if !exists {
			return nil, fmt.Errorf("directory not found")
		}

		blob, err := fsc.repo.GetPackfileBlob(packfile, offset, length)
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
		for _, child := range dirEntry.Children {
			if child.FileInfo.Name() == components[0] {
				// Recursively continue with the child checksum
				return fsc.statRecursive(child.Checksum, components[1:])
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
		for _, child := range fsEntry.(*DirEntry).Children {
			ch <- child.FileInfo.Name()
		}
	}()
	return ch, nil
}
