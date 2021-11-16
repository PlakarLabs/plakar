package snapshot

import (
	"path/filepath"
)

func (snapshot *Snapshot) LookupPathChildren(pathname string) (map[string]*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)

	parent, err := snapshot.Filesystem.Lookup(pathname)
	if err != nil {
		return nil, false
	}

	ret := make(map[string]*Fileinfo)
	for child, node := range parent.Children {
		ret[child] = node.Inode
	}
	return ret, true
}

func (snapshot *Snapshot) LookupInodeFromPathname(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)

	snapshot.Filesystem.muInodes.Lock()
	fileinfo, exists := snapshot.Filesystem.Inodes[pathname]
	snapshot.Filesystem.muInodes.Unlock()
	return fileinfo, exists
}

func (snapshot *Snapshot) LookupObjectFromPathname(pathname string) *Object {
	snapshot.muFilenames.Lock()
	defer snapshot.muFilenames.Unlock()

	objectChecksum, exists := snapshot.Filenames[filepath.Clean(pathname)]
	if !exists {
		return nil
	}

	object, exists := snapshot.Objects[objectChecksum]
	if !exists {
		return nil
	}

	return object
}
