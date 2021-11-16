package snapshot

import (
	"path/filepath"

	"github.com/poolpOrg/plakar/filesystem"
)

func (snapshot *Snapshot) LookupPathChildren(pathname string) (map[string]*filesystem.Fileinfo, bool) {
	pathname = filepath.Clean(pathname)

	parent, err := snapshot.Filesystem.Lookup(pathname)
	if err != nil {
		return nil, false
	}

	ret := make(map[string]*filesystem.Fileinfo)
	for child, node := range parent.Children {
		ret[child] = node.Inode
	}
	return ret, true
}

func (snapshot *Snapshot) LookupInodeForPathname(pathname string) (*filesystem.Fileinfo, bool) {
	return snapshot.Filesystem.LookupInode(pathname)
}

func (snapshot *Snapshot) LookupInodeForFilename(pathname string) (*filesystem.Fileinfo, bool) {
	return snapshot.Filesystem.LookupInodeForFile(pathname)
}

func (snapshot *Snapshot) LookupInodeForDirectory(pathname string) (*filesystem.Fileinfo, bool) {
	return snapshot.Filesystem.LookupInodeForDirectory(pathname)
}

func (snapshot *Snapshot) LookupObjectForPathname(pathname string) *Object {
	snapshot.muFilenames.Lock()
	defer snapshot.muFilenames.Unlock()

	objectChecksum, exists := snapshot.Filenames[filepath.Clean(pathname)]
	if !exists {
		return nil
	}

	return snapshot.LookupObjectForChecksum(objectChecksum)
}

func (snapshot *Snapshot) LookupObjectForChecksum(checksum string) *Object {
	snapshot.muObjects.Lock()
	defer snapshot.muObjects.Unlock()

	object, exists := snapshot.Objects[checksum]
	if !exists {
		return nil
	}

	return object
}

func (snapshot *Snapshot) LookupChunkForChecksum(checksum string) *Chunk {
	snapshot.muChunks.Lock()
	defer snapshot.muChunks.Unlock()

	chunk, exists := snapshot.Chunks[checksum]
	if !exists {
		return nil
	}

	return chunk
}
