package snapshot

import (
	"path/filepath"

	"github.com/poolpOrg/plakar/filesystem"
)

func (snapshot *Snapshot) LookupPathChildren(pathname string) (map[string]*filesystem.Fileinfo, bool) {
	pathname = filepath.Clean(pathname)

	parent, err := snapshot.Index.Filesystem.Lookup(pathname)
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
	return snapshot.Index.Filesystem.LookupInode(pathname)
}

func (snapshot *Snapshot) LookupInodeForFilename(pathname string) (*filesystem.Fileinfo, bool) {
	return snapshot.Index.Filesystem.LookupInodeForFile(pathname)
}

func (snapshot *Snapshot) LookupInodeForDirectory(pathname string) (*filesystem.Fileinfo, bool) {
	return snapshot.Index.Filesystem.LookupInodeForDirectory(pathname)
}

func (snapshot *Snapshot) LookupObjectForPathname(pathname string) *Object {
	snapshot.Index.muPathnames.Lock()
	defer snapshot.Index.muPathnames.Unlock()

	objectChecksum, exists := snapshot.Index.Pathnames[filepath.Clean(pathname)]
	if !exists {
		return nil
	}

	return snapshot.LookupObjectForChecksum(objectChecksum)
}

func (snapshot *Snapshot) LookupObjectForChecksum(checksum string) *Object {
	snapshot.Index.muObjects.Lock()
	defer snapshot.Index.muObjects.Unlock()

	object, exists := snapshot.Index.Objects[checksum]
	if !exists {
		return nil
	}

	return object
}

func (snapshot *Snapshot) LookupChunkForChecksum(checksum string) *Chunk {
	snapshot.Index.muChunks.Lock()
	defer snapshot.Index.muChunks.Unlock()

	chunk, exists := snapshot.GetChunkInfo(checksum)
	if !exists {
		return nil
	}

	return chunk
}
