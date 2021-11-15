package snapshot

import (
	"path/filepath"
	"strings"
)

func (snapshot *Snapshot) SetInode(pathname string, fileinfo *Fileinfo) {

	p := snapshot.Tree
	if pathname == "/" {
		p.Inode = fileinfo
		return
	}

	atoms := strings.Split(pathname, "/")[1:]
	for _, atom := range atoms {
		p.muNode.Lock()
		tmp, exists := p.Children[atom]
		p.muNode.Unlock()

		if !exists {
			p.muNode.Lock()
			p.Children[atom] = &TreeNode{Children: make(map[string]*TreeNode)}
			tmp = p.Children[atom]
			p.muNode.Unlock()
		}
		p = tmp
	}
	p.muNode.Lock()
	p.Inode = fileinfo
	p.muNode.Unlock()

	if p.Inode.Mode.IsDir() {
		snapshot.Filesystem.muDirectories.Lock()
		snapshot.Filesystem.Directories[pathname] = fileinfo
		snapshot.Filesystem.muDirectories.Unlock()
	} else if p.Inode.Mode.IsRegular() {
		snapshot.Filesystem.muFiles.Lock()
		snapshot.Filesystem.Files[pathname] = fileinfo
		snapshot.Filesystem.muFiles.Unlock()
	} else {
		snapshot.Filesystem.muNonRegular.Lock()
		snapshot.Filesystem.NonRegular[pathname] = fileinfo
		snapshot.Filesystem.muNonRegular.Unlock()
	}
}

func (snapshot *Snapshot) stateGetTree(pathname string) (*Fileinfo, bool) {
	p := snapshot.Tree
	if pathname == "/" {
		return p.Inode, true
	}

	atoms := strings.Split(pathname, "/")[1:]
	for _, atom := range atoms {
		p.muNode.Lock()
		tmp, exists := p.Children[atom]
		p.muNode.Unlock()
		if !exists {
			return nil, false
		}
		p = tmp
	}
	return p.Inode, true
}

func (snapshot *Snapshot) GetPathChildren(pathname string) (map[string]*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	ret := make(map[string]*Fileinfo)

	p := snapshot.Tree
	if pathname == "/" {
		p.muNode.Lock()
		for child, node := range p.Children {
			ret[child] = node.Inode
		}
		p.muNode.Unlock()
		return ret, true
	}

	atoms := strings.Split(pathname, "/")[1:]
	for _, atom := range atoms {
		p.muNode.Lock()
		tmp, exists := p.Children[atom]
		p.muNode.Unlock()
		if !exists {
			return nil, false
		}
		p = tmp
	}
	p.muNode.Lock()
	for child, node := range p.Children {
		ret[child] = node.Inode
	}
	p.muNode.Unlock()
	return ret, true
}

func (snapshot *Snapshot) GetInode(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	return snapshot.stateGetTree(pathname)
}

func (snapshot *Snapshot) GetFileInode(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	snapshot.Filesystem.muFiles.Lock()
	info, exists := snapshot.Filesystem.Files[pathname]
	snapshot.Filesystem.muFiles.Unlock()
	return info, exists
}

func (snapshot *Snapshot) GetDirectoryInode(pathname string) (*Fileinfo, bool) {
	pathname = filepath.Clean(pathname)
	snapshot.Filesystem.muDirectories.Lock()
	info, exists := snapshot.Filesystem.Directories[pathname]
	snapshot.Filesystem.muDirectories.Unlock()
	return info, exists
}

func (snapshot *Snapshot) GetObjectFromPathname(pathname string) *Object {
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
