package snapshot

import (
	"path/filepath"
	"strings"
)

func (snapshot *Snapshot) SetInode(pathname string, fileinfo *FileInfo) {

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
		snapshot.muDirectories.Lock()
		snapshot.Directories[pathname] = fileinfo
		snapshot.muDirectories.Unlock()
	} else if p.Inode.Mode.IsRegular() {
		snapshot.muFiles.Lock()
		snapshot.Files[pathname] = fileinfo
		snapshot.muFiles.Unlock()
	} else {
		snapshot.muNonRegular.Lock()
		snapshot.NonRegular[pathname] = fileinfo
		snapshot.muNonRegular.Unlock()
	}
}

func (snapshot *Snapshot) stateGetTree(pathname string) (*FileInfo, bool) {
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

func (snapshot *Snapshot) GetPathChildren(pathname string) (map[string]*FileInfo, bool) {
	pathname = filepath.Clean(pathname)
	ret := make(map[string]*FileInfo)

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

func (snapshot *Snapshot) GetInode(pathname string) (*FileInfo, bool) {
	pathname = filepath.Clean(pathname)
	return snapshot.stateGetTree(pathname)
}

func (snapshot *Snapshot) GetFileInode(pathname string) (*FileInfo, bool) {
	pathname = filepath.Clean(pathname)
	snapshot.muFiles.Lock()
	info, exists := snapshot.Files[pathname]
	snapshot.muFiles.Unlock()
	return info, exists
}

func (snapshot *Snapshot) GetDirectoryInode(pathname string) (*FileInfo, bool) {
	pathname = filepath.Clean(pathname)
	snapshot.muDirectories.Lock()
	info, exists := snapshot.Directories[pathname]
	snapshot.muDirectories.Unlock()
	return info, exists
}

func (snapshot *Snapshot) GetObjectFromPathname(pathname string) *Object {
	snapshot.muPathnames.Lock()
	defer snapshot.muPathnames.Unlock()

	objectChecksum, exists := snapshot.Pathnames[filepath.Clean(pathname)]
	if !exists {
		return nil
	}

	object, exists := snapshot.Objects[objectChecksum]
	if !exists {
		return nil
	}

	return object
}
