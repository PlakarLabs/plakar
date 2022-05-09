package plakarfs

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

var inodeMutex sync.Mutex
var nextInode fuseops.InodeID = fuseops.RootInodeID

type inodeEntry struct {
	parentID   fuseops.InodeID
	inodeID    fuseops.InodeID
	name       string
	path       string
	snapshotID uuid.UUID
}

func allocateInodeID() fuseops.InodeID {
	inodeMutex.Lock()
	defer inodeMutex.Unlock()

	nextInode++

	return nextInode
}

type plakarFS struct {
	fuseutil.NotImplementedFileSystem

	repository *storage.Repository
	mountpoint string

	inodeEntries *sync.Map
	inode        *sync.Map

	metadataCache *sync.Map
	fsCache       *sync.Map
}

func NewPlakarFS(repository *storage.Repository, mountpoint string) (fuse.Server, error) {

	fs := &plakarFS{
		repository:   repository,
		mountpoint:   mountpoint,
		inodeEntries: &sync.Map{},
		inode:        &sync.Map{},

		metadataCache: &sync.Map{},
		fsCache:       &sync.Map{},
	}
	fs.setInodeEntry(&inodeEntry{
		name:     "/",
		parentID: fuseops.RootInodeID,
		inodeID:  fuseops.RootInodeID,
		path:     mountpoint,
	})

	return fuseutil.NewFileSystemServer(fs), nil
}

func (fs *plakarFS) setInodeEntry(entry *inodeEntry) {
	fs.inodeEntries.Store(entry.inodeID, entry)
	fs.inode.Store(entry.path, entry.inodeID)
}

func (fs *plakarFS) getInodeEntry(id fuseops.InodeID) (*inodeEntry, bool) {
	entry, exists := fs.inodeEntries.Load(id)
	if !exists {
		return nil, false
	}
	return entry.(*inodeEntry), true
}

func (fs *plakarFS) getInode(pathname string) (fuseops.InodeID, bool) {
	entry, exists := fs.inode.Load(pathname)
	if !exists {
		return fuseops.InodeID(0), false
	}
	return entry.(fuseops.InodeID), true
}

func (fs *plakarFS) getMetadata(snapshotID uuid.UUID) (*snapshot.Metadata, error) {
	entry, exists := fs.metadataCache.Load(snapshotID)
	if !exists {
		md, _, err := snapshot.GetMetadata(fs.repository, snapshotID)
		if err != nil {
			return md, err
		}
		fs.metadataCache.Store(snapshotID, md)
		return md, err
	}
	return entry.(*snapshot.Metadata), nil
}

func (fs *plakarFS) getFilesystem(snapshotID uuid.UUID) (*snapshot.Filesystem, error) {
	entry, exists := fs.fsCache.Load(snapshotID)
	if !exists {
		filesystem, _, err := snapshot.GetFilesystem(fs.repository, snapshotID)
		if err != nil {
			return filesystem, err
		}
		fs.fsCache.Store(snapshotID, filesystem)
		return filesystem, err
	}
	return entry.(*snapshot.Filesystem), nil
}

func (fs *plakarFS) getAttributes(id fuseops.InodeID) (fuseops.InodeAttributes, error) {

	if id == fuseops.RootInodeID {
		return fuseops.InodeAttributes{
			Nlink: 1,
			Mode:  0777 | os.ModeDir,
		}, nil
	}

	inode, exists := fs.getInodeEntry(id)
	if !exists {
		return fuseops.InodeAttributes{}, fuse.ENOENT
	}

	if inode.parentID == fuseops.RootInodeID {
		// snapshots are right below root,
		// they're a special case as there's no fileinfo for them.
		metadata, err := fs.getMetadata(uuid.MustParse(inode.name))
		if err != nil {
			return fuseops.InodeAttributes{}, fuse.EIO
		}
		return fuseops.InodeAttributes{
			Nlink: 1,
			Mode:  0700 | os.ModeDir,
			Ctime: metadata.CreationTime,
			Mtime: metadata.CreationTime,
			Size:  metadata.ScanProcessedSize,
			Uid:   uint32(os.Geteuid()),
			Gid:   uint32(os.Getgid()),
		}, nil
	}

	// from this point, use the snapshot filesystem view
	filesystem, err := fs.getFilesystem(inode.snapshotID)
	if err != nil {
		return fuseops.InodeAttributes{}, fuse.EIO
	}

	// the inode path has a snapshot-uuid:/path format, strip prefix
	fileinfo, exists := filesystem.LookupInode(inode.path[36+1:])
	if !exists {
		return fuseops.InodeAttributes{}, fuse.ENOENT
	}
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode:  fileinfo.Mode,
		Ctime: fileinfo.ModTime,
		Mtime: fileinfo.ModTime,
		Size:  uint64(fileinfo.Size),
		Uid:   uint32(os.Geteuid()),
		Gid:   uint32(os.Getgid()),
	}, nil
}

func (fs *plakarFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	return nil
}

func (fs *plakarFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {

	if op.Parent == fuseops.RootInodeID {
		pathname := fmt.Sprintf("%s/%s", fs.mountpoint, op.Name)
		inodeID, exists := fs.getInode(pathname)
		if !exists {
			return fuse.ENOENT
		}

		metadata, err := fs.getMetadata(uuid.MustParse(op.Name))
		if err != nil {
			return fuse.EIO
		}

		op.Entry.Child = inodeID
		op.Entry.Attributes = fuseops.InodeAttributes{
			Size:  metadata.ScanProcessedSize,
			Nlink: 1,
			Mode:  0700 | os.ModeDir,
			Ctime: metadata.CreationTime,
			Atime: metadata.CreationTime,
			Mtime: metadata.CreationTime,
		}
		return nil
	}

	inodeParent, exists := fs.getInodeEntry(op.Parent)
	if !exists {
		return fuse.ENOENT
	}

	var pathname string
	if inodeParent.parentID == fuseops.RootInodeID {
		pathname = fmt.Sprintf("%s:/%s", inodeParent.name, op.Name)

	} else {
		pathname = fmt.Sprintf("%s/%s", inodeParent.path, op.Name)
	}

	inodeID, exists := fs.getInode(pathname)
	if !exists {
		return fuse.ENOENT
	}

	inode, exists := fs.getInodeEntry(inodeID)
	if !exists {
		return fuse.ENOENT
	}

	var snapshotID uuid.UUID
	var lookupPath string
	if inodeParent.parentID == fuseops.RootInodeID {
		lookupPath = inode.path[len(inodeParent.name)+1:]
		snapshotID = uuid.MustParse(inodeParent.name)
	} else {
		lookupPath = inode.path[36+1:]
		snapshotID = inodeParent.snapshotID

	}

	filesystem, err := fs.getFilesystem(snapshotID)
	if err != nil {
		return fuse.EIO
	}

	stat, exists := filesystem.LookupInode(lookupPath)
	if !exists {
		return fuse.ENOENT
	}

	op.Entry.Child = inodeID
	op.Entry.Attributes = fuseops.InodeAttributes{
		Size:  uint64(stat.Size),
		Nlink: 1,
		Mode:  stat.Mode,
		Ctime: stat.ModTime,
		Atime: stat.ModTime,
		Mtime: stat.ModTime,
	}

	return nil
}

func (fs *plakarFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	var err error
	op.Attributes, err = fs.getAttributes(op.Inode)
	return err
}

func (fs *plakarFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	return fuse.ENOENT
}

func (fs *plakarFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	return nil
}

func (fs *plakarFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {

	inode, exists := fs.getInodeEntry(op.Inode)
	if !exists {
		return fuse.ENOENT
	}

	snap, err := snapshot.Load(fs.repository, inode.snapshotID)
	if err != nil {
		return fuse.EIO
	}

	info, exists := snap.Filesystem.LookupInode(inode.path[37:])
	if !exists {
		return fuse.ENOENT
	}

	rd, err := snap.NewReader(inode.path[37:])
	if err != nil {
		return fuse.EIO
	}

	if op.Offset > info.Size {
		return nil
	}

	_, err = rd.Seek(op.Offset, os.SEEK_SET)
	if err != nil {
		return err
	}

	buf := make([]byte, 16384)
	rd.Read(buf)
	b := copy(op.Dst, buf)
	op.BytesRead += b

	return nil

}

func (fs *plakarFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {

	if op.Inode == fuseops.RootInodeID {
		snapshotIDs, err := snapshot.List(fs.repository)
		if err != nil {
			return fuse.EIO
		}
		for _, snapshotID := range snapshotIDs {
			pathname := fmt.Sprintf("%s/%s", fs.mountpoint, snapshotID.String())

			_, exists := fs.getInode(pathname)
			if !exists {
				inodeID := allocateInodeID()
				fs.setInodeEntry(&inodeEntry{
					parentID:   fuseops.RootInodeID,
					inodeID:    inodeID,
					name:       snapshotID.String(),
					path:       pathname,
					snapshotID: snapshotID,
				})
			}
		}
		return nil
	}

	inode, exists := fs.getInodeEntry(op.Inode)
	if !exists {
		return fuse.ENOENT
	}

	var snapshotID uuid.UUID
	var lookupPath string
	if inode.parentID == fuseops.RootInodeID {
		snapshotID = uuid.MustParse(inode.name)
		lookupPath = "/"
	} else {
		snapshotID = inode.snapshotID
		lookupPath = inode.path[36+1:]
	}

	filesystem, err := fs.getFilesystem(snapshotID)
	if err != nil {
		return fuse.EIO
	}

	children, err := filesystem.LookupChildren(lookupPath)
	if err != nil {
		return fuse.EIO
	}

	for _, child := range children {
		var pathname string
		if inode.parentID == fuseops.RootInodeID {
			pathname = fmt.Sprintf("%s:/%s", inode.name, child)
		} else {
			pathname = fmt.Sprintf("%s:%s/%s", inode.snapshotID, lookupPath, child)
		}
		_, exists := fs.getInode(pathname)
		if !exists {
			inodeID := allocateInodeID()
			fs.setInodeEntry(&inodeEntry{
				parentID:   inode.inodeID,
				inodeID:    inodeID,
				name:       child,
				path:       pathname,
				snapshotID: snapshotID,
			})

		}
	}
	return nil

}

func (fs *plakarFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {

	dirents := make([]*fuseutil.Dirent, 0)

	if op.Inode == fuseops.RootInodeID {
		snapshotIDs, err := snapshot.List(fs.repository)
		if err != nil {
			return fuse.EIO
		}
		for i, snapshotID := range snapshotIDs {
			pathname := fmt.Sprintf("%s/%s", fs.mountpoint, snapshotID)
			inodeID, exists := fs.getInode(pathname)
			if !exists {
				return fuse.ENOENT
			}
			dirents = append(dirents, &fuseutil.Dirent{
				Offset: fuseops.DirOffset(i + 1),
				Inode:  inodeID,
				Name:   snapshotID.String(),
				Type:   fuseutil.DT_Directory,
			})
		}
	} else {
		inode, exists := fs.getInodeEntry(op.Inode)
		if !exists {
			return fuse.ENOENT
		}

		var snapshotID uuid.UUID
		var lookupPath string
		if inode.parentID == fuseops.RootInodeID {
			snapshotID = uuid.MustParse(inode.name)
			lookupPath = "/"
		} else {
			snapshotID = inode.snapshotID
			lookupPath = inode.path[36+1:]
		}

		filesystem, err := fs.getFilesystem(snapshotID)
		if err != nil {
			return fuse.EIO
		}

		children, err := filesystem.LookupChildren(lookupPath)
		if err != nil {
			return fuse.EIO
		}

		for i, child := range children {
			var inodeLookupPath string
			if inode.parentID == fuseops.RootInodeID {
				inodeLookupPath = "/" + child
			} else {
				inodeLookupPath = lookupPath + "/" + child
			}

			stat, exists := filesystem.LookupInode(inodeLookupPath)
			if !exists {
				return fuse.EIO
			}

			dtype := fuseutil.DT_Directory
			if stat.Mode.IsRegular() {
				dtype = fuseutil.DT_Char
			}

			pathname := fmt.Sprintf("%s:%s", inode.snapshotID, inodeLookupPath)
			inodeID, exists := fs.getInode(pathname)
			if !exists {
				return fuse.ENOENT
			}

			dirents = append(dirents, &fuseutil.Dirent{
				Offset: fuseops.DirOffset(i + 1),
				Inode:  inodeID,
				Name:   child,
				Type:   dtype,
			})
		}
	}

	if op.Offset > fuseops.DirOffset(len(dirents)) {
		return fuse.EIO
	}

	for _, dirent := range dirents[op.Offset:] {
		bytesWritten := fuseutil.WriteDirent(op.Dst[op.BytesRead:], *dirent)
		if bytesWritten == 0 {
			break
		}
		op.BytesRead += bytesWritten
	}
	return nil
}
