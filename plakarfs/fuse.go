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
	id         fuseops.InodeID
	name       string
	path       string
	isSnapshot bool
	snapshotID uuid.UUID
}

func allocateInodeID() fuseops.InodeID {
	inodeMutex.Lock()
	defer inodeMutex.Unlock()

	nextInode++

	return nextInode
}

var pathnameToInode map[string]fuseops.InodeID

type plakarFS struct {
	fuseutil.NotImplementedFileSystem

	repository *storage.Repository
	mountpoint string
	inodes     *sync.Map
}

func NewPlakarFS(repository *storage.Repository, mountpoint string) (fuse.Server, error) {

	pathnameToInode = make(map[string]fuseops.InodeID)

	fs := &plakarFS{
		repository: repository,
		mountpoint: mountpoint,
		inodes:     &sync.Map{},
	}
	fs.inodes.Store(fuseops.RootInodeID, &inodeEntry{
		name:       "/",
		id:         fuseops.RootInodeID,
		path:       mountpoint,
		isSnapshot: false,
	})

	return fuseutil.NewFileSystemServer(fs), nil
}

func (fs *plakarFS) getAttributes(id fuseops.InodeID) (fuseops.InodeAttributes, error) {

	if id == fuseops.RootInodeID {
		return fuseops.InodeAttributes{
			Nlink: 1,
			Mode:  0777 | os.ModeDir,
		}, nil
	}

	entry, exists := fs.inodes.Load(id)
	if !exists {
		return fuseops.InodeAttributes{}, fuse.ENOENT
	}

	inode := entry.(*inodeEntry)
	if inode.isSnapshot {
		metadata, _, err := snapshot.GetMetadata(fs.repository, uuid.MustParse(inode.name))
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

	filesystem, _, err := snapshot.GetFilesystem(fs.repository, inode.snapshotID)
	if err != nil {
		return fuseops.InodeAttributes{}, fuse.EIO
	}

	fmt.Println("=>", inode.path)
	lookuppath := inode.path[36+1:]
	fmt.Println("=>", lookuppath)

	fileinfo, exists := filesystem.LookupInode(lookuppath)
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
	fmt.Println("LookUpInode", op.Parent, op.Name)

	if op.Parent == fuseops.RootInodeID {
		pathname := fmt.Sprintf("%s/%s", fs.mountpoint, op.Name)
		fmt.Println(pathname)
		inodeID, exists := pathnameToInode[pathname]
		if !exists {
			return fuse.ENOENT
		}

		metadata, _, err := snapshot.GetMetadata(fs.repository, uuid.MustParse(op.Name))
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

	parentEntry, exists := fs.inodes.Load(op.Parent)
	if !exists {
		return fuse.ENOENT
	}

	inodeParent := parentEntry.(*inodeEntry)
	if inodeParent.isSnapshot {
		pathname := fmt.Sprintf("%s:/%s", inodeParent.name, op.Name)
		fmt.Println(pathname)
		inodeID, exists := pathnameToInode[pathname]
		if !exists {
			return fuse.ENOENT
		}

		entry, exists := fs.inodes.Load(inodeID)
		if !exists {
			return fuse.ENOENT
		}
		inode := entry.(*inodeEntry)

		lookuppath := inode.path[len(inodeParent.name)+1:]

		fmt.Println(">>>>>>>>>>>", inode.path, lookuppath)

		filesystem, _, err := snapshot.GetFilesystem(fs.repository, uuid.MustParse(inodeParent.name))
		if err != nil {
			return fuse.EIO
		}

		stat, exists := filesystem.LookupInode(lookuppath)
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

	} else {
		pathname := fmt.Sprintf("%s/%s", inodeParent.path, op.Name)
		fmt.Println(pathname)
		inodeID, exists := pathnameToInode[pathname]
		if !exists {
			return fuse.ENOENT
		}

		entry, exists := fs.inodes.Load(inodeID)
		if !exists {
			return fuse.ENOENT
		}
		inode := entry.(*inodeEntry)

		lookuppath := inode.path[36+1:]

		fmt.Println("#>>>>>>>>>>>", inode.path, lookuppath)

		filesystem, _, err := snapshot.GetFilesystem(fs.repository, inodeParent.snapshotID)
		if err != nil {
			return fuse.EIO
		}

		stat, exists := filesystem.LookupInode(lookuppath)
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

	return nil
}

func (fs *plakarFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	fmt.Println("GetInodeAttributes", op.Inode)
	var err error
	op.Attributes, err = fs.getAttributes(op.Inode)
	return err
}

func (fs *plakarFS) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	fmt.Println("SetInodeAttributes")
	return fuse.ENOENT
}

func (fs *plakarFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	fmt.Println("OpenFile")
	return nil
}

func (fs *plakarFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	fmt.Println("ReadFile")

	entry, exists := fs.inodes.Load(op.Inode)
	if !exists {
		return fuse.ENOENT
	}

	inode := entry.(*inodeEntry)
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

	// temphack until rd is a ReadSeekCloser
	buf := make([]byte, op.Offset)
	rd.Read(buf)

	buf = make([]byte, 16384)
	rd.Read(buf)
	b := copy(op.Dst, buf)
	op.BytesRead += b

	return nil

}

func (fs *plakarFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	fmt.Println("OpenDir", op)

	if op.Inode == fuseops.RootInodeID {
		snapshotIDs, err := snapshot.List(fs.repository)
		if err != nil {
			return fuse.EIO
		}
		for _, snapshotID := range snapshotIDs {
			pathname := fmt.Sprintf("%s/%s", fs.mountpoint, snapshotID.String())
			_, exists := pathnameToInode[pathname]
			if !exists {
				inodeID := allocateInodeID()
				fs.inodes.Store(inodeID, &inodeEntry{
					id:         inodeID,
					name:       snapshotID.String(),
					path:       pathname,
					isSnapshot: true,
				})
				pathnameToInode[pathname] = inodeID
			}
		}
		return nil
	}

	fmt.Println("#####1")
	entry, exists := fs.inodes.Load(op.Inode)
	if !exists {
		return fuse.ENOENT
	}
	fmt.Println("#####2")

	inode := entry.(*inodeEntry)
	if inode.isSnapshot {
		fmt.Println("#####3")

		filesystem, _, err := snapshot.GetFilesystem(fs.repository, uuid.MustParse(inode.name))
		if err != nil {
			return fuse.EIO
		}
		children, err := filesystem.LookupChildren("/")
		if err != nil {
			return fuse.EIO
		}
		fmt.Println("#####4")

		for _, child := range children {
			pathname := fmt.Sprintf("%s:/%s", inode.name, child)
			fmt.Println("#####5", pathname)
			_, exists := pathnameToInode[pathname]
			if !exists {
				inodeID := allocateInodeID()
				fs.inodes.Store(inodeID, &inodeEntry{
					id:         inodeID,
					name:       child,
					path:       pathname,
					isSnapshot: false,
					snapshotID: uuid.MustParse(inode.name),
				})
				pathnameToInode[pathname] = inodeID
			}
		}
		return nil

	}

	//
	fmt.Println("#####1000000000")
	filesystem, _, err := snapshot.GetFilesystem(fs.repository, inode.snapshotID)
	if err != nil {
		return fuse.EIO
	}
	children, err := filesystem.LookupChildren(inode.path[37:])
	if err != nil {
		return fuse.EIO
	}

	for _, child := range children {
		pathname := fmt.Sprintf("%s:%s/%s", inode.snapshotID, inode.path[37:], child)
		fmt.Println("#####5", pathname)
		_, exists := pathnameToInode[pathname]
		if !exists {
			inodeID := allocateInodeID()
			fs.inodes.Store(inodeID, &inodeEntry{
				id:         inodeID,
				name:       child,
				path:       pathname,
				isSnapshot: false,
				snapshotID: inode.snapshotID,
			})
			pathnameToInode[pathname] = inodeID
		}
	}
	return nil

}

func (fs *plakarFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	fmt.Println("ReadDir")

	dirents := make([]*fuseutil.Dirent, 0)

	if op.Inode == fuseops.RootInodeID {
		snapshotIDs, err := snapshot.List(fs.repository)
		if err != nil {
			return fuse.EIO
		}

		for i, snapshotID := range snapshotIDs {
			dirents = append(dirents, &fuseutil.Dirent{
				Offset: fuseops.DirOffset(i + 1),
				Inode:  pathnameToInode[fmt.Sprintf("%s/%s", fs.mountpoint, snapshotID)],
				Name:   snapshotID.String(),
				Type:   fuseutil.DT_Directory,
			})
		}
	} else {
		entry, exists := fs.inodes.Load(op.Inode)
		if !exists {
			return fuse.ENOENT
		}

		inode := entry.(*inodeEntry)
		if inode.isSnapshot {

			filesystem, _, err := snapshot.GetFilesystem(fs.repository, uuid.MustParse(inode.name))
			if err != nil {
				return fuse.EIO
			}

			children, err := filesystem.LookupChildren("/")
			if err != nil {
				return fuse.EIO
			}

			for i, child := range children {
				stat, exists := filesystem.LookupInode("/" + child)
				if !exists {
					return fuse.EIO
				}

				dtype := fuseutil.DT_Directory
				if stat.Mode.IsRegular() {
					dtype = fuseutil.DT_Char
				}

				dirents = append(dirents, &fuseutil.Dirent{
					Offset: fuseops.DirOffset(i + 1),
					Inode:  pathnameToInode[fmt.Sprintf("%s:/%s", inode.name, child)],
					Name:   child,
					Type:   dtype,
				})
			}
		} else {
			fmt.Println("@@@@1")

			filesystem, _, err := snapshot.GetFilesystem(fs.repository, inode.snapshotID)
			if err != nil {
				return fuse.EIO
			}
			children, err := filesystem.LookupChildren(inode.path[37:])
			if err != nil {
				return fuse.EIO
			}
			fmt.Println("@@@@2")

			for i, child := range children {
				fmt.Println("inode.path:", inode.path)
				stat, exists := filesystem.LookupInode(inode.path[37:] + "/" + child)
				if !exists {
					return fuse.EIO
				}
				fmt.Println("#2 inode.path:", inode.path)
				dtype := fuseutil.DT_Directory
				if stat.Mode.IsRegular() {
					dtype = fuseutil.DT_Char
				}
				dirents = append(dirents, &fuseutil.Dirent{
					Offset: fuseops.DirOffset(i + 1),
					Inode:  pathnameToInode[fmt.Sprintf("%s/%s", inode.path, child)],
					Name:   child,
					Type:   dtype,
				})
			}

		}
	}

	if op.Offset > fuseops.DirOffset(len(dirents)) {
		return fuse.EIO
	}

	for _, dirent := range dirents[op.Offset:] {
		fmt.Println(dirent)
		bytesWritten := fuseutil.WriteDirent(op.Dst[op.BytesRead:], *dirent)
		if bytesWritten == 0 {
			break
		}
		op.BytesRead += bytesWritten
	}
	return nil
}

/*
func (fs *plakarFS) getAttributes(id fuseops.InodeID) (fuseops.InodeAttributes, error) {
	if id == fuseops.RootInodeID {
		return fuseops.InodeAttributes{
			Nlink: 1,
			Mode:  0777 | os.ModeDir,
		}, nil
	}

	if snapshotUuid, exists := inodeToSnapshot[id]; exists {
		snap, _ := snapshot.Load(fs.store, snapshotUuid)
		return fuseops.InodeAttributes{
			Nlink:  1,
			Uid:    0,
			Gid:    0,
			Mode:   0555 | os.ModeDir,
			Size:   snap.Size,
			Atime:  snap.CreationTime,
			Ctime:  snap.CreationTime,
			Mtime:  snap.CreationTime,
			Crtime: snap.CreationTime,
		}, nil
	}

	if inodeInfo, exists := inodeToInfo[id]; exists {
		//snapshotUuid := inodeInSnapshot[inodeInfo.Inode]
		//, _ := snapshot.Load(fs.store, snapshotUuid)
		return fuseops.InodeAttributes{
			Nlink:  1,
			Uid:    inodeInfo.Uid,
			Gid:    inodeInfo.Gid,
			Mode:   inodeInfo.Mode,
			Size:   inodeInfo.Size,
			Atime:  inodeInfo.CreationTime,
			Ctime:  inodeInfo.CreationTime,
			Mtime:  inodeInfo.CreationTime,
			Crtime: inodeInfo.CreationTime,
		}, nil
	}

	return fuseops.InodeAttributes{}, fuse.ENOENT
}

////////////////////////////////////////////////////////////////////////
// FileSystem methods
////////////////////////////////////////////////////////////////////////

func (fs *plakarFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	return nil
}

func (fs *plakarFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {

	for _, inodeID := range children[op.Parent] {
		inodeInfo := inodeToInfo[inodeID]
		if inodeInfo.Name == op.Name {
			attr, _ := fs.getAttributes(inodeID)
			op.Entry = fuseops.ChildInodeEntry{
				Child:      inodeID,
				Attributes: attr,
			}
			return nil
		}
	}
	return fuse.ENOENT
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

	// Ignore any changes and simply return existing attributes.
	var err error
	op.Attributes, err = fs.getAttributes(op.Inode)
	return err
}

func (fs *plakarFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	return nil
}

func (fs *plakarFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {

	snapshotUuid := inodeInSnapshot[op.Inode]
	snapshotInstance, _ := snapshot.Load(fs.store, snapshotUuid)
	objectChecksum := snapshotInstance.Filenames[inodeToPath[op.Inode]]
	fileInfo := snapshotInstance.Filesystem.Files[inodeToPath[op.Inode]]
	object := snapshotInstance.Objects[objectChecksum]

	if op.Offset > fileInfo.Size {
		return nil
	}

	var bytesCopied int
	for _, chunk := range object.Chunks {
		if int64(chunk.Start+chunk.Length) < op.Offset {
			continue
		}

		data, _ := snapshotInstance.GetChunk(chunk.Checksum)

		var start int64
		if chunk.Start < uint(op.Offset) {
			b := copy(op.Dst, data[start:])
			bytesCopied += b
			op.BytesRead += b
		} else {
			b := copy(op.Dst, data[start:])
			bytesCopied += b
			op.BytesRead += b
		}

		if bytesCopied > 16384 {
			break
		}
	}
	return nil
}

func (fs *plakarFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	return nil
}

func (fs *plakarFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {

	var dirents []fuseutil.Dirent

	for offset, inodeID := range children[op.Inode] {
		inodeInfo := inodeToInfo[inodeID]
		var dtype fuseutil.DirentType
		if inodeInfo.Mode.IsDir() {
			dtype = fuseutil.DT_Directory
		} else {
			dtype = fuseutil.DT_File
		}

		dirents = append(dirents, fuseutil.Dirent{
			Offset: fuseops.DirOffset(offset) + 1,
			Inode:  inodeInfo.Inode,
			Name:   inodeInfo.Name,
			Type:   dtype,
		})
	}

	// If the offset is for the end of the listing, we're done. Otherwise we
	// expect it to be for the start.
	switch op.Offset {
	case fuseops.DirOffset(len(dirents)):
		return nil

	case 0:

	default:
		return fmt.Errorf("Unexpected offset: %v", op.Offset)
	}

	// Fill in the listing.
	for _, de := range dirents {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], de)

		// We don't support doing this in anything more than one shot.
		if n == 0 {
			return fmt.Errorf("Couldn't fit listing in %v bytes", len(op.Dst))
		}

		op.BytesRead += n
	}

	return nil
}
*/
