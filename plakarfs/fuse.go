package plakarfs

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

var inodeMutex sync.Mutex
var nextInode fuseops.InodeID = fuseops.RootInodeID

func allocateInodeID() fuseops.InodeID {
	inodeMutex.Lock()
	defer inodeMutex.Unlock()

	nextInode++

	return nextInode
}

type InodeInfo struct {
	Name         string
	Path         string
	Inode        fuseops.InodeID
	Mode         os.FileMode
	Size         uint64
	Uid          uint32
	Gid          uint32
	CreationTime time.Time
}

var snapshotToInode map[string]fuseops.InodeID
var inodeToSnapshot map[fuseops.InodeID]string
var inodeInSnapshot map[fuseops.InodeID]string
var inodeToInfo map[fuseops.InodeID]InodeInfo
var children map[fuseops.InodeID][]fuseops.InodeID
var pathToInode map[string]fuseops.InodeID
var inodeToPath map[fuseops.InodeID]string

type plakarFS struct {
	fuseutil.NotImplementedFileSystem

	store storage.Store
}

func NewPlakarFS(store storage.Store) (fuse.Server, error) {
	snapshotToInode = make(map[string]fuseops.InodeID)
	inodeToSnapshot = make(map[fuseops.InodeID]string)
	inodeInSnapshot = make(map[fuseops.InodeID]string)
	inodeToInfo = make(map[fuseops.InodeID]InodeInfo)
	children = make(map[fuseops.InodeID][]fuseops.InodeID)
	pathToInode = make(map[string]fuseops.InodeID)
	inodeToPath = make(map[fuseops.InodeID]string)

	fs := &plakarFS{store: store}

	snapshotsList, _ := snapshot.List(store)
	for _, snapshotUuid := range snapshotsList {
		snapshotInstance, _ := snapshot.Load(store, snapshotUuid)
		snapshotInodeID := allocateInodeID()
		inodeInfo := InodeInfo{
			Name:  snapshotUuid,
			Path:  fmt.Sprintf("/%s", snapshotUuid),
			Inode: snapshotInodeID,
			Mode:  0555 | os.ModeDir,
		}
		children[fuseops.RootInodeID] = append(children[fuseops.RootInodeID], snapshotInodeID)
		inodeToInfo[snapshotInodeID] = inodeInfo
		inodeToSnapshot[snapshotInodeID] = snapshotUuid

		for pathname, fileInfo := range snapshotInstance.Directories {
			dirInodeID := allocateInodeID()
			inodeInfo := InodeInfo{
				Name:  fileInfo.Name,
				Inode: dirInodeID,
				Mode:  fileInfo.Mode | os.ModeDir,
				Size:  uint64(fileInfo.Size),
			}
			inodeToInfo[dirInodeID] = inodeInfo
			children[snapshotInodeID] = append(children[snapshotInodeID], dirInodeID)
			inodeInSnapshot[dirInodeID] = snapshotUuid
			pathToInode[path.Clean(pathname)] = dirInodeID
		}

		for pathname, fileInfo := range snapshotInstance.Files {
			inodeID := allocateInodeID()
			inodeInfo := InodeInfo{
				Name:  fileInfo.Name,
				Path:  fmt.Sprintf("/%s/%s", snapshotUuid, fileInfo.Name),
				Inode: inodeID,
				Mode:  fileInfo.Mode,
				Size:  uint64(fileInfo.Size),
			}
			inodeToInfo[inodeID] = inodeInfo
			basedir := filepath.Dir(pathname)
			inodeToPath[inodeID] = pathname

			dirInodeID := pathToInode[basedir]
			children[dirInodeID] = append(children[dirInodeID], inodeID)
			inodeInSnapshot[inodeID] = snapshotUuid

		}

	}

	return fuseutil.NewFileSystemServer(fs), nil
}

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
	objectChecksum := snapshotInstance.Pathnames[inodeToPath[op.Inode]]
	fileInfo := snapshotInstance.Files[inodeToPath[op.Inode]]
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
