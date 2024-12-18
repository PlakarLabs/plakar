package vfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/PlakarKorp/plakar/packfile"
)

type VFilep struct {
	vfs      *Filesystem
	vfsEntry FSEntry

	isDir bool

	obuf          *bytes.Buffer
	chunksLengths []uint32
	offset        int64
	size          int64
}

var _ fs.File = (*VFilep)(nil)
var _ fs.ReadDirFile = (*VFilep)(nil)

func NewVFilep(fs *Filesystem, entry FSEntry) *VFilep {
	chunksLengths := make([]uint32, 0)
	size := int64(0)

	if entry.Stat().IsDir() {
		return &VFilep{
			vfs:      fs,
			vfsEntry: entry,
			isDir:    true,
		}
	} else if fileEntry, ok := entry.(*FileEntry); ok {
		if fileEntry.Object != nil {
			for _, chunk := range fileEntry.Object.Chunks {
				chunksLengths = append(chunksLengths, chunk.Length)
				size += int64(chunk.Length)
			}
		}
		return &VFilep{
			vfs:           fs,
			vfsEntry:      entry,
			chunksLengths: chunksLengths,
			obuf:          bytes.NewBuffer([]byte{}),
			offset:        0,
			size:          size,
		}
	}
	panic("invalid entry type")
}

func (vf *VFilep) Read(p []byte) (int, error) {
	if vf.isDir {
		return 0, errors.New("not a file")
	}

	fileEntry := vf.vfsEntry.(*FileEntry)

	if fileEntry.Object == nil {
		return 0, fs.ErrInvalid
	}

	if vf.offset == vf.size {
		return 0, io.EOF
	}

	readSize := uint(len(p))
	chunkStart := int64(0)
	for chunkOffset, chunkLength := range vf.chunksLengths {
		// reader offset is past this chunk, skip
		if vf.offset > chunkStart+int64(chunkLength) {
			chunkStart += int64(chunkLength)
			continue
		}

		// we have data to read from this chunk, fetch content
		rd, err := vf.vfs.repo.GetBlob(packfile.TYPE_CHUNK, fileEntry.Object.Chunks[chunkOffset].Checksum)
		if err != nil {
			return -1, err
		}

		data, err := io.ReadAll(rd)
		if err != nil {
			return -1, err
		}

		// compute how much we can read from this one
		endOffset := chunkStart + int64(chunkLength)
		available := endOffset - int64(vf.offset)
		chunkStart += int64(chunkLength)

		// find beginning and ending offsets in current chunk
		beg := int64(chunkLength) - available
		end := beg + available
		if available >= int64(readSize) {
			end = beg + int64(readSize)
		}
		if end > int64(len(data)) {
			end = int64(len(data))
		}

		nbytes, err := vf.obuf.Write(data[beg:end])
		if err != nil {
			return -1, err
		}

		// update offset and remaining buffer capacity, possibly exiting loop
		vf.offset += int64(nbytes)
		readSize -= uint(nbytes)
		if vf.offset == vf.size || readSize == 0 {
			break
		}
	}

	return vf.obuf.Read(p)
}

func (vf *VFilep) Close() error {
	if !vf.isDir {
		vf.offset = 0
		vf.size = 0
		vf.obuf.Reset()
	}
	return nil
}

func (vf *VFilep) Stat() (fs.FileInfo, error) {
	return vf.vfsEntry.Stat(), nil
}

func (vf *VFilep) ReadDir(count int) ([]fs.DirEntry, error) {
	if !vf.isDir {
		return nil, errors.New("not a directory")
	}

	children, err := vf.vfs.Children(vf.vfsEntry.Path())
	if err != nil {
		return nil, err
	}

	ret := make([]fs.DirEntry, 0)

	offset := 0
	for childname := range children {
		if count > 0 && offset >= count {
			offset++
			continue
		}
		offset++

		st, err := vf.vfs.Stat(filepath.Join(vf.vfsEntry.Path(), childname))
		if err != nil {
			continue
		} else {
			ret = append(ret, NewVFilep(vf.vfs, st))
		}
	}

	return ret, nil
}

func (vf *VFilep) Seek(offset int64, whence int) (int64, error) {
	if vf.isDir {
		return 0, errors.New("not a file")
	}

	fileEntry := vf.vfsEntry.(*FileEntry)

	if fileEntry.Object == nil {
		return 0, fs.ErrInvalid
	}

	switch whence {
	case io.SeekStart:
		if offset > vf.size {
			return 0, io.EOF
		}
		vf.offset = offset
	case io.SeekCurrent:
		if vf.offset+offset > vf.size {
			return 0, io.EOF
		}
		vf.offset += offset
	case io.SeekEnd:
		if offset > vf.size {
			return 0, io.EOF
		}
		vf.offset = vf.size - offset
	}
	return vf.offset, nil
}

func (vf *VFilep) Name() string {
	return vf.vfsEntry.Name()
}

func (vf *VFilep) IsDir() bool {
	_, isDir := vf.vfsEntry.(*DirEntry)
	return isDir
}

func (vf *VFilep) Type() fs.FileMode {
	if vf.isDir {
		return fs.ModeDir
	}
	return 0
}

func (vf *VFilep) Info() (fs.FileInfo, error) {
	return vf.vfsEntry.Stat(), nil
}

func (vf *VFilep) Size() int64 {
	return vf.vfsEntry.Size()
}

func (vf *VFilep) Mode() fs.FileMode {
	if vf.isDir {
		return fs.ModeDir | 0755
	}
	return 0644
}

func (vf *VFilep) ModTime() (time.Time, error) {
	return vf.vfsEntry.Stat().ModTime(), nil
}

func (vf *VFilep) Sys() interface{} {
	return nil
}
