package vfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"

	"github.com/PlakarKorp/plakar/packfile"
)

type VFile struct {
	vfs      *Filesystem
	vfsEntry *FileEntry

	obuf          *bytes.Buffer
	chunksLengths []uint32
	offset        int64
	size          int64
}

func NewVFile(fs *Filesystem, entry *FileEntry) *VFile {
	chunksLengths := make([]uint32, 0)
	size := int64(0)

	if entry.Object != nil {
		for _, chunk := range entry.Object.Chunks {
			chunksLengths = append(chunksLengths, chunk.Length)
			size += int64(chunk.Length)
		}
	}
	return &VFile{
		vfs:           fs,
		vfsEntry:      entry,
		chunksLengths: chunksLengths,
		obuf:          bytes.NewBuffer([]byte{}),
		offset:        0,
		size:          size,
	}
}

func (vf *VFile) Read(p []byte) (int, error) {
	if vf.vfsEntry.Object == nil {
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
		rd, _, err := vf.vfs.repo.GetBlob(packfile.TYPE_CHUNK, vf.vfsEntry.Object.Chunks[chunkOffset].Checksum)
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

func (vf *VFile) Close() error {
	vf.offset = 0
	vf.size = 0
	vf.obuf.Reset()
	return nil
}

func (vf *VFile) Stat() (fs.FileInfo, error) {
	return vf.vfsEntry.FileInfo, nil
}

func (vf *VFile) ReadDir(count int) ([]fs.DirEntry, error) {
	return nil, errors.New("not a directory")
}

func (vf *VFile) Seek(offset int64, whence int) (int64, error) {
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
