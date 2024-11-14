package snapshot

import (
	"bytes"
	"io"
	"os"
	"path"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

type Reader struct {
	snapshot *Snapshot
	object   *objects.Object
	obuf     *bytes.Buffer

	chunksLengths []uint32
	offset        int64
	size          int64
}

func (reader *Reader) GetContentType() string {
	return reader.object.ContentType
}

func (reader *Reader) Read(buf []byte) (int, error) {
	if reader.offset == reader.size {
		return 0, io.EOF
	}

	readSize := uint(len(buf))
	chunkStart := int64(0)
	for chunkOffset, chunkLength := range reader.chunksLengths {
		// reader offset is past this chunk, skip
		if reader.offset > chunkStart+int64(chunkLength) {
			chunkStart += int64(chunkLength)
			continue
		}

		// we have data to read from this chunk, fetch content
		data, err := reader.snapshot.GetChunk(reader.object.Chunks[chunkOffset].Checksum)
		if err != nil {
			return -1, err
		}

		// compute how much we can read from this one
		endOffset := chunkStart + int64(chunkLength)
		available := endOffset - int64(reader.offset)
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

		nbytes, err := reader.obuf.Write(data[beg:end])
		if err != nil {
			return -1, err
		}

		// update offset and remaining buffer capacity, possibly exiting loop
		reader.offset += int64(nbytes)
		readSize -= uint(nbytes)
		if reader.offset == reader.size || readSize == 0 {
			break
		}
	}

	return reader.obuf.Read(buf)
}

func (reader *Reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset >= reader.size {
			return 0, io.EOF
		}
		reader.offset = offset
	case io.SeekCurrent:
		if reader.offset+offset >= reader.size {
			return 0, io.EOF
		}
		reader.offset += offset
	case io.SeekEnd:
		if offset > reader.size {
			return 0, io.EOF
		}
		reader.offset = reader.size - offset
	}
	return reader.offset, nil
}

func (reader *Reader) Close() error {
	return nil
}

func NewReader(snap *Snapshot, pathname string) (*Reader, error) {
	pathname = path.Clean(pathname)

	fs, err := vfs.NewFilesystem(snap.Repository(), snap.Header.Root)
	if err != nil {
		return nil, err
	}

	st, err := fs.Stat(pathname)
	if err != nil {
		return nil, err
	} else if _, isDir := st.(*vfs.DirEntry); isDir {
		return nil, os.ErrInvalid
	}

	if st := st.(*vfs.FileEntry); st != nil && st.Type == importer.RecordTypeFile {
		object, err := snap.LookupObject(st.Object.Checksum)
		if err != nil {
			return nil, err
		}
		chunksLengths := make([]uint32, 0)
		size := int64(0)
		for _, chunk := range object.Chunks {
			chunksLengths = append(chunksLengths, chunk.Length)
			size += int64(chunk.Length)
		}
		return &Reader{snapshot: snap, object: object, chunksLengths: chunksLengths, obuf: bytes.NewBuffer([]byte("")), offset: 0, size: size}, nil
	}
	return nil, os.ErrNotExist
}
