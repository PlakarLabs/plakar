package storage

import (
	"bytes"
	"io"
	"os"
	"path"

	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/index"
	"github.com/PlakarLabs/plakar/objects"
)

type Reader struct {
	repository *Repository
	//index        *index.Index
	object *objects.Object
	//objectOffset int
	obuf *bytes.Buffer

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
		data, err := reader.repository.GetChunk(reader.object.Chunks[chunkOffset])
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

func (repository *Repository) NewReader(rdIndex *index.Index, pathname string) (*Reader, error) {
	pathname = path.Clean(pathname)

	hasher := encryption.GetHasher(repository.Configuration().Hashing)
	hasher.Write([]byte(pathname))
	pathnameHash := hasher.Sum(nil)

	var key [32]byte
	copy(key[:], pathnameHash[:32])

	object := rdIndex.LookupObjectForPathnameHash(key)
	if object == nil {
		return nil, os.ErrNotExist
	}

	chunksLengths := make([]uint32, 0)
	size := int64(0)
	for _, chunkChecksum := range object.Chunks {
		chunkID, exists := rdIndex.ChecksumToId(chunkChecksum)
		if !exists {
			return nil, os.ErrNotExist
		}
		chunkLength := rdIndex.Chunks[chunkID]

		chunksLengths = append(chunksLengths, chunkLength)
		size += int64(chunkLength)
	}

	return &Reader{repository: repository, object: object, chunksLengths: chunksLengths, obuf: bytes.NewBuffer([]byte("")), offset: 0, size: size}, nil
}
