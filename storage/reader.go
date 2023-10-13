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
	object       *objects.Object
	objectOffset int
	obuf         *bytes.Buffer

	chunks []index.IndexChunk
	offset int64
	size   int64
}

func (reader *Reader) GetContentType() string {
	return reader.object.ContentType
}

func (reader *Reader) Read(buf []byte) (int, error) {

	if reader.offset == reader.size {
		return 0, io.EOF
	}

	readSize := uint(len(buf))
	for chunkOffset, chunkInfo := range reader.chunks {
		// reader offset is past this chunk, skip
		if reader.offset > int64(chunkInfo.Start)+int64(chunkInfo.Length) {
			continue
		}

		// we have data to read from this chunk, fetch content
		data, err := reader.repository.GetChunk(reader.object.Chunks[chunkOffset])
		if err != nil {
			return -1, err
		}

		// compute how much we can read from this one
		endOffset := chunkInfo.Start + uint64(chunkInfo.Length)
		available := endOffset - uint64(reader.offset)

		// find beginning and ending offsets in current chunk
		beg := uint64(chunkInfo.Length) - available
		end := beg + available
		if available >= uint64(readSize) {
			end = beg + uint64(readSize)
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

	chunks := make([]index.IndexChunk, 0)
	size := int64(0)
	for _, chunkChecksum := range object.Chunks {
		chunkID, exists := rdIndex.ChecksumToId(chunkChecksum)
		if !exists {
			return nil, os.ErrNotExist
		}
		chunk := rdIndex.Chunks[chunkID]

		chunks = append(chunks, chunk)
		size += int64(chunk.Length)
	}

	return &Reader{repository: repository, object: object, chunks: chunks, obuf: bytes.NewBuffer([]byte("")), offset: 0, size: size}, nil
}
