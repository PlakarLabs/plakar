package index

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/PlakarLabs/plakar/logger"
)

const VERSION string = "0.1.0"

type Index struct {
	dirname string
	db      *leveldb.DB
	hasher  hash.Hash
}

func NewIndex() (*Index, error) {
	dname, err := os.MkdirTemp("", "plakar-index-")
	if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(dname, nil)
	if err != nil {
		return nil, err
	}
	return &Index{
		dirname: dname,
		db:      db,
		hasher:  sha256.New(),
	}, err
}

func FromBytes(data []byte) (*Index, error) {
	dname, err := os.MkdirTemp("", "plakar-vfs-")
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(data)
	gr, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	tr := tar.NewReader(gr)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return nil, err
		}

		target := filepath.Join(dname, hdr.Name)

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return nil, err

			}
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(hdr.Mode))
			if err != nil {
				return nil, err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return nil, err
			}
			f.Close()
		}
	}

	db, err := leveldb.OpenFile(dname, nil)
	if err != nil {
		return nil, err
	}
	return &Index{
		dirname: dname,
		db:      db,
		hasher:  sha256.New(),
	}, err
}

func (i *Index) Checksum() []byte {
	return i.hasher.Sum(nil)
}

func (i *Index) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	err := filepath.Walk(i.dirname, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		hdr, err := tar.FileInfoHeader(fi, file)
		if err != nil {
			return err
		}

		hdr.Name = filepath.ToSlash(file[len(i.dirname):])

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		if !fi.Mode().IsRegular() {
			return nil
		}

		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(tw, f); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func (index *Index) Close() error {
	return index.db.Close()
}

func (index *Index) AddChunk(chunk *objects.Chunk) error {
	logger.Trace("index", "AddChunk(%064x)", chunk.Checksum)

	key := fmt.Sprintf("chunk:%064x", chunk.Checksum)
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, chunk.Length)

	return index.db.Put([]byte(key), lengthBytes, nil)
}

func (index *Index) AddObject(object *objects.Object) error {
	logger.Trace("index", "AddObject(%064x)", object.Checksum)

	key := fmt.Sprintf("object:%064x", object.Checksum)
	var buffer bytes.Buffer
	for _, chunk := range object.Chunks {
		buffer.Write(chunk[:])
	}

	return index.db.Put([]byte(key), buffer.Bytes(), nil)
}

func (index *Index) LinkPathnameToObject(pathnameChecksum [32]byte, object *objects.Object) error {
	key := fmt.Sprintf("pathname:%064x", pathnameChecksum)
	value := object.Checksum[:]

	return index.db.Put([]byte(key), value, nil)
}

func (index *Index) LookupChunk(checksum [32]byte) (*objects.Chunk, error) {
	key := fmt.Sprintf("chunk:%064x", checksum)
	data, err := index.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(data)
	return &objects.Chunk{
		Checksum: checksum,
		Length:   length,
	}, nil
}

func (index *Index) ChunkExists(checksum [32]byte) (bool, error) {
	_, err := index.LookupChunk(checksum)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (index *Index) LookupObject(checksum [32]byte) (*objects.Object, error) {
	key := fmt.Sprintf("object:%064x", checksum)
	data, err := index.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	chunks := make([][32]byte, len(data)/32)
	for i := 0; i < len(chunks); i++ {
		copy(chunks[i][:], data[i*32:(i+1)*32])
	}

	return &objects.Object{
		Checksum: checksum,
		Chunks:   chunks,
	}, nil
}

func (index *Index) ObjectExists(checksum [32]byte) (bool, error) {
	_, err := index.LookupObject(checksum)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (index *Index) LookupObjectForPathnameChecksum(pathnameChecksum [32]byte) (*objects.Object, error) {
	key := fmt.Sprintf("pathname:%064x", pathnameChecksum)
	data, err := index.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	var objectChecksum [32]byte
	copy(objectChecksum[:], data)
	return index.LookupObject(objectChecksum)
}

func (index *Index) GetChunkLength(checksum [32]byte) (uint32, bool, error) {
	chunk, err := index.LookupChunk(checksum)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}

	return chunk.Length, true, nil
}

func (index *Index) ListObjects() <-chan [32]byte {
	ch := make(chan [32]byte)
	go func() {
		iter := index.db.NewIterator(nil, nil)
		for iter.Next() {
			key := iter.Key()
			if bytes.HasPrefix(key, []byte("object:")) {
				byteArray, err := hex.DecodeString(string(key[7:]))
				if err != nil {
					panic(err)
				}
				var checksum [32]byte
				copy(checksum[:], byteArray)
				ch <- checksum
			}
		}
		iter.Release()
		// TODO handle error later
		//if err := iter.Error(); err != nil {
		//	return err
		//}
		close(ch)
	}()
	return ch
}

func (index *Index) ListChunks() <-chan [32]byte {
	ch := make(chan [32]byte)
	go func() {
		iter := index.db.NewIterator(nil, nil)
		for iter.Next() {
			key := iter.Key()
			if bytes.HasPrefix(key, []byte("chunk:")) {
				byteArray, err := hex.DecodeString(string(key[6:]))
				if err != nil {
					panic(err)
				}
				var checksum [32]byte
				copy(checksum[:], byteArray)
				ch <- checksum
			}
		}
		iter.Release()
		// TODO handle error later
		//if err := iter.Error(); err != nil {
		//	return err
		//}
		close(ch)
	}()
	return ch
}
