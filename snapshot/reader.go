package snapshot

import (
	"bytes"
	"io"
	"os"
)

type Reader struct {
	snapshot     *Snapshot
	object       *Object
	objectOffset int
	obuf         *bytes.Buffer
}

func (reader *Reader) Read(buf []byte) (int, error) {
	if reader.objectOffset == len(reader.object.Chunks) {
		if len(reader.obuf.Bytes()) != 0 {
			return reader.obuf.Read(buf)
		}
		return 0, io.EOF
	}

	obufLen := len(reader.obuf.Bytes())
	if obufLen < len(buf) {
		data, err := reader.snapshot.GetChunk(reader.object.Chunks[reader.objectOffset])
		if err != nil {
			return -1, err
		}

		_, err = reader.obuf.Write(data)
		if err != nil {
			return -1, err
		}

		reader.objectOffset++
	}

	return reader.obuf.Read(buf)
}

func (snapshot *Snapshot) NewReader(pathname string) (*Reader, error) {
	object := snapshot.LookupObjectForPathname(pathname)
	if object == nil {
		return nil, os.ErrNotExist
	}

	return &Reader{snapshot: snapshot, object: object, obuf: bytes.NewBuffer([]byte(""))}, nil
}
