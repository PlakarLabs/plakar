package packfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
)

const VERSION = 100

const (
	TYPE_SNAPSHOT  = 0
	TYPE_CHUNK     = 1
	TYPE_OBJECT    = 2
	TYPE_FILE      = 3
	TYPE_DIRECTORY = 4
	TYPE_DATA      = 5
)

type Blob struct {
	Type     uint8
	Checksum [32]byte
	Offset   uint32
	Length   uint32
}

func (b Blob) TypeName() string {
	switch b.Type {
	case TYPE_SNAPSHOT:
		return "snapshot"
	case TYPE_CHUNK:
		return "chunk"
	case TYPE_OBJECT:
		return "object"
	case TYPE_FILE:
		return "file"
	case TYPE_DIRECTORY:
		return "directory"
	case TYPE_DATA:
		return "data"
	default:
		return "unknown"
	}
}

type PackFile struct {
	Blobs  []byte
	Index  []Blob
	Footer PackFileFooter
}

type PackFileFooter struct {
	Version       uint32
	Timestamp     int64
	Count         uint32
	IndexOffset   uint32
	IndexChecksum [32]byte
}

type Configuration struct {
	MaxSize uint32
}

func DefaultConfiguration() *Configuration {
	return &Configuration{
		MaxSize: (20 << 10) << 10,
	}
}

func NewFooterFromBytes(serialized []byte) (PackFileFooter, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.NewFooterFromBytes", time.Since(t0))
		logger.Trace("packfile", "NewFooterFromBytes(...): %s", time.Since(t0))
	}()

	reader := bytes.NewReader(serialized)
	var footer PackFileFooter
	if err := binary.Read(reader, binary.LittleEndian, &footer.Version); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Timestamp); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Count); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexOffset); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexChecksum); err != nil {
		return footer, err
	}
	return footer, nil
}

func NewIndexFromBytes(serialized []byte) ([]Blob, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.NewIndexFromBytes", time.Since(t0))
		logger.Trace("packfile", "NewIndexFromBytes(...): %s", time.Since(t0))
	}()

	reader := bytes.NewReader(serialized)
	index := make([]Blob, 0)
	for reader.Len() > 0 {
		var dataType uint8
		var checksum [32]byte
		var chunkOffset uint32
		var chunkLength uint32
		if err := binary.Read(reader, binary.LittleEndian, &dataType); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &chunkOffset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &chunkLength); err != nil {
			return nil, err
		}
		index = append(index, Blob{
			Type:     dataType,
			Checksum: checksum,
			Offset:   chunkOffset,
			Length:   chunkLength,
		})
	}
	return index, nil
}

func New() *PackFile {
	return &PackFile{
		Blobs: make([]byte, 0),
		Index: make([]Blob, 0),
		Footer: PackFileFooter{
			Version:   VERSION,
			Timestamp: time.Now().UnixNano(),
			Count:     0,
		},
	}
}

func NewFromBytes(serialized []byte) (*PackFile, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.NewFromBytes", time.Since(t0))
		logger.Trace("packfile", "NewFromBytes(...): %s", time.Since(t0))
	}()

	reader := bytes.NewReader(serialized)

	var footer PackFileFooter
	_, err := reader.Seek(-52, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Version); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Count); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexChecksum); err != nil {
		return nil, err
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	data := make([]byte, footer.IndexOffset)
	if err := binary.Read(reader, binary.LittleEndian, &data); err != nil {
		return nil, err
	}

	// we won't read the totalLength again
	remaining := reader.Len() - 52

	p := New()
	p.Footer = footer
	p.Blobs = data
	hasher := sha256.New()
	for remaining > 0 {
		var dataType uint8
		var checksum [32]byte
		var chunkOffset uint32
		var chunkLength uint32
		if err := binary.Read(reader, binary.LittleEndian, &dataType); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &chunkOffset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &chunkLength); err != nil {
			return nil, err
		}

		if chunkOffset+chunkLength > p.Footer.IndexOffset {
			return nil, fmt.Errorf("chunk offset + chunk length exceeds total length of packfile")
		}

		if err := binary.Write(hasher, binary.LittleEndian, dataType); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunkOffset); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunkLength); err != nil {
			return nil, err
		}
		p.Index = append(p.Index, Blob{
			Type:     dataType,
			Checksum: checksum,
			Offset:   chunkOffset,
			Length:   chunkLength,
		})
		remaining -= (len(checksum) + 9)
	}
	checksum := [32]byte(hasher.Sum(nil))
	if checksum != p.Footer.IndexChecksum {
		return nil, fmt.Errorf("index checksum mismatch")
	}

	return p, nil
}

func (p *PackFile) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.Serialize", time.Since(t0))
		logger.Trace("packfile", "Serialize(): %s", time.Since(t0))
	}()

	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, p.Blobs); err != nil {
		return nil, err
	}

	hasher := sha256.New()
	for _, chunk := range p.Index {
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}

		if err := binary.Write(hasher, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}
	}
	p.Footer.IndexChecksum = [32]byte(hasher.Sum(nil))

	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Count); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.IndexOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.IndexChecksum); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (p *PackFile) SerializeData() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.SerializeData", time.Since(t0))
		logger.Trace("packfile", "SerializeData(): %s", time.Since(t0))
	}()

	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, p.Blobs); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (p *PackFile) SerializeIndex() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.SerializeIndex", time.Since(t0))
		logger.Trace("packfile", "SerializeIndex(): %s", time.Since(t0))
	}()

	var buffer bytes.Buffer
	hasher := sha256.New()
	for _, chunk := range p.Index {
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}

		if err := binary.Write(hasher, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (p *PackFile) SerializeFooter() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.SerializeFooter", time.Since(t0))
		logger.Trace("packfile", "SerializeFooter(): %s", time.Since(t0))
	}()

	var buffer bytes.Buffer
	hasher := sha256.New()
	for _, chunk := range p.Index {
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}

		if err := binary.Write(hasher, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}
	}
	p.Footer.IndexChecksum = [32]byte(hasher.Sum(nil))

	buffer.Reset()
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Count); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.IndexOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.IndexChecksum); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (p *PackFile) AddBlob(dataType uint8, checksum [32]byte, data []byte) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.AddBlob", time.Since(t0))
		logger.Trace("packfile", "AddBlob(...): %s", time.Since(t0))
	}()
	p.Index = append(p.Index, Blob{dataType, checksum, uint32(len(p.Blobs)), uint32(len(data))})
	p.Blobs = append(p.Blobs, data...)
	p.Footer.Count++
	p.Footer.IndexOffset = uint32(len(p.Blobs))
}

func (p *PackFile) GetBlob(checksum [32]byte) ([]byte, bool) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.GetBlob", time.Since(t0))
		logger.Trace("packfile", "GetBlob(...): %s", time.Since(t0))
	}()

	for _, chunk := range p.Index {
		if chunk.Checksum == checksum {
			return p.Blobs[chunk.Offset : chunk.Offset+chunk.Length], true
		}
	}
	return nil, false
}

func (p *PackFile) Size() uint32 {
	return uint32(len(p.Blobs))
}
