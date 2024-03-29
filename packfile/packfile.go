package packfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
)

const (
	TYPE_CHUNK  = 1
	TYPE_OBJECT = 2
)

type Chunk struct {
	DataType uint8
	Checksum [32]byte
	Offset   uint32
	Length   uint32
}

type PackFile struct {
	Data  []byte
	Index []Chunk
}

func New() *PackFile {
	return &PackFile{
		Data:  make([]byte, 0),
		Index: make([]Chunk, 0),
	}
}

func NewFromBytes(serialized []byte) (*PackFile, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.NewFromBytes", time.Since(t0))
		logger.Trace("packfile", "NewFromBytes(...): %s", time.Since(t0))
	}()

	reader := bytes.NewReader(serialized)

	var totalLength uint32
	_, err := reader.Seek(-4, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &totalLength); err != nil {
		return nil, err
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	data := make([]byte, totalLength)
	if err := binary.Read(reader, binary.LittleEndian, &data); err != nil {
		return nil, err
	}

	// we won't read the totalLength again
	remaining := reader.Len() - 4

	p := New()
	p.Data = data
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

		if chunkOffset+chunkLength > totalLength {
			return nil, fmt.Errorf("chunk offset + chunk length exceeds total length of packfile")
		}

		p.Index = append(p.Index, Chunk{
			DataType: dataType,
			Checksum: checksum,
			Offset:   chunkOffset,
			Length:   chunkLength,
		})
		remaining -= (len(checksum) + 9)
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
	if err := binary.Write(&buffer, binary.LittleEndian, p.Data); err != nil {
		return nil, err
	}
	for _, chunk := range p.Index {
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.DataType); err != nil {
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
	}
	totalLength := uint32(len(p.Data))
	if err := binary.Write(&buffer, binary.LittleEndian, totalLength); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (p *PackFile) AddData(dataType uint8, checksum [32]byte, data []byte) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.AddChunk", time.Since(t0))
		logger.Trace("packfile", "AddChunk(...): %s", time.Since(t0))
	}()
	p.Index = append(p.Index, Chunk{dataType, checksum, uint32(len(p.Data)), uint32(len(data))})
	p.Data = append(p.Data, data...)

}

func (p *PackFile) GetChunk(checksum [32]byte) ([]byte, bool) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.GetChunk", time.Since(t0))
		logger.Trace("packfile", "GetChunk(...): %s", time.Since(t0))
	}()

	for _, chunk := range p.Index {
		if chunk.Checksum == checksum {
			return p.Data[chunk.Offset : chunk.Offset+chunk.Length], true
		}
	}
	return nil, false
}

func (p *PackFile) Size() uint32 {
	return uint32(len(p.Data))
}
