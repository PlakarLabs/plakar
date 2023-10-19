package packfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
)

type Chunk struct {
	Offset uint32
	Length uint32
}

type PackFile struct {
	Data  []byte
	Index map[[32]byte]Chunk
}

func New() *PackFile {
	return &PackFile{
		Data:  make([]byte, 0),
		Index: make(map[[32]byte]Chunk),
	}
}

func NewFromBytes(serialized []byte) (*PackFile, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.NewFromBytes", time.Since(t0))
		logger.Trace("packfile", "NewFromBytes(...): %s", time.Since(t0))
	}()

	var totalLength uint32
	reader := bytes.NewReader(serialized)
	if err := binary.Read(reader, binary.LittleEndian, &totalLength); err != nil {
		return nil, err
	}

	data := make([]byte, totalLength)
	if err := binary.Read(reader, binary.LittleEndian, &data); err != nil {
		return nil, err
	}

	p := New()
	p.Data = data
	for reader.Len() > 0 {
		var checksum [32]byte
		var chunkOffset uint32
		var chunkLength uint32
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

		p.Index[checksum] = Chunk{
			Offset: chunkOffset,
			Length: chunkLength,
		}
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
	totalLength := uint32(len(p.Data))
	if err := binary.Write(&buffer, binary.LittleEndian, totalLength); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Data); err != nil {
		return nil, err
	}
	for checksum, chunk := range p.Index {
		if err := binary.Write(&buffer, binary.LittleEndian, checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (p *PackFile) AddChunk(checksum [32]byte, data []byte) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.AddChunk", time.Since(t0))
		logger.Trace("packfile", "AddChunk(...): %s", time.Since(t0))
	}()

	if _, exists := p.Index[checksum]; !exists {
		p.Index[checksum] = Chunk{uint32(len(p.Data)), uint32(len(data))}
		p.Data = append(p.Data, data...)
	}
}

func (p *PackFile) GetChunk(checksum [32]byte) ([]byte, bool) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.GetChunk", time.Since(t0))
		logger.Trace("packfile", "GetChunk(...): %s", time.Since(t0))
	}()

	if chunk, exists := p.Index[checksum]; !exists {
		return nil, false
	} else {
		return p.Data[chunk.Offset : chunk.Offset+chunk.Length], true
	}
}

func (p *PackFile) Size() uint32 {
	return uint32(len(p.Data))
}
