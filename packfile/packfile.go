package packfile

import (
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/profiler"
	"github.com/vmihailenco/msgpack/v5"
)

type PackFile struct {
	Chunks [][]byte
	Index  map[[32]byte]uint32
	size   uint32
}

func New() *PackFile {
	return &PackFile{
		Chunks: make([][]byte, 0),
	}
}

func NewFromBytes(serialized []byte) (*PackFile, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.NewFromBytes", time.Since(t0))
		logger.Trace("packfile", "NewFromBytes(...): %s", time.Since(t0))
	}()

	var p PackFile
	if err := msgpack.Unmarshal(serialized, &p); err != nil {
		return nil, err
	}
	for _, chunk := range p.Chunks {
		p.size += uint32(len(chunk))
	}
	return &p, nil
}

func (p *PackFile) Serialize() ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.Serialize", time.Since(t0))
		logger.Trace("packfile", "Serialize(): %s", time.Since(t0))
	}()

	serialized, err := msgpack.Marshal(p)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

func (p *PackFile) AddChunk(checksum [32]byte, data []byte) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("packfile.AddChunk", time.Since(t0))
		logger.Trace("packfile", "AddChunk(...): %s", time.Since(t0))
	}()

	if _, exists := p.Index[checksum]; !exists {
		p.Index[checksum] = uint32(len(p.Chunks))
		p.Chunks = append(p.Chunks, data)
		p.size += uint32(len(data))
	}
}

func (p *PackFile) Size() uint32 {
	return p.size
}
