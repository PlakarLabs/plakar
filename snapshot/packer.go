package snapshot

import "github.com/PlakarKorp/plakar/packfile"

type Packer struct {
	Blobs    map[packfile.Type]map[[32]byte][]byte
	Packfile *packfile.PackFile
}

func NewPacker() *Packer {
	blobs := make(map[packfile.Type]map[[32]byte][]byte)
	for _, Type := range packfile.Types() {
		blobs[Type] = make(map[[32]byte][]byte)
	}
	return &Packer{
		Packfile: packfile.New(),
		Blobs:    blobs,
	}
}

func (packer *Packer) AddBlob(Type packfile.Type, checksum [32]byte, data []byte) {
	if _, ok := packer.Blobs[Type]; !ok {
		packer.Blobs[Type] = make(map[[32]byte][]byte)
	}
	packer.Blobs[Type][checksum] = data
	packer.Packfile.AddBlob(Type, checksum, data)
}

func (packer *Packer) Size() uint32 {
	return packer.Packfile.Size()
}

func (packer *Packer) Types() []packfile.Type {
	ret := make([]packfile.Type, 0, len(packer.Blobs))
	for k := range packer.Blobs {
		ret = append(ret, k)
	}
	return ret
}
