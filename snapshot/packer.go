package snapshot

import "github.com/PlakarKorp/plakar/packfile"

type Packer struct {
	Blobs    map[packfile.BlobType]map[[32]byte][]byte
	Packfile *packfile.PackFile
}

func NewPacker() *Packer {
	blobs := make(map[packfile.BlobType]map[[32]byte][]byte)
	for _, blobType := range packfile.Types() {
		blobs[blobType] = make(map[[32]byte][]byte)
	}
	return &Packer{
		Packfile: packfile.New(),
		Blobs:    blobs,
	}
}

func (packer *Packer) AddBlob(blobType packfile.BlobType, checksum [32]byte, data []byte) {
	if _, ok := packer.Blobs[blobType]; !ok {
		packer.Blobs[blobType] = make(map[[32]byte][]byte)
	}
	packer.Blobs[blobType][checksum] = data
	packer.Packfile.AddBlob(blobType, checksum, data)
}

func (packer *Packer) Size() uint32 {
	return packer.Packfile.Size()
}

func (packer *Packer) BlobTypes() []packfile.BlobType {
	ret := make([]packfile.BlobType, 0, len(packer.Blobs))
	for k := range packer.Blobs {
		ret = append(ret, k)
	}
	return ret
}
