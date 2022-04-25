package snapshot

type Chunk struct {
	Checksum [32]byte
	Start    uint
	Length   uint
}
