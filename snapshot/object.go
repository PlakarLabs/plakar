package snapshot

type Object struct {
	Checksum    [32]byte
	Chunks      [][32]byte
	ContentType string
}
