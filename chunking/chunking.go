package chunking

type Configuration struct {
	Algorithm  string // Content-defined chunking algorithm (e.g., "rolling-hash", "fastcdc")
	MinSize    uint32 // Minimum chunk size
	NormalSize uint32 // Expected (average) chunk size
	MaxSize    uint32 // Maximum chunk size
}

func DefaultConfiguration() *Configuration {
	return &Configuration{
		Algorithm:  "FASTCDC",
		MinSize:    64 * 1024,
		NormalSize: 1 * 1024 * 1024,
		MaxSize:    4 * 1024 * 1024,
	}
}
