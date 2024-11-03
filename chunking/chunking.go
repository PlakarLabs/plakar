package chunking

import chunkers "github.com/PlakarLabs/go-cdc-chunkers"

func DefaultAlgorithm() string {
	return "fastcdc"
}

func DefaultConfiguration() *chunkers.ChunkerOpts {
	return &chunkers.ChunkerOpts{
		MinSize:    64 * 1024,
		NormalSize: 1 * 1024 * 1024,
		MaxSize:    4 * 1024 * 1024,
	}
}
