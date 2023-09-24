package encryption

import (
	"crypto/sha256"
	"hash"

	"github.com/zeebo/blake3"
)

func GetHasher(name string) hash.Hash {
	switch name {
	case "sha256":
		return sha256.New()
	case "blake3":
		return blake3.New()
	default:
		return nil
	}
}
