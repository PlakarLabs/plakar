package hashing

import (
	"crypto/sha256"
	"hash"
)

func DefaultAlgorithm() string {
	return "sha256"
}

func GetHasher(name string) hash.Hash {
	switch name {
	case "sha256":
		return sha256.New()
	default:
		return nil
	}
}
