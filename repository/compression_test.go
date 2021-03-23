package repository

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestCompression(t *testing.T) {
	token := make([]byte, 65*1024)
	rand.Read(token)
	deflated := Deflate(token)
	fmt.Println(deflated)
	inflated, err := Inflate(deflated)
	if err != nil {
		t.Errorf("Inflate(Deflate(%q)) != %q", inflated, token)
	}
}
