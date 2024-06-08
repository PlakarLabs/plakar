package main

import (
	"crypto/ed25519"
	"log"
	"time"

	"github.com/PlakarLabs/plakar/agent"
)

var Uptime = time.Now()
var PublicKey, PrivateKey []byte = nil, nil
var srv *agent.Server

func init() {
	_publicKey, _privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		log.Fatalf("Could not generate keys: %s\n", err.Error())
	}
	PublicKey = _publicKey
	PrivateKey = _privateKey
}

func main() {
	srv = agent.NewServer(":8081", PublicKey, PrivateKey)
	srv.ListenAndServe()
}
