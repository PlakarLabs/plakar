package main

import (
	"crypto/ed25519"
	"flag"
	"log"
	"os"
	"time"

	"github.com/PlakarLabs/plakar/agent"
	"gopkg.in/yaml.v2"
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

func parseConfig(filename string) (*agent.ServerConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config agent.ServerConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func main() {
	var opt_listen string
	var opt_config string

	flag.StringVar(&opt_config, "config", "/tmp/plakar.yaml", "config file")
	flag.StringVar(&opt_listen, "listen", ":8081", "listen address")
	flag.Parse()

	config, err := parseConfig(opt_config)
	if err != nil {
		log.Fatalf("Could not parse config: %s\n", err.Error())
	}

	srv = agent.NewServer(opt_listen, PublicKey, PrivateKey, config)
	srv.ListenAndServe()
}
