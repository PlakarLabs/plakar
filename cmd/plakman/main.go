package main

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/PlakarLabs/plakar/agent/server"
)

var uptime = time.Now()
var publicKey, privateKey []byte = nil, nil
var srv *server.Server

func init() {
	_publicKey, _privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		log.Fatalf("Could not generate keys: %s\n", err.Error())
	}
	publicKey = _publicKey
	privateKey = _privateKey
}

type GetStatsResponse struct {
	PublicKey []byte         `json:"public_key"`
	Uptime    time.Time      `json:"uptime"`
	Agents    []server.Agent `json:"agents"`
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	var response GetStatsResponse
	response.PublicKey = publicKey
	response.Uptime = uptime
	response.Agents = srv.Agents()

	json.NewEncoder(w).Encode(&response)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
}

type GetAgentsResponse struct {
	Agents []server.Agent `json:"agents"`
}

func agentsHandler(w http.ResponseWriter, r *http.Request) {
	var response GetAgentsResponse
	response.Agents = srv.Agents()

	json.NewEncoder(w).Encode(&response)
}

func main() {
	srv = server.NewServer(":8081", publicKey, privateKey)
	go srv.Run()

	// define routes and handlers
	http.HandleFunc("/", statsHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/agents", agentsHandler)

	// start the server
	port := ":8080"
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Could not start server: %s\n", err.Error())
	}
}
