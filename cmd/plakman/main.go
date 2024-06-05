package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/PlakarLabs/plakar/agent/server"
)

// handler for the root route
func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to the Plakman Control Center!")
}

// handler for a health check route
func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
}

func agentsHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(server.Sessions())
}

func main() {
	go server.Server(":8081")

	// define routes and handlers
	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/agents", agentsHandler)

	// start the server
	port := ":8080"
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Could not start server: %s\n", err.Error())
	}
}
