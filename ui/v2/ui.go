/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package v2

import (
	"embed"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os/exec"
	"runtime"
	"strings"

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

var lrepository *storage.Repository

//go:embed frontend/build/*
var content embed.FS

func getConfigHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("received get config request")

	var res network.ResOpen
	config := lrepository.Configuration()
	res.RepositoryConfig = &config
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getSnapshotsHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("received get snapshots request")

	snapshotsIDs, err := lrepository.GetSnapshots()

	var res network.ResGetSnapshots
	res.Snapshots = snapshotsIDs
	res.Err = err
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func Ui(repository *storage.Repository, addr string, spawn bool) error {
	lrepository = repository

	var url string
	if addr != "" {
		url = fmt.Sprintf("http://%s", addr)
	} else {
		var port uint16
		for {
			port = uint16(rand.Uint32() % 0xffff)
			if port >= 1024 {
				break
			}
		}
		addr = fmt.Sprintf("localhost:%d", port)
		url = fmt.Sprintf("http://%s", addr)
	}
	var err error
	fmt.Println("lauching browser UI pointing at", url)
	if spawn {
		switch runtime.GOOS {
		case "windows":
			err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
		case "darwin":
			err = exec.Command("open", url).Start()
		default: // "linux", "freebsd", "openbsd", "netbsd"
			err = exec.Command("xdg-open", url).Start()
		}
		if err != nil {
			return err
		}
	}

	r := mux.NewRouter()

	r.PathPrefix("/api/config").HandlerFunc(getConfigHandler).Methods("GET")
	r.PathPrefix("/api/snapshots").HandlerFunc(getSnapshotsHandler).Methods("GET")

	r.PathPrefix("/static/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip the "/static/" prefix from the request path
		httpPath := r.URL.Path

		// Read the file from the embedded content
		data, err := content.ReadFile("frontend/build" + httpPath)
		if err != nil {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}

		// Determine the content type based on the file extension
		contentType := ""
		switch {
		case strings.HasSuffix(httpPath, ".css"):
			contentType = "text/css"
		case strings.HasSuffix(httpPath, ".js"):
			contentType = "application/javascript"
		case strings.HasSuffix(httpPath, ".png"):
			contentType = "image/png"
		case strings.HasSuffix(httpPath, ".jpg"), strings.HasSuffix(httpPath, ".jpeg"):
			contentType = "image/jpeg"
			// Add more content types as needed
		}

		// Set the content type header
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}

		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})

	r.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := content.ReadFile("frontend/build/index.html")
		if err != nil {
			http.Error(w, "App not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})

	return http.ListenAndServe(addr, handlers.CORS()(r))
}
