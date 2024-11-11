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
	_ "embed"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os/exec"
	"runtime"
	"strings"

	"github.com/PlakarKorp/plakar/api"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/gorilla/handlers"
)

//go:embed frontend/*
var content embed.FS

func Ui(repo *repository.Repository, addr string, spawn bool, cors bool) error {

	r := api.NewRouter(repo)

	r.PathPrefix("/static/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Strip the "/static/" prefix from the request path
		httpPath := r.URL.Path

		// Read the file from the embedded content
		data, err := content.ReadFile("frontend" + httpPath)
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
		data, err := content.ReadFile("frontend/index.html")
		if err != nil {
			http.Error(w, "App not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})

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
	fmt.Println("lauching browser UI pointing at", url)
	var err error
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

	if cors {
		return http.ListenAndServe(addr, handlers.CORS()(r))
	}
	return http.ListenAndServe(addr, r)
}
