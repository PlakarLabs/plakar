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
	"io/fs"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/PlakarKorp/plakar/api"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/gorilla/handlers"
)

type UiOptions struct {
	MaxConcurrency uint64
	NoSpawn        bool
	Cors           bool
	Token          string
}

//go:embed frontend/*
var content embed.FS

func Ui(repo *repository.Repository, addr string, opts *UiOptions) error {
	r := api.NewRouter(repo, opts.Token)

	// Serve files from the ./frontend directory
	r.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Join internally call path.Clean to prevent directory traversal
		path := filepath.Join("frontend", r.URL.Path)

		_, err := content.Open(path)
		if os.IsNotExist(err) {
			// File does not exist, serve index.html
			index, err := content.ReadFile(filepath.Join("frontend", "index.html"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write(index)
			return
		} else if err != nil {
			// If we got an error (that wasn't that the file doesn't exist) stating the
			// file, return a 500 internal server error and stop
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		statics, err := fs.Sub(content, "frontend")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.FileServer(http.FS(statics)).ServeHTTP(w, r)
	})

	if addr == "" {
		var port uint16
		for {
			port = uint16(rand.Uint32() % 0xffff)
			if port >= 1024 {
				break
			}
		}
		addr = fmt.Sprintf("localhost:%d", port)
	}

	var url string
	if opts.Token == "" {
		url = fmt.Sprintf("http://%s", addr)
	} else {
		url = fmt.Sprintf("http://%s?token=%s", addr, opts.Token)
	}

	var err error
	if !opts.NoSpawn {
		switch runtime.GOOS {
		case "windows":
			err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
		case "darwin":
			err = exec.Command("open", url).Start()
		default: // "linux", "freebsd", "openbsd", "netbsd"
			err = exec.Command("xdg-open", url).Start()
		}
		if err != nil {
			logging.Printf("failed to launch browser: %s", err)
			logging.Printf("you can access the webUI at %s", url)
			return err
		}
	} else {
		fmt.Println("lauching webUI at", url)
	}

	if opts.Cors {
		return http.ListenAndServe(addr, handlers.CORS(
			handlers.AllowedHeaders([]string{"Authorization", "Content-Type"}),
		)(r))
	}
	return http.ListenAndServe(addr, r)
}
