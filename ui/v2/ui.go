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
	"crypto/sha256"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/snapshot/header"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

var lrepository *storage.Repository

var lcache *snapshot.Snapshot
var lcacheMtx sync.Mutex

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

type ResGetSnapshots struct {
	Page            uint64            `json:"page"`
	PageSize        uint64            `json:"pageSize"`
	TotalItems      uint64            `json:"totalItems"`
	TotalPages      uint64            `json:"totalPages"`
	HasPreviousPage bool              `json:"hasPreviousPage"`
	HasNextPage     bool              `json:"hasNextPage"`
	Snapshot        string            `json:"snapshot"`
	Path            string            `json:"path"`
	Items           []SnapshotSummary `json:"items"`
}

type SnapshotSummary struct {
	ID        string   `json:"id"`
	ShortID   string   `json:"shortId"`
	Username  string   `json:"username"`
	Hostname  string   `json:"hostName"`
	Location  string   `json:"location"`
	RootPath  string   `json:"rootPath"`
	Date      string   `json:"date"`
	Size      string   `json:"size"`
	Tags      []string `json:"tags"`
	Os        string   `json:"os"`
	Signature string   `json:"signature"`
}

func getSnapshotsHandler(w http.ResponseWriter, r *http.Request) {
	//fmt.Println("received get snapshots request")

	snapshotsIDs, err := lrepository.GetSnapshots()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	headers := make([]header.Header, 0)
	for _, snapshotID := range snapshotsIDs {
		header, _, err := snapshot.GetSnapshot(lrepository, snapshotID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		headers = append(headers, *header)
	}
	sort.Slice(headers, func(i, j int) bool {
		return headers[i].CreationTime.After(headers[j].CreationTime)
	})

	var offsetStr, limitStr string
	if offsetStr = r.URL.Query().Get("offset"); offsetStr == "" {
		offsetStr = "0"
	}
	if limitStr = r.URL.Query().Get("limit"); limitStr == "" {
		limitStr = "10"
	}

	var offset, limit int64
	if offset, err = strconv.ParseInt(offsetStr, 10, 64); err != nil || offset < 0 {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if limit, err = strconv.ParseInt(limitStr, 10, 64); err != nil || limit <= 0 {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if offset >= int64(len(snapshotsIDs)) {
		offset = 0
	}
	if limit == 0 {
		limit = 10
	}

	var res ResGetSnapshots
	res.Page = uint64(offset)
	res.PageSize = uint64(limit)
	res.TotalItems = uint64(len(snapshotsIDs))
	res.TotalPages = uint64(len(snapshotsIDs)) / uint64(limit)
	if uint64(len(snapshotsIDs))%uint64(limit) != 0 {
		res.TotalPages++
	}
	res.HasPreviousPage = false
	res.HasNextPage = false
	res.Snapshot = ""
	res.Path = ""
	res.Items = []SnapshotSummary{}

	begin := uint64(offset)
	end := uint64(offset) + uint64(limit)
	if end >= uint64(len(snapshotsIDs)) {
		end = uint64(len(snapshotsIDs))
	}

	for _, index := range headers[begin:end] {
		SnapshotSummary := SnapshotSummary{
			ID:        index.IndexID.String(),
			ShortID:   index.GetIndexShortID(),
			Username:  index.Username,
			Hostname:  index.Hostname,
			RootPath:  index.ScannedDirectories[0],
			Date:      index.CreationTime.UTC().Format(time.RFC3339),
			Size:      humanize.Bytes(index.ScanSize),
			Tags:      index.Tags,
			Os:        index.OperatingSystem,
			Signature: "",
		}

		res.Items = append(res.Items, SnapshotSummary)
	}

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type ResGetSnapshot struct {
	Page            uint64               `json:"page"`
	PageSize        uint64               `json:"pageSize"`
	TotalItems      uint64               `json:"totalItems"`
	TotalPages      uint64               `json:"totalPages"`
	HasPreviousPage bool                 `json:"hasPreviousPage"`
	HasNextPage     bool                 `json:"hasNextPage"`
	Snapshot        SnapshotSummary      `json:"snapshot"`
	Path            string               `json:"path"`
	Items           []ResGetSnapshotItem `json:"items"`
}

type ResGetSnapshotItem struct {
	Name          string `json:"name"`
	DirectoryPath string `json:"directoryPath"`
	Path          string `json:"path"`
	RawPath       string `json:"rawPath"`
	MimeType      string `json:"mimeType"`
	IsDir         bool   `json:"isDirectory"`
	Mode          string `json:"mode"`
	Uid           string `json:"uid"`
	Gid           string `json:"gid"`
	Mtime         string `json:"modificationTime"`
	Size          string `json:"size"`
	ByteSize      uint64 `json:"byteSize"`
	Checksum      string `json:"checksum"`
	Device        string `json:"device"`
	Inode         string `json:"inode"`
}

func getSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	//fmt.Println("received get snapshot request", id, path)

	lcacheMtx.Lock()
	if lcache == nil || lcache.Header.IndexID.String() != id {
		tmp, err := snapshot.Load(lrepository, uuid.MustParse(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			lcacheMtx.Unlock()
			return
		}
		lcache = tmp
	}
	currSnapshot := lcache
	lcacheMtx.Unlock()

	header := currSnapshot.Header

	var res ResGetSnapshot
	res.Snapshot = SnapshotSummary{
		ID:        header.IndexID.String(),
		ShortID:   header.GetIndexShortID(),
		Username:  header.Username,
		Hostname:  header.Hostname,
		RootPath:  header.ScannedDirectories[0],
		Date:      header.CreationTime.UTC().Format(time.RFC3339),
		Size:      humanize.Bytes(header.ScanSize),
		Tags:      header.Tags,
		Os:        header.OperatingSystem,
		Signature: "",
	}
	res.Path = path
	res.Items = []ResGetSnapshotItem{}

	children, err := currSnapshot.Filesystem.LookupChildren(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var offsetStr, limitStr string
	if offsetStr = r.URL.Query().Get("offset"); offsetStr == "" {
		offsetStr = "0"
	}
	if limitStr = r.URL.Query().Get("limit"); limitStr == "" {
		limitStr = "10"
	}

	var offset, limit uint64
	if offset, err = strconv.ParseUint(offsetStr, 10, 64); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if limit, err = strconv.ParseUint(limitStr, 10, 64); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if offset >= uint64(len(children)) {
		offset = 0
	}
	if limit == 0 {
		limit = 10
	}

	begin := offset
	end := offset + limit
	if end >= uint64(len(children)) {
		end = uint64(len(children))
	}
	res.Page = offset
	res.PageSize = limit
	res.TotalItems = uint64(len(children))
	res.TotalPages = uint64(len(children)) / limit
	if uint64(len(children))%limit != 0 {
		res.TotalPages++
	}
	res.HasPreviousPage = false
	res.HasNextPage = false

	for _, entry := range children[begin:end] {
		st, err := currSnapshot.Filesystem.Lookup(filepath.Join(path, entry))
		if err != nil {
			continue
		}

		ResGetSnapshotItem := ResGetSnapshotItem{
			Name:  entry,
			IsDir: st.Inode.IsDir(),
			Mode:  st.Inode.Lmode.String(),
			Uid:   fmt.Sprintf("%d", st.Inode.Uid()),
			Gid:   fmt.Sprintf("%d", st.Inode.Gid()),
			Mtime: st.Inode.ModTime().UTC().Format(time.RFC3339),
			Size:  humanize.Bytes(uint64(st.Inode.Size())),
		}
		if !ResGetSnapshotItem.IsDir {
			pathChecksum := sha256.Sum256([]byte(filepath.Join(path, entry)))
			object := currSnapshot.Index.LookupObjectForPathnameChecksum(pathChecksum)
			if object != nil {
				mimeType, _ := currSnapshot.Metadata.LookupKeyForValue(object.Checksum)
				if mimeType != "" {
					ResGetSnapshotItem.MimeType = strings.Split(mimeType, ";")[0]
				} else if mimeType == "" {
					object.ContentType, _ = currSnapshot.Metadata.LookupKeyForValue(object.Checksum)

					contentType := mime.TypeByExtension(filepath.Ext(entry))
					if contentType == "" {
						contentType = object.ContentType
					}

					if contentType == "application/x-tex" {
						contentType = "text/plain"
					}

					ResGetSnapshotItem.MimeType = contentType

				}
				//fmt.Println("mime: [", ResGetSnapshotItem.MimeType, "]")
				ResGetSnapshotItem.Checksum = fmt.Sprintf("%064x", object.Checksum)
			}

			ResGetSnapshotItem.Path = filepath.Join(id+":"+path, entry)
			ResGetSnapshotItem.DirectoryPath = id + ":" + path
			ResGetSnapshotItem.RawPath = fmt.Sprintf("/api/raw/%s:%s", id, filepath.Join(path, entry))
			ResGetSnapshotItem.ByteSize = uint64(st.Inode.Size())

		} else {
			ResGetSnapshotItem.Path = filepath.Join(id+":"+path, entry, "") + "/"
		}
		ResGetSnapshotItem.Device = fmt.Sprintf("%d", st.Inode.Dev())
		ResGetSnapshotItem.Inode = fmt.Sprintf("%d", st.Inode.Ino())

		//fmt.Println("adding", ResGetSnapshotItem)
		res.Items = append(res.Items, ResGetSnapshotItem)
	}

	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func getRawHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]
	download := r.URL.Query().Get("download")

	lcacheMtx.Lock()
	if lcache == nil || lcache.Header.IndexID.String() != id {
		tmp, err := snapshot.Load(lrepository, uuid.MustParse(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			lcacheMtx.Unlock()
			return
		}
		lcache = tmp
	}
	snap := lcache
	lcacheMtx.Unlock()

	var mimeType string

	pathChecksum := sha256.Sum256([]byte(path))
	object := snap.Index.LookupObjectForPathnameChecksum(pathChecksum)
	if object != nil {
		mimeType, _ = snap.Metadata.LookupKeyForValue(object.Checksum)
		if mimeType != "" {
			mimeType = strings.Split(mimeType, ";")[0]
		} else {
			object.ContentType, _ = snap.Metadata.LookupKeyForValue(object.Checksum)

			contentType := mime.TypeByExtension(filepath.Ext(path))
			if contentType == "" {
				contentType = object.ContentType
			}

			if contentType == "application/x-tex" {
				contentType = "text/plain"
			}

			mimeType = contentType
		}
		//fmt.Println("mime:", mimeType)
	}

	//fmt.Println("mime: [", ResGetSnapshotItem.MimeType, "]")
	//ResGetSnapshotItem.Checksum = fmt.Sprintf("%064x", object.Checksum)

	rd, err := snapshot.NewReader(snap, path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", mimeType)
	if download != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filepath.Base(path)))
	}
	io.Copy(w, rd)
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
	r.PathPrefix("/api/snapshot/{snapshot}:{path:.+}/").HandlerFunc(getSnapshotHandler).Methods("GET")
	r.PathPrefix("/api/snapshot/{snapshot}:/").HandlerFunc(getSnapshotHandler).Methods("GET")

	r.PathPrefix("/api/raw/{snapshot}:{path:.+}").HandlerFunc(getRawHandler).Methods("GET")

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
