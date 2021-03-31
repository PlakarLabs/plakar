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

package ui

import (
	_ "embed"
	"fmt"
	"html/template"
	"math/rand"
	"net/http"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	"github.com/gorilla/mux"
	"github.com/poolpOrg/plakar/storage"
)

var lstore storage.Store

//go:embed base.tmpl
var baseTemplate string

//go:embed store.tmpl
var storeTemplate string

//go:embed snapshot.tmpl
var snapshotTemplate string

//go:embed browse.tmpl
var browseTemplate string

//go:embed object.tmpl
var objectTemplate string

//go:embed search.tmpl
var searchTemplate string

var templates map[string]*template.Template

func viewStore(w http.ResponseWriter, r *http.Request) {
	snapshots, _ := lstore.Snapshots()

	snapshotsList := make([]*storage.Snapshot, 0)
	for _, id := range snapshots {
		snapshot, err := lstore.Snapshot(id)
		if err != nil {
			/* failed to lookup snapshot */
			continue
		}
		snapshotsList = append(snapshotsList, snapshot)
	}

	sort.Slice(snapshotsList, func(i, j int) bool {
		return snapshotsList[i].CreationTime.Before(snapshotsList[j].CreationTime)
	})

	res := make([]*storage.SnapshotSummary, 0)
	for _, snapshot := range snapshotsList {
		res = append(res, storage.SnapshotToSummary(snapshot))
	}

	ctx := &struct {
		Store     storage.StoreConfig
		Snapshots []*storage.SnapshotSummary
	}{
		lstore.Configuration(),
		res,
	}

	templates["store"].Execute(w, ctx)
}

func snapshot(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]

	snapshot, err := lstore.Snapshot(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	roots := make(map[string]struct{})
	for directory, _ := range snapshot.Directories {
		tmp := strings.Split(directory, "/")
		root := ""
		for i := 0; i < len(tmp); i++ {
			if i == 0 {
				root = tmp[i]
			} else {
				root = strings.Join([]string{root, tmp[i]}, "/")
			}
			if _, ok := snapshot.Directories[root+"/"]; ok {
				roots[root] = struct{}{}
				break
			}
		}
	}
	rootsList := make([]string, 0)
	for root, _ := range roots {
		rootsList = append(rootsList, root)
	}
	sort.Slice(rootsList, func(i, j int) bool {
		return strings.Compare(rootsList[i], rootsList[j]) < 0
	})

	ctx := &struct {
		Snapshot *storage.Snapshot
		Roots    []string
	}{snapshot, rootsList}

	templates["snapshot"].Execute(w, ctx)
}

func browse(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	snapshot, err := lstore.Snapshot(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, ok := snapshot.Directories[path]
	if !ok {
		_, ok := snapshot.Directories[path+"/"]
		if !ok {
			http.Error(w, "", http.StatusNotFound)
			return
		}
	}

	directories := make([]*storage.FileInfo, 0)
	for directory, fi := range snapshot.Directories {
		if directory == path+"/" {
			continue
		}
		if !strings.HasPrefix(directory, path) {
			continue
		}

		if strings.Count(directory[len(path):], "/") != 1 {
			continue
		}
		directories = append(directories, fi)
	}
	sort.Slice(directories, func(i, j int) bool {
		return strings.Compare(directories[i].Name, directories[j].Name) < 0
	})

	files := make([]*storage.FileInfo, 0)
	for file, fi := range snapshot.Files {
		if !strings.HasPrefix(file, path) {
			continue
		}

		if strings.Count(file[len(path):], "/") != 1 {
			continue
		}

		files = append(files, fi)
	}
	sort.Slice(files, func(i, j int) bool {
		return strings.Compare(files[i].Name, files[j].Name) < 0
	})

	root := ""
	for _, atom := range strings.Split(path, "/") {
		root = root + atom + "/"
		if _, ok := snapshot.Directories[root]; ok {
			break
		}
	}

	nav := make([]string, 0)
	nav = append(nav, root[:len(root)-1])
	buf := ""
	for _, atom := range strings.Split(path, "/")[1:] {
		buf = buf + atom + "/"
		if len(buf) > len(root) {
			nav = append(nav, atom)
		}
	}

	ctx := &struct {
		Snapshot    *storage.Snapshot
		Directories []*storage.FileInfo
		Files       []*storage.FileInfo
		Root        string
		Path        string
		Navigation  []string
	}{snapshot, directories, files, root, path, nav}
	templates["browse"].Execute(w, ctx)

}

func object(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	snapshot, err := lstore.Snapshot(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	checksum, ok := snapshot.Sums[path]
	if !ok {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	object := snapshot.Objects[checksum]
	info := snapshot.Files[path]

	root := ""
	for _, atom := range strings.Split(path, "/") {
		root = root + atom + "/"
		if _, ok := snapshot.Directories[root]; ok {
			break
		}
	}

	nav := make([]string, 0)
	nav = append(nav, root[:len(root)-1])
	buf := ""
	for _, atom := range strings.Split(path, "/")[1:] {
		buf = buf + atom + "/"
		if len(buf) > len(root) {
			nav = append(nav, atom)
		}
	}

	enableViewer := false
	if strings.HasPrefix(object.ContentType, "text/") ||
		strings.HasPrefix(object.ContentType, "image/") ||
		strings.HasPrefix(object.ContentType, "audio/") ||
		strings.HasPrefix(object.ContentType, "video/") ||
		object.ContentType == "application/pdf" {
		enableViewer = true
	}

	ctx := &struct {
		Snapshot     *storage.Snapshot
		Object       *storage.Object
		Info         *storage.FileInfo
		Root         string
		Path         string
		Navigation   []string
		EnableViewer bool
	}{snapshot, object, info, root, path, nav, enableViewer}
	templates["object"].Execute(w, ctx)
}

func raw(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	snapshot, err := lstore.Snapshot(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	checksum, ok := snapshot.Sums[path]
	if !ok {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	object := snapshot.Objects[checksum]

	w.Header().Add("Content-Type", object.ContentType)
	for _, chunk := range object.Chunks {
		data, err := snapshot.ChunkGet(chunk.Checksum)
		if err != nil {
		}
		w.Write(data)
	}
	return
}

func search_snapshots(w http.ResponseWriter, r *http.Request) {
	urlParams := r.URL.Query()
	q := urlParams["q"][0]

	snapshots, err := lstore.Snapshots()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	snapshotsList := make([]*storage.Snapshot, 0)
	for _, id := range snapshots {
		snapshot, err := lstore.Snapshot(id)
		if err != nil {
			/* failed to lookup snapshot */
			continue
		}
		snapshotsList = append(snapshotsList, snapshot)
	}
	sort.Slice(snapshotsList, func(i, j int) bool {
		return snapshotsList[i].CreationTime.Before(snapshotsList[j].CreationTime)
	})

	directories := make([]struct {
		Snapshot string
		Path     string
	}, 0)
	files := make([]struct {
		Snapshot string
		Path     string
	}, 0)
	for _, snapshot := range snapshotsList {
		for directory := range snapshot.Directories {
			if strings.Contains(directory, q) {
				directories = append(directories, struct {
					Snapshot string
					Path     string
				}{snapshot.Uuid, directory})
			}
		}
		for file := range snapshot.Sums {
			if strings.Contains(file, q) {
				files = append(files, struct {
					Snapshot string
					Path     string
				}{snapshot.Uuid, file})
			}
		}
	}
	sort.Slice(directories, func(i, j int) bool {
		return strings.Compare(directories[i].Path, directories[j].Path) < 0
	})
	sort.Slice(files, func(i, j int) bool {
		return strings.Compare(files[i].Path, files[j].Path) < 0
	})

	ctx := &struct {
		SearchTerms string
		Directories []struct {
			Snapshot string
			Path     string
		}
		Files []struct {
			Snapshot string
			Path     string
		}
	}{q, directories, files}
	templates["search"].Execute(w, ctx)
}

func Ui(store storage.Store) {
	lstore = store

	templates = make(map[string]*template.Template)

	t, err := template.New("store").Parse(baseTemplate + storeTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("snapshot").Parse(baseTemplate + snapshotTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("browse").Parse(baseTemplate + browseTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("object").Parse(baseTemplate + objectTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("search").Parse(baseTemplate + searchTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	port := rand.Uint32() % 0xffff
	fmt.Println("Launched UI on port", port)

	url := fmt.Sprintf("http://localhost:%d", port)
	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	_ = err

	r := mux.NewRouter()
	r.HandleFunc("/", viewStore)
	r.HandleFunc("/snapshot/{snapshot}", snapshot)
	r.HandleFunc("/snapshot/{snapshot}:{path:.+}/", browse)
	r.HandleFunc("/raw/{snapshot}:{path:.+}", raw)
	r.HandleFunc("/snapshot/{snapshot}:{path:.+}", object)

	r.HandleFunc("/search", search_snapshots)

	http.ListenAndServe(fmt.Sprintf(":%d", port), r)
}
