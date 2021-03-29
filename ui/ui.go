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
	"log"
	"math/rand"
	"net/http"
	"os/exec"
	"runtime"
	"sort"
	"strings"

	"github.com/gorilla/mux"
	"github.com/poolpOrg/plakar/repository"
)

var lstore repository.Store

//go:embed base.tmpl
var baseTemplate string

//go:embed snapshots.tmpl
var snapshotsTemplate string

//go:embed snapshot.tmpl
var snapshotTemplate string

var templates map[string]*template.Template

func snapshots(w http.ResponseWriter, r *http.Request) {
	snapshots, _ := lstore.Snapshots()

	snapshotsList := make([]*repository.Snapshot, 0)
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

	res := make([]*repository.SnapshotSummary, 0)
	for _, snapshot := range snapshotsList {
		res = append(res, repository.SnapshotToSummary(snapshot))
	}

	ctx := &struct{ Snapshots []*repository.SnapshotSummary }{res}

	templates["snapshots"].Execute(w, ctx)
}

func snapshot(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	snapshot, err := lstore.Snapshot(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	file, ok := snapshot.Sums[path]
	if ok {
		fmt.Println("FILE: ", path, file)
		templates["snapshot"].Execute(w, snapshot)
		return
	}

	_, ok = snapshot.Directories[path]
	if !ok && path != "/" {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	directories := make([]*repository.FileInfo, 0)
	for directory, fi := range snapshot.Directories {
		if directory == path {
			continue
		}
		if !strings.HasPrefix(directory, path) {
			continue
		}

		if strings.Count(directory[len(path):], "/") != 1 {
			fmt.Println("skipping")
			continue
		}
		directories = append(directories, fi)
	}
	sort.Slice(directories, func(i, j int) bool {
		return strings.Compare(directories[i].Name, directories[j].Name) < 0
	})

	files := make([]*repository.FileInfo, 0)
	for file, fi := range snapshot.Files {
		if !strings.HasPrefix(file, path) {
			continue
		}

		if strings.Count(file[len(path):], "/") != 0 {
			continue
		}

		files = append(files, fi)
	}
	sort.Slice(files, func(i, j int) bool {
		return strings.Compare(files[i].Name, files[j].Name) < 0
	})

	ctx := &struct {
		Path        string
		SplitPath   []string
		Snapshot    *repository.Snapshot
		Directories []*repository.FileInfo
		Files       []*repository.FileInfo
	}{path, strings.Split(path, "/"), snapshot, directories, files}

	templates["snapshot"].Execute(w, ctx)
}

func Ui(store repository.Store) {
	lstore = store

	templates = make(map[string]*template.Template)

	t, err := template.New("snapshots").Parse(baseTemplate + snapshotsTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("snapshot").Parse(baseTemplate + snapshotTemplate)
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
	if err != nil {
		log.Fatal(err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/", snapshots)
	r.HandleFunc("/snapshot/{snapshot}{path:.+}", snapshot)

	http.ListenAndServe(fmt.Sprintf(":%d", port), r)
}
