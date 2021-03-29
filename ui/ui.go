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

	templates["snapshots"].Execute(w, &struct{ Snapshots []*repository.SnapshotSummary }{res})
}

func snapshot(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	fmt.Println(id)
	snapshot, err := lstore.Snapshot(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	templates["snapshot"].Execute(w, snapshot)
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
	r.HandleFunc("/snapshot/{snapshot}", snapshot)

	http.ListenAndServe(fmt.Sprintf(":%d", port), r)
}
