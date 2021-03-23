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
	snapshots := lstore.Snapshots()

	ids := make([]string, 0)
	for id, _ := range snapshots {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return snapshots[ids[i]].ModTime().Before(snapshots[ids[j]].ModTime())
	})

	res := make([]*repository.SnapshotSummary, 0)
	for _, id := range ids {
		snapshot, err := lstore.Snapshot(id)
		if err == nil {
			res = append(res, repository.SnapshotToSummary(snapshot))
		}
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

	r := mux.NewRouter()
	r.HandleFunc("/", snapshots)
	r.HandleFunc("/snapshot/{snapshot}", snapshot)

	http.ListenAndServe(fmt.Sprintf(":%d", port), r)
}
