package api

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/gorilla/mux"
)

func repositoryConfiguration(w http.ResponseWriter, r *http.Request) {
	configuration := lrepository.Configuration()
	json.NewEncoder(w).Encode(configuration)
}

func repositorySnapshots(w http.ResponseWriter, r *http.Request) {
	var err error
	var sortKeys []string
	var offset int64
	var limit int64
	var reversed bool

	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")
	orderStr := r.URL.Query().Get("order")

	sortKeysStr := r.URL.Query().Get("sort")
	if sortKeysStr == "" {
		sortKeysStr = "CreationTime"
	}

	sortKeys, err = header.ParseSortKeys(sortKeysStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else if offset < 0 {
			http.Error(w, "Invalid offset", http.StatusBadRequest)
			return
		}
	}
	if limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else if limit < 0 {
			http.Error(w, "Invalid limit", http.StatusBadRequest)
			return
		}
	}

	reversed = false
	if orderStr != "" {
		if orderStr != "asc" && orderStr != "desc" {
			http.Error(w, "Invalid order", http.StatusBadRequest)
			return
		}
		if orderStr == "desc" {
			reversed = true
		}
	}

	snapshotIDs, err := lrepository.GetSnapshots()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	headers := make([]header.Header, 0, len(snapshotIDs))
	for _, snapshotID := range snapshotIDs {
		snap, err := snapshot.Load(lrepository, snapshotID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		headers = append(headers, *snap.Header)
	}

	if limit == 0 {
		limit = int64(len(headers))
	}

	header.SortHeaders(headers, sortKeys, reversed)
	if offset > int64(len(headers)) {
		headers = []header.Header{}
	} else if offset+limit > int64(len(headers)) {
		headers = headers[offset:]
	} else {
		headers = headers[offset : offset+limit]
	}

	err = json.NewEncoder(w).Encode(headers)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func repositoryStates(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_ = vars

	states, err := lrepository.GetStates()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(states)
}

func repositoryState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	stateID := vars["state"]

	stateBytes, err := hex.DecodeString(stateID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(stateBytes) != 32 {
		http.Error(w, "Invalid state ID", http.StatusBadRequest)
		return
	}

	stateBytes32 := [32]byte{}
	copy(stateBytes32[:], stateBytes)

	buffer, _, err := lrepository.GetState(stateBytes32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(buffer)
}

func repositoryPackfiles(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_ = vars

	packfiles, err := lrepository.GetPackfiles()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(packfiles)
}

func repositoryPackfile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	packfileIDStr := vars["packfile"]
	offsetStr, offsetExists := vars["offset"]
	lengthStr, lengthExists := vars["length"]

	packfileBytes, err := hex.DecodeString(packfileIDStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(packfileBytes) != 32 {
		http.Error(w, "Invalid packfile ID", http.StatusBadRequest)
		return
	}

	if (offsetExists && !lengthExists) || (!offsetExists && lengthExists) {
		http.Error(w, "Invalid packfile range", http.StatusBadRequest)
		return
	}

	packfileBytes32 := [32]byte{}
	copy(packfileBytes32[:], packfileBytes)

	var rd io.Reader
	if offsetExists && lengthExists {
		offset, err := strconv.ParseUint(offsetStr, 10, 32)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		length, err := strconv.ParseUint(lengthStr, 10, 32)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rd, _, err = lrepository.GetPackfileBlob(packfileBytes32, uint32(offset), uint32(length))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		rd, _, err = lrepository.GetPackfile(packfileBytes32)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	io.Copy(w, rd)
}
