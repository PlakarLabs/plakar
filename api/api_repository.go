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
	sortKeys, err := header.ParseSortKeys(r.URL.Query().Get("sort"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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

	header.SortHeaders(headers, sortKeys)

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
