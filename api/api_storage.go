package api

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

func storageConfiguration(w http.ResponseWriter, r *http.Request) {
	configuration := lstore.Configuration()
	json.NewEncoder(w).Encode(configuration)
}

func storageStates(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_ = vars

	states, err := lstore.GetStates()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(states)
}

func storageState(w http.ResponseWriter, r *http.Request) {
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

	rd, _, err := lstore.GetState(stateBytes32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	io.Copy(w, rd)
}

func storagePackfiles(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_ = vars

	packfiles, err := lstore.GetPackfiles()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(packfiles)
}

func storagePackfile(w http.ResponseWriter, r *http.Request) {
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
		rd, _, err = lstore.GetPackfileBlob(packfileBytes32, uint32(offset), uint32(length))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		rd, _, err = lstore.GetPackfile(packfileBytes32)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	io.Copy(w, rd)
}
