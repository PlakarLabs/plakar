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

	items := Items{
		Total: len(states),
		Items: make([]interface{}, len(states)),
	}
	for i, state := range states {
		items.Items[i] = state
	}

	json.NewEncoder(w).Encode(items)
}

func storageState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	stateID := vars["state"]

	stateBytes, err := hex.DecodeString(stateID)
	if err != nil {
		paramError(w, "state", InvalidArgument, err)
		return
	}
	if len(stateBytes) != 32 {
		paramError(w, "state", InvalidArgument, ErrInvalidID)
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

	items := Items{
		Total: len(packfiles),
		Items: make([]interface{}, len(packfiles)),
	}
	for i, packfile := range packfiles {
		items.Items[i] = packfile
	}

	json.NewEncoder(w).Encode(items)
}

func storagePackfile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	packfileIDStr := vars["packfile"]
	offsetStr, offsetExists := vars["offset"]
	lengthStr, lengthExists := vars["length"]

	packfileBytes, err := hex.DecodeString(packfileIDStr)
	if err != nil {
		paramError(w, "packfile", InvalidArgument, err)
		return
	}
	if len(packfileBytes) != 32 {
		paramError(w, "packfile", InvalidArgument, ErrInvalidID)
		return
	}

	if (offsetExists && !lengthExists) || (!offsetExists && lengthExists) {
		param := "offset"
		if !offsetExists {
			param = "length"
		}
		paramError(w, param, MissingArgument, ErrMissingField)
		return
	}

	packfileBytes32 := [32]byte{}
	copy(packfileBytes32[:], packfileBytes)

	var rd io.Reader
	if offsetExists && lengthExists {
		offset, err := strconv.ParseUint(offsetStr, 10, 32)
		if err != nil {
			paramError(w, "offset", InvalidArgument, err)
			return
		}
		length, err := strconv.ParseUint(lengthStr, 10, 32)
		if err != nil {
			paramError(w, "length", InvalidArgument, err)
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
