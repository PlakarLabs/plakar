package api

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

func storageConfiguration(w http.ResponseWriter, r *http.Request) error {
	configuration := lstore.Configuration()
	return json.NewEncoder(w).Encode(configuration)
}

func storageStates(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	_ = vars

	states, err := lstore.GetStates()
	if err != nil {
		return err
	}

	items := Items{
		Total: len(states),
		Items: make([]interface{}, len(states)),
	}
	for i, state := range states {
		items.Items[i] = state
	}

	return json.NewEncoder(w).Encode(items)
}

func storageState(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	stateID := vars["state"]

	stateBytes, err := hex.DecodeString(stateID)
	if err != nil {
		return err
	}
	if len(stateBytes) != 32 {
		return parameterError("state", InvalidArgument, ErrInvalidID)
	}

	stateBytes32 := [32]byte{}
	copy(stateBytes32[:], stateBytes)

	rd, _, err := lstore.GetState(stateBytes32)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, rd)
	return err
}

func storagePackfiles(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	_ = vars

	packfiles, err := lstore.GetPackfiles()
	if err != nil {
		return err
	}

	items := Items{
		Total: len(packfiles),
		Items: make([]interface{}, len(packfiles)),
	}
	for i, packfile := range packfiles {
		items.Items[i] = packfile
	}

	return json.NewEncoder(w).Encode(items)
}

func storagePackfile(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	packfileIDStr := vars["packfile"]
	offsetStr, offsetExists := vars["offset"]
	lengthStr, lengthExists := vars["length"]

	packfileBytes, err := hex.DecodeString(packfileIDStr)
	if err != nil {
		return parameterError("packfile", InvalidArgument, err)
	}
	if len(packfileBytes) != 32 {
		return parameterError("packfile", InvalidArgument, ErrInvalidID)
	}

	if (offsetExists && !lengthExists) || (!offsetExists && lengthExists) {
		param := "offset"
		if !offsetExists {
			param = "length"
		}
		return parameterError(param, MissingArgument, ErrMissingField)
	}

	packfileBytes32 := [32]byte{}
	copy(packfileBytes32[:], packfileBytes)

	var rd io.Reader
	if offsetExists && lengthExists {
		offset, err := strconv.ParseUint(offsetStr, 10, 32)
		if err != nil {
			return parameterError("offset", InvalidArgument, err)
		}
		length, err := strconv.ParseUint(lengthStr, 10, 32)
		if err != nil {
			return parameterError("length", InvalidArgument, err)
		}
		rd, _, err = lstore.GetPackfileBlob(packfileBytes32, uint32(offset), uint32(length))
		if err != nil {
			return err
		}
	} else {
		rd, _, err = lstore.GetPackfile(packfileBytes32)
		if err != nil {
			return err
		}
	}
	_, err = io.Copy(w, rd)
	return err
}
