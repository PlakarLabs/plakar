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

func repositoryConfiguration(w http.ResponseWriter, r *http.Request) error {
	configuration := lrepository.Configuration()
	return json.NewEncoder(w).Encode(configuration)
}

func repositorySnapshots(w http.ResponseWriter, r *http.Request) error {
	var err error
	var sortKeys []string
	var offset int64
	var limit int64

	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	sortKeysStr := r.URL.Query().Get("sort")
	if sortKeysStr == "" {
		sortKeysStr = "CreationTime"
	}

	sortKeys, err = header.ParseSortKeys(sortKeysStr)
	if err != nil {
		return parameterError("sort", InvalidArgument, err)
	}

	if offsetStr != "" {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return parameterError("offset", BadNumber, err)
		} else if offset < 0 {
			return parameterError("offset", BadNumber, ErrNegativeNumber)
		}
	}
	if limitStr != "" {
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			return parameterError("limit", BadNumber, err)
		} else if limit < 0 {
			return parameterError("limit", BadNumber, ErrNegativeNumber)
		}
	}

	lrepository.RebuildState()

	snapshotIDs, err := lrepository.GetSnapshots()
	if err != nil {
		return err
	}

	headers := make([]header.Header, 0, len(snapshotIDs))
	for _, snapshotID := range snapshotIDs {
		snap, err := snapshot.Load(lrepository, snapshotID)
		if err != nil {
			return err
		}
		headers = append(headers, *snap.Header)
	}

	if limit == 0 {
		limit = int64(len(headers))
	}

	header.SortHeaders(headers, sortKeys)
	if offset > int64(len(headers)) {
		headers = []header.Header{}
	} else if offset+limit > int64(len(headers)) {
		headers = headers[offset:]
	} else {
		headers = headers[offset : offset+limit]
	}

	items := Items{
		Total: len(snapshotIDs),
		Items: make([]interface{}, len(headers)),
	}
	for i, header := range headers {
		items.Items[i] = header
	}

	return json.NewEncoder(w).Encode(items)
}

func repositoryStates(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	_ = vars

	states, err := lrepository.GetStates()
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

func repositoryState(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	stateID := vars["state"]

	stateBytes, err := hex.DecodeString(stateID)
	if err != nil {
		return parameterError("state", InvalidArgument, err)
	}
	if len(stateBytes) != 32 {
		return parameterError("state", InvalidArgument, ErrInvalidID)
	}

	stateBytes32 := [32]byte{}
	copy(stateBytes32[:], stateBytes)

	buffer, _, err := lrepository.GetState(stateBytes32)
	if err != nil {
		return err
	}

	_, err = w.Write(buffer)
	return err
}

func repositoryPackfiles(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	_ = vars

	packfiles, err := lrepository.GetPackfiles()
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

func repositoryPackfile(w http.ResponseWriter, r *http.Request) error {
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
		rd, _, err = lrepository.GetPackfileBlob(packfileBytes32, uint32(offset), uint32(length))
		if err != nil {
			return err
		}
	} else {
		rd, _, err = lrepository.GetPackfile(packfileBytes32)
		if err != nil {
			return err
		}
	}
	_, err = io.Copy(w, rd)
	return err
}
