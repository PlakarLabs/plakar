package api

import (
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/gorilla/mux"
)

/* snapshot API */
/*
	GetStates() ([][32]byte, error)
//	GetState(checksum [32]byte) (io.Reader, uint64, error)
	GetState(checksum [32]byte) ([]byte, uint64, error)

	GetPackfiles() ([][32]byte, error)
	GetPackfile(checksum [32]byte) (io.Reader, uint64, error)
	GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) (io.Reader, uint32, error)
*/

func snapshotHeader(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	snapshotIDstr := vars["snapshot"]

	snapshotID, err := hex.DecodeString(snapshotIDstr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(snapshotID) != 32 {
		http.Error(w, "Invalid snapshot ID", http.StatusBadRequest)
		return
	}
	snapshotID32 := [32]byte{}
	copy(snapshotID32[:], snapshotID)

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(snap.Header)
}
