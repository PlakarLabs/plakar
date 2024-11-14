package api

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"

	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
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

func snapshotReader(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	snapshotIDstr := vars["snapshot"]
	path := vars["path"]

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

	rd, err := snap.NewReader(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if rd.GetContentType() != "" {
		w.Header().Set("Content-Type", rd.GetContentType())
	}

	_, err = io.Copy(w, rd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func snapshotVFSBrowse(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	snapshotIDstr := vars["snapshot"]
	path := vars["path"]

	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")
	orderStr := r.URL.Query().Get("order")

	var offset int64
	var limit int64
	var err error
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
	if orderStr != "" {
		if orderStr != "asc" && orderStr != "desc" {
			http.Error(w, "Invalid order", http.StatusBadRequest)
			return
		}
	} else {
		orderStr = "asc"
	}

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

	fs, err := snap.Filesystem()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if path == "" {
		path = "/"
	}
	fsinfo, err := fs.Stat(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if dirEntry, ok := fsinfo.(*vfs.DirEntry); ok {
		if orderStr == "desc" {
			sort.Slice(dirEntry.Children, func(i, j int) bool {
				return dirEntry.Children[i].FileInfo.Name() > dirEntry.Children[j].FileInfo.Name()
			})
		}

		if offset != 0 {
			if offset >= int64(len(dirEntry.Children)) {
				http.Error(w, "offset out of range", http.StatusBadRequest)
				return
			}
			dirEntry.Children = dirEntry.Children[offset:]
		}
		if limit != 0 {
			if limit >= int64(len(dirEntry.Children)) {
				limit = int64(len(dirEntry.Children))
			}
			dirEntry.Children = dirEntry.Children[:limit]
		}
		json.NewEncoder(w).Encode(dirEntry)
		return
	} else if fileEntry, ok := fsinfo.(*vfs.FileEntry); ok {
		json.NewEncoder(w).Encode(fileEntry)
		return
	} else {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
}
