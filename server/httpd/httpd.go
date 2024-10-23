package httpd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/gorilla/mux"
)

var lrepository *repository.Repository
var lNoDelete bool

func openRepository(w http.ResponseWriter, r *http.Request) {
	var reqOpen network.ReqOpen
	if err := json.NewDecoder(r.Body).Decode(&reqOpen); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	config := lrepository.Configuration()

	var resOpen network.ResOpen
	resOpen.Configuration = &config
	resOpen.Err = ""
	if err := json.NewEncoder(w).Encode(resOpen); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func closeRepository(w http.ResponseWriter, r *http.Request) {
	var reqClose network.ReqClose
	if err := json.NewDecoder(r.Body).Decode(&reqClose); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if reqClose.Uuid != lrepository.Configuration().StoreID.String() {
		http.Error(w, "UUID mismatch", http.StatusBadRequest)
		return
	}

	var resClose network.ResClose
	resClose.Err = ""
	if err := json.NewEncoder(w).Encode(resClose); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// states
func getStates(w http.ResponseWriter, r *http.Request) {
	var reqGetIndexes network.ReqGetStates
	if err := json.NewDecoder(r.Body).Decode(&reqGetIndexes); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetStates network.ResGetStates
	indexes, err := lrepository.GetStates()
	if err != nil {
		resGetStates.Err = err.Error()
	} else {
		resGetStates.Checksums = indexes
	}
	if err := json.NewEncoder(w).Encode(resGetStates); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putState(w http.ResponseWriter, r *http.Request) {
	var reqPutState network.ReqPutState
	if err := json.NewDecoder(r.Body).Decode(&reqPutState); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resPutIndex network.ResPutState
	data := reqPutState.Data
	datalen := int64(len(data))
	_, err := lrepository.PutState(reqPutState.Checksum, bytes.NewBuffer(data), datalen)
	if err != nil {
		resPutIndex.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(resPutIndex); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getState(w http.ResponseWriter, r *http.Request) {
	var reqGetState network.ReqGetState
	if err := json.NewDecoder(r.Body).Decode(&reqGetState); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetState network.ResGetState
	data, _, err := lrepository.GetState(reqGetState.Checksum)
	if err != nil {
		resGetState.Err = err.Error()
	} else {
		resGetState.Data = data
	}
	if err := json.NewEncoder(w).Encode(resGetState); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deleteState(w http.ResponseWriter, r *http.Request) {
	if lNoDelete {
		http.Error(w, fmt.Errorf("not allowed to delete").Error(), http.StatusForbidden)
		return
	}

	var reqDeleteState network.ReqDeleteState
	if err := json.NewDecoder(r.Body).Decode(&reqDeleteState); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resDeleteState network.ResDeleteState
	err := lrepository.DeleteState(reqDeleteState.Checksum)
	if err != nil {
		resDeleteState.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(resDeleteState); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// packfiles
func getPackfiles(w http.ResponseWriter, r *http.Request) {
	var reqGetPackfiles network.ReqGetPackfiles
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfiles); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetPackfiles network.ResGetPackfiles
	packfiles, err := lrepository.GetPackfiles()
	if err != nil {
		resGetPackfiles.Err = err.Error()
	} else {
		resGetPackfiles.Checksums = packfiles
	}
	if err := json.NewEncoder(w).Encode(resGetPackfiles); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putPackfile(w http.ResponseWriter, r *http.Request) {
	var reqPutPackfile network.ReqPutPackfile
	if err := json.NewDecoder(r.Body).Decode(&reqPutPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resPutPackfile network.ResPutPackfile
	err := lrepository.PutPackfile(reqPutPackfile.Checksum, bytes.NewBuffer(reqPutPackfile.Data), uint64(len(reqPutPackfile.Data)))
	if err != nil {
		resPutPackfile.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(resPutPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getPackfile(w http.ResponseWriter, r *http.Request) {
	var reqGetPackfile network.ReqGetPackfile
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetPackfile network.ResGetPackfile
	rd, _, err := lrepository.GetPackfile(reqGetPackfile.Checksum)
	if err != nil {
		resGetPackfile.Err = err.Error()
	} else {
		data, err := io.ReadAll(rd)
		if err != nil {
			resGetPackfile.Err = err.Error()
		} else {
			resGetPackfile.Data = data
		}
	}
	if err := json.NewEncoder(w).Encode(resGetPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func GetPackfileBlob(w http.ResponseWriter, r *http.Request) {
	var reqGetPackfileBlob network.ReqGetPackfileBlob
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfileBlob); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetPackfileBlob network.ResGetPackfileBlob
	rd, _, err := lrepository.GetPackfileBlob(reqGetPackfileBlob.Checksum, reqGetPackfileBlob.Offset, reqGetPackfileBlob.Length)
	if err != nil {
		resGetPackfileBlob.Err = err.Error()
	} else {
		data, err := io.ReadAll(rd)
		if err != nil {
			resGetPackfileBlob.Err = err.Error()
		} else {
			resGetPackfileBlob.Data = data
		}
	}
	if err := json.NewEncoder(w).Encode(resGetPackfileBlob); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deletePackfile(w http.ResponseWriter, r *http.Request) {
	if lNoDelete {
		http.Error(w, fmt.Errorf("not allowed to delete").Error(), http.StatusForbidden)
		return
	}

	var reqDeletePackfile network.ReqDeletePackfile
	if err := json.NewDecoder(r.Body).Decode(&reqDeletePackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resDeletePackfile network.ResDeletePackfile
	err := lrepository.DeletePackfile(reqDeletePackfile.Checksum)
	if err != nil {
		resDeletePackfile.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(resDeletePackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func Server(repo *repository.Repository, addr string, noDelete bool) error {

	lNoDelete = noDelete

	lrepository = repo
	network.ProtocolRegister()

	r := mux.NewRouter()
	r.HandleFunc("/", openRepository).Methods("GET")
	r.HandleFunc("/", closeRepository).Methods("POST")

	r.HandleFunc("/indexes", getStates).Methods("GET")
	r.HandleFunc("/index", putState).Methods("PUT")
	r.HandleFunc("/index", getState).Methods("GET")
	r.HandleFunc("/index", deleteState).Methods("DELETE")

	r.HandleFunc("/packfiles", getPackfiles).Methods("GET")
	r.HandleFunc("/packfile", putPackfile).Methods("PUT")
	r.HandleFunc("/packfile", getPackfile).Methods("GET")
	r.HandleFunc("/packfile/subpart", GetPackfileBlob).Methods("GET")
	r.HandleFunc("/packfile", deletePackfile).Methods("DELETE")

	return http.ListenAndServe(addr, r)
}
