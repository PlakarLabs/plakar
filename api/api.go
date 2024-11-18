package api

import (
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/gorilla/mux"
)

var lstore *storage.Store
var lrepository *repository.Repository

type Item struct {
	Item interface{} `json:"item"`
}

type Items struct {
	Total int           `json:"total"`
	Items []interface{} `json:"items"`
}

func NewRouter(repo *repository.Repository) *mux.Router {
	lstore = repo.Store()
	lrepository = repo

	r := mux.NewRouter()

	r.HandleFunc("/api/storage/configuration", storageConfiguration).Methods("GET")
	r.HandleFunc("/api/storage/states", storageStates).Methods("GET")
	r.HandleFunc("/api/storage/state/{state}", storageState).Methods("GET")
	r.HandleFunc("/api/storage/packfiles", storagePackfiles).Methods("GET")
	r.HandleFunc("/api/storage/packfile/{packfile}", storagePackfile).Methods("GET")

	r.HandleFunc("/api/repository/configuration", repositoryConfiguration).Methods("GET")
	r.HandleFunc("/api/repository/snapshots", repositorySnapshots).Methods("GET")
	r.HandleFunc("/api/repository/states", repositoryStates).Methods("GET")
	r.HandleFunc("/api/repository/state/{state}", repositoryState).Methods("GET")
	r.HandleFunc("/api/repository/packfiles", repositoryPackfiles).Methods("GET")
	r.HandleFunc("/api/repository/packfile/{packfile}", repositoryPackfile).Methods("GET")

	r.HandleFunc("/api/snapshot/{snapshot}", snapshotHeader).Methods("GET")
	r.HandleFunc("/api/snapshot/reader/{snapshot}:{path:.+}", snapshotReader).Methods("GET")

	r.HandleFunc("/api/snapshot/vfs/{snapshot}:/", snapshotVFSBrowse).Methods("GET")
	r.HandleFunc("/api/snapshot/vfs/{snapshot}:{path:.+}/", snapshotVFSBrowse).Methods("GET")
	r.HandleFunc("/api/snapshot/vfs/{snapshot}:{path:.+}", snapshotVFSBrowse).Methods("GET")

	return r
}
