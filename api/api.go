package api

import (
	"net/http"

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

// AuthMiddleware is a middleware that checks for the authkey in the request.
func AuthMiddleware(authkey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Authorization")
			if key == "" {
				http.Error(w, "Unauthorized: missing Authorization header", http.StatusUnauthorized)
				return
			}

			if key != authkey {
				http.Error(w, "Unauthorized: invalid authkey", http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func NewRouter(repo *repository.Repository, authkey string) *mux.Router {
	lstore = repo.Store()
	lrepository = repo

	r := mux.NewRouter()

	apiRouter := r.PathPrefix("/api").Subrouter()
	if authkey != "" {
		apiRouter.Use(AuthMiddleware(authkey))
	}

	apiRouter.HandleFunc("/storage/configuration", storageConfiguration).Methods("GET")
	apiRouter.HandleFunc("/storage/states", storageStates).Methods("GET")
	apiRouter.HandleFunc("/storage/state/{state}", storageState).Methods("GET")
	apiRouter.HandleFunc("/storage/packfiles", storagePackfiles).Methods("GET")
	apiRouter.HandleFunc("/storage/packfile/{packfile}", storagePackfile).Methods("GET")

	apiRouter.HandleFunc("/repository/configuration", repositoryConfiguration).Methods("GET")
	apiRouter.HandleFunc("/repository/snapshots", repositorySnapshots).Methods("GET")
	apiRouter.HandleFunc("/repository/states", repositoryStates).Methods("GET")
	apiRouter.HandleFunc("/repository/state/{state}", repositoryState).Methods("GET")
	apiRouter.HandleFunc("/repository/packfiles", repositoryPackfiles).Methods("GET")
	apiRouter.HandleFunc("/repository/packfile/{packfile}", repositoryPackfile).Methods("GET")

	apiRouter.HandleFunc("/snapshot/{snapshot}", snapshotHeader).Methods("GET")
	apiRouter.HandleFunc("/snapshot/reader/{snapshot}:{path:.+}", snapshotReader).Methods("GET")

	apiRouter.HandleFunc("/snapshot/vfs/{snapshot}:/", snapshotVFSBrowse).Methods("GET")
	apiRouter.HandleFunc("/snapshot/vfs/{snapshot}:{path:.+}/", snapshotVFSBrowse).Methods("GET")
	apiRouter.HandleFunc("/snapshot/vfs/{snapshot}:{path:.+}", snapshotVFSBrowse).Methods("GET")

	apiRouter.HandleFunc("/snapshot/vfs/children/{snapshot}:/", snapshotVFSChildren).Methods("GET")
	apiRouter.HandleFunc("/snapshot/vfs/children/{snapshot}:{path:.+}/", snapshotVFSChildren).Methods("GET")
	apiRouter.HandleFunc("/snapshot/vfs/children/{snapshot}:{path:.+}", snapshotVFSChildren).Methods("GET")

	apiRouter.HandleFunc("/snapshot/vfs/errors/{snapshot}:/", snapshotVFSErrors).Methods("GET")
	apiRouter.HandleFunc("/snapshot/vfs/errors/{snapshot}:{path:.+}/", snapshotVFSErrors).Methods("GET")
	apiRouter.HandleFunc("/snapshot/vfs/errors/{snapshot}:{path:.+}", snapshotVFSErrors).Methods("GET")

	return r
}
