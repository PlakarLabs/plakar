package api

import (
	"encoding/json"
	"net/http"
	"strings"

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

type Handler func(http.ResponseWriter, *http.Request) error

type ApiErrorRes struct {
	Error *ApiError `json:"error"`
}

func (fn Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := fn(w, r)
	if err == nil {
		return
	}

	handleError(w, err)
}

func handleError(w http.ResponseWriter, err error) {
	apierr, ok := err.(*ApiError)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "application/json")
	w.WriteHeader(apierr.HttpCode)

	json.NewEncoder(w).Encode(&ApiErrorRes{apierr})
}

// AuthMiddleware is a middleware that checks for the token in the request.
func AuthMiddleware(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Authorization")
			if key == "" {
				handleError(w, authError("missing Authorization header"))
				return
			}

			if strings.Compare(key, "Bearer "+token) != 0 {
				handleError(w, authError("invalid token"))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func NewRouter(repo *repository.Repository, token string) *mux.Router {
	lstore = repo.Store()
	lrepository = repo

	r := mux.NewRouter()

	apiRouter := r.PathPrefix("/api").Subrouter()
	if token != "" {
		apiRouter.Use(AuthMiddleware(token))
	}

	urlSigner := NewSnapshotReaderURLSigner(token)

	readerRouter := r.PathPrefix("/api").Subrouter()
	if token != "" {
		readerRouter.Use(urlSigner.VerifyMiddleware)
	}

	publicRouter := r.PathPrefix("/api").Subrouter()

	handle := func(path string, handler func(http.ResponseWriter, *http.Request) error) *mux.Route {
		return apiRouter.Handle(path, Handler(handler))
	}

	readerHandle := func(path string, handler func(http.ResponseWriter, *http.Request) error) *mux.Route {
		return readerRouter.Handle(path, Handler(handler))
	}

	publicHandle := func (path string, handler func(http.ResponseWriter, *http.Request) error) *mux.Route {
		return publicRouter.Handle(path, Handler(handler))
	}

	handle("/storage/configuration", storageConfiguration).Methods("GET")
	handle("/storage/states", storageStates).Methods("GET")
	handle("/storage/state/{state}", storageState).Methods("GET")
	handle("/storage/packfiles", storagePackfiles).Methods("GET")
	handle("/storage/packfile/{packfile}", storagePackfile).Methods("GET")

	handle("/repository/configuration", repositoryConfiguration).Methods("GET")
	handle("/repository/snapshots", repositorySnapshots).Methods("GET")
	handle("/repository/states", repositoryStates).Methods("GET")
	handle("/repository/state/{state}", repositoryState).Methods("GET")
	handle("/repository/packfiles", repositoryPackfiles).Methods("GET")
	handle("/repository/packfile/{packfile}", repositoryPackfile).Methods("GET")

	handle("/snapshot/{snapshot}", snapshotHeader).Methods("GET")
	readerHandle("/snapshot/reader/{snapshot}:{path:.+}", snapshotReader).Methods("GET")
	handle("/snapshot/reader-sign-url/{snapshot}:{path:.+}", urlSigner.Sign).Methods("POST")
	handle("/snapshot/search/{snapshot}:{path:.+}", snapshotSearch).Methods("GET")

	handle("/snapshot/vfs/{snapshot}:/", snapshotVFSBrowse).Methods("GET")
	handle("/snapshot/vfs/{snapshot}:{path:.+}/", snapshotVFSBrowse).Methods("GET")
	handle("/snapshot/vfs/{snapshot}:{path:.+}", snapshotVFSBrowse).Methods("GET")

	handle("/snapshot/vfs/downloader/{snapshot}", snapshotVFSDownloader).Methods("POST")
	publicHandle("/snapshot/vfs/downloader-sign-url/{id}", snapshotVFSDownloaderSigned).Methods("GET")

	handle("/snapshot/vfs/children/{snapshot}:/", snapshotVFSChildren).Methods("GET")
	handle("/snapshot/vfs/children/{snapshot}:{path:.+}/", snapshotVFSChildren).Methods("GET")
	handle("/snapshot/vfs/children/{snapshot}:{path:.+}", snapshotVFSChildren).Methods("GET")

	handle("/snapshot/vfs/errors/{snapshot}:/", snapshotVFSErrors).Methods("GET")
	handle("/snapshot/vfs/errors/{snapshot}:{path:.+}/", snapshotVFSErrors).Methods("GET")
	handle("/snapshot/vfs/errors/{snapshot}:{path:.+}", snapshotVFSErrors).Methods("GET")

	return r
}
