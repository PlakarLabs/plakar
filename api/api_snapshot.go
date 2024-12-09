package api

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/search"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/mux"
)

func snapshotHeader(w http.ResponseWriter, r *http.Request) error {
	snapshotID32, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(Item{Item: snap.Header})
}

func snapshotReader(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	path := vars["path"]

	do_highlight := false
	do_download := false

	download := r.URL.Query().Get("download")
	if download == "true" {
		do_download = true
	}

	render := r.URL.Query().Get("render")
	if render == "highlight" {
		do_highlight = true
	}

	snapshotID32, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	rd, err := snap.NewReader(path)
	if err != nil {
		return err
	}

	if do_download {
		w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(filepath.Base(path)))
	}

	if do_highlight {
		lexer := lexers.Match(path)
		if lexer == nil {
			lexer = lexers.Get(rd.ContentType())
		}
		if lexer == nil {
			lexer = lexers.Fallback // Fallback if no lexer is found
		}
		formatter := formatters.Get("html")
		style := styles.Get("dracula")

		w.Header().Set("Content-Type", "text/html")
		if _, err := w.Write([]byte("<!DOCTYPE html>")); err != nil {
			return err
		}

		reader := bufio.NewReader(rd)
		buffer := make([]byte, 4096) // Fixed-size buffer for chunked reading
		for {
			n, err := reader.Read(buffer) // Read up to the size of the buffer
			if n > 0 {
				chunk := string(buffer[:n])

				// Tokenize the chunk and apply syntax highlighting
				iterator, errTokenize := lexer.Tokenise(nil, chunk)
				if errTokenize != nil {
					break
				}

				errFormat := formatter.Format(w, style, iterator)
				if errFormat != nil {
					break
				}
			}

			// Check for end of file (EOF)
			if err == io.EOF {
				break
			} else if err != nil {
				break
			}
		}
	} else {
		http.ServeContent(w, r, filepath.Base(path), rd.ModTime(), rd)
	}

	return nil
}

type SnapshotReaderURLSigner struct {
	token string
}

func NewSnapshotReaderURLSigner(token string) SnapshotReaderURLSigner {
	return SnapshotReaderURLSigner{token}
}

type SnapshotSignedURLClaims struct {
	SnapshotID string `json:"snapshot_id"`
	Path       string `json:"path"`
	jwt.RegisteredClaims
}

func (signer SnapshotReaderURLSigner) Sign(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	path := vars["path"]

	_, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	now := time.Now()
	jwtToken := jwt.NewWithClaims(jwt.SigningMethodHS256, SnapshotSignedURLClaims{
		SnapshotID: vars["snapshot"],
		Path:       path,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(2 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			Issuer:    "plakar-api",
		},
	})

	signature, err := jwtToken.SignedString([]byte(signer.token))
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(Item{
		struct {
			Signature string `json:"signature"`
		}{signature},
	})
}

// VerifyMiddleware is a middleware that checks if the request to read the file
// content is authorized. It checks if the ?signature query parameter is valid.
// If it is not valid, it falls back to the Authorization header.
func (signer SnapshotReaderURLSigner) VerifyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signature := r.URL.Query().Get("signature")

		// No signature provided, fall back to Authorization header
		if signature == "" {
			AuthMiddleware(signer.token)(next).ServeHTTP(w, r)
			return
		}

		// Parse signature
		vars := mux.Vars(r)
		path := vars["path"]
		snapshotId := vars["snapshot"]

		jwtToken, err := jwt.ParseWithClaims(signature, &SnapshotSignedURLClaims{}, func(jwtToken *jwt.Token) (interface{}, error) {
			if _, ok := jwtToken.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, authError(fmt.Sprintf("unexpected signing method: %v", jwtToken.Header["alg"]))
			}
			return []byte(signer.token), nil
		})

		if err != nil {
			if errors.Is(err, jwt.ErrTokenExpired) {
				handleError(w, authError("token expired"))
				return
			}
			handleError(w, authError(fmt.Sprintf("unable to parse JWT token: %v", err)))
			return
		}

		if claims, ok := jwtToken.Claims.(*SnapshotSignedURLClaims); ok {
			if claims.Path != path {
				handleError(w, authError("invalid URL path"))
				return
			}
			if claims.SnapshotID != snapshotId {
				handleError(w, authError("invalid URL snapshot"))
				return
			}
		} else {
			handleError(w, authError("invalid URL signature"))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func snapshotVFSBrowse(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	path := vars["path"]

	snapshotID32, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	if path == "" {
		path = "/"
	}
	fsinfo, err := fs.Stat(path)
	if err != nil {
		return err
	}

	if dirEntry, ok := fsinfo.(*vfs.DirEntry); ok {
		return json.NewEncoder(w).Encode(Item{Item: dirEntry})
	} else if fileEntry, ok := fsinfo.(*vfs.FileEntry); ok {
		return json.NewEncoder(w).Encode(Item{Item: fileEntry})
	} else {
		http.Error(w, "", http.StatusInternalServerError)
		return nil
	}
}

func snapshotVFSChildren(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	path := vars["path"]

	offset, _, err := QueryParamToInt64(r, "offset")
	if err != nil {
		return err
	}

	limit, _, err := QueryParamToInt64(r, "limit")
	if err != nil {
		return err
	}

	sortKeysStr := r.URL.Query().Get("sort")
	if sortKeysStr == "" {
		sortKeysStr = "Name"
	}
	sortKeys, err := objects.ParseFileInfoSortKeys(sortKeysStr)
	if err != nil {
		return parameterError("sort", InvalidArgument, err)
	}
	_ = sortKeys

	snapshotID32, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	if path == "" {
		path = "/"
	}
	fsinfo, err := fs.Stat(path)
	if err != nil {
		return err
	}

	if dirEntry, ok := fsinfo.(*vfs.DirEntry); !ok {
		http.Error(w, "not a directory", http.StatusBadRequest)
		return nil
	} else {
		items := Items{
			Total: int(dirEntry.Summary.Directory.Children),
			Items: make([]interface{}, 0),
		}
		childrenList, err := fs.ChildrenIter(dirEntry)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return nil
		}

		if limit == 0 {
			limit = int64(dirEntry.Summary.Directory.Children)
		}

		i := int64(0)
		for child := range childrenList {
			if child == nil {
				break
			}
			if i < offset {
				i++
				continue
			}
			if i >= limit+offset {
				break
			}
			items.Items = append(items.Items, child)
			i++
		}
		return json.NewEncoder(w).Encode(items)
	}
}

func snapshotVFSErrors(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	path := vars["path"]

	sortKeysStr := r.URL.Query().Get("sort")
	if sortKeysStr == "" {
		sortKeysStr = "Name"
	}
	if sortKeysStr != "Name" && sortKeysStr != "-Name" {
		return parameterError("sort", InvalidArgument, ErrInvalidSortKey)
	}

	offset, _, err := QueryParamToInt64(r, "offset")
	if err != nil {
		return err
	}

	limit, _, err := QueryParamToInt64(r, "limit")
	if err != nil {
		return err
	}

	snapshotID32, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	if path == "" {
		path = "/"
	}

	errorList, err := snap.Errors(path)
	if err != nil {
		return err
	}

	var i int64
	items := Items{
		Items: []interface{}{},
	}
	for errorEntry := range errorList {
		if i < offset {
			i++
			continue
		}
		if limit > 0 && i >= limit+offset {
			i++
			continue
		}
		items.Items = append(items.Items, errorEntry)
		i++
	}
	items.Total = int(i)
	return json.NewEncoder(w).Encode(items)
}

func snapshotSearch(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	path := vars["path"]

	queryStr := r.URL.Query().Get("q")

	offset, _, err := QueryParamToInt64(r, "offset")
	if err != nil {
		return err
	}

	limit, _, err := QueryParamToInt64(r, "limit")
	if err != nil {
		return err
	}

	snapshotID32, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	results, err := snap.Search(path, queryStr)
	if err != nil {
		return err
	}

	items := Items{
		Total: 0,
		Items: make([]interface{}, 0),
	}
	i := int64(0)
	for result := range results {
		if i >= offset {
			if limit != 0 {
				if i >= limit+offset {
					break
				}
			}
			if entry, isFilename := result.(search.FileEntry); isFilename {
				items.Total += 1
				items.Items = append(items.Items, entry)
			}
		}
		i++
	}

	return json.NewEncoder(w).Encode(items)
}
