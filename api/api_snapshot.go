package api

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

type downloadSignedUrl struct {
	snapshotID [32]byte
	rebase     string
	files      []string
}

var downloadSignedUrls = make(map[string]downloadSignedUrl)
var muDownloadSignedUrls sync.Mutex

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

type DownloadItem struct {
	Pathname string `json:"pathname"`
}
type DownloadQuery struct {
	Name   string         `json:"name"`
	Format string         `json:"format"`
	Items  []DownloadItem `json:"items"`
	Rebase string         `json:"rebase,omitempty"`
}

func randomID(n int) string {
	alphabet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

func snapshotVFSDownloader(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	_ = vars

	snapshotID32, err := PathParamToID(r, "snapshot")
	if err != nil {
		return err
	}

	var query DownloadQuery
	if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
		return parameterError("BODY", InvalidArgument, err)
	}

	_, err = snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	muDownloadSignedUrls.Lock()
	defer muDownloadSignedUrls.Unlock()
	for {
		link := randomID(32)
		if _, ok := downloadSignedUrls[link]; ok {
			continue
		}

		url := downloadSignedUrl{
			snapshotID: snapshotID32,
			rebase:     query.Rebase,
		}

		for _, item := range query.Items {
			url.files = append(url.files, item.Pathname)
		}

		downloadSignedUrls[link] = url
		res := struct{ Link string }{link}
		json.NewEncoder(w).Encode(&res)
		return nil
	}
}

func snapshotVFSDownloaderSigned(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	id := vars["id"]

	muDownloadSignedUrls.Lock()
	link, ok := downloadSignedUrls[id]
	muDownloadSignedUrls.Unlock()
	if !ok {
		return &ApiError{
			HttpCode: 404,
			ErrCode:  "signed-link-not-found",
			Message:  "Signed Link Not Found",
		}
	}

	snap, err := snapshot.Load(lrepository, link.snapshotID)
	if err != nil {
		return err
	}

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	out := w

	name := r.URL.Query().Get("name")
	if name == "" {
		name = fmt.Sprintf("snapshot-%x-%s", link.snapshotID[:4], time.Now().Format("2006-01-02-15-04-05"))
	}

	format := r.URL.Query().Get("format")

	var ext string
	if format == "zip" {
		ext = ".zip"
		w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(name+ext))
		w.WriteHeader(200)
		if err := archiveZip(snap, out, fs, link.files, link.rebase, name); err != nil {
			return err
		}
	} else if format == "tar" {
		ext = ".tar"
		w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(name+ext))
		w.WriteHeader(200)
		if err := archiveTarball(snap, out, fs, link.files, link.rebase, name); err != nil {
			return err
		}
	} else if format == "tarball" || format == "" {
		ext = ".tar.gz"
		w.Header().Set("Content-Disposition", "attachment; filename="+strconv.Quote(name+ext))
		w.WriteHeader(200)
		gzipWriter := gzip.NewWriter(out)
		defer gzipWriter.Close()
		if err := archiveTarball(snap, gzipWriter, fs, link.files, link.rebase, name); err != nil {
			return err
		}
	} else {
		return parameterError("format", InvalidArgument, ErrInvalidFormat)
	}

	return nil
}

func archiveTarball(snap *snapshot.Snapshot, out io.Writer, fs *vfs.Filesystem, dl []string, rebase string, prefix string) error {
	tarWriter := tar.NewWriter(out)
	defer tarWriter.Close()

	addFile := func(pathname string, fi vfs.FSEntry) error {
		header := &tar.Header{
			Name:     prefix + "/" + strings.TrimPrefix(strings.TrimPrefix(pathname, rebase), "/"),
			Typeflag: tar.TypeReg,

			Size:    fi.Stat().Size(),
			Mode:    int64(fi.Stat().Mode()),
			ModTime: fi.Stat().ModTime(),
		}
		if fi.Stat().Mode().IsRegular() {
			rd, err := snap.NewReader(pathname)
			if err != nil {
				return err
			}
			defer rd.Close()

			if err := tarWriter.WriteHeader(header); err != nil {
				return err
			}

			if _, err := io.Copy(tarWriter, rd); err != nil {
				return err
			}
		}
		return nil
	}

	for _, pathname := range dl {
		fi, err := fs.Stat(pathname)
		if err != nil {
			return err
		}

		filepath := pathname
		if rebase != "" {
			filepath = strings.TrimPrefix(filepath, rebase)
		}

		if fi.Stat().IsDir() {
			tarWriter.WriteHeader(&tar.Header{
				Name:     prefix + "/" + strings.TrimPrefix(filepath, "/"),
				Typeflag: tar.TypeDir,
				//				Size:    fi.Size(),
				Mode:    int64(fi.Stat().Mode()),
				ModTime: fi.Stat().ModTime(),
			})
			c, err := fs.PathnamesFrom(pathname)
			if err != nil {
				return err
			}
			for childpath := range c {
				subfi, err := fs.Stat(childpath)
				if err != nil {
					return err
				}
				if subfi.Stat().IsDir() {
					tarWriter.WriteHeader(&tar.Header{
						Name:     prefix + "/" + strings.TrimPrefix(strings.TrimPrefix(childpath, rebase), "/"),
						Typeflag: tar.TypeDir,
						//Size:    subfi.Size(),
						Mode:    int64(subfi.Stat().Mode()),
						ModTime: subfi.Stat().ModTime(),
					})
				} else {
					if err := addFile(childpath, subfi); err != nil {
						return err
					}
				}
			}
		} else {
			if err := addFile(pathname, fi); err != nil {
				return err
			}
		}
	}

	return nil
}

func archiveZip(snap *snapshot.Snapshot, out io.Writer, fs *vfs.Filesystem, dl []string, rebase string, prefix string) error {
	zipWriter := zip.NewWriter(out)
	defer zipWriter.Close()

	addFile := func(pathname string, fi vfs.FSEntry) error {
		header, err := zip.FileInfoHeader(fi.Stat())
		if err != nil {
			log.Printf("could not create header for file %s: %s", pathname, err)
			return err
		}
		header.Name = prefix + "/" + strings.TrimPrefix(strings.TrimLeft(pathname, "/"), rebase)
		header.Method = zip.Deflate

		if fi.Stat().Mode().IsRegular() {
			rd, err := snap.NewReader(pathname)
			if err != nil {
				return err
			}
			defer rd.Close()

			writer, err := zipWriter.CreateHeader(header)
			if err != nil {
				log.Printf("could not create zip entry for file %s: %s", pathname, err)
				rd.Close()
				return err
			}

			if _, err := io.Copy(writer, rd); err != nil {
				rd.Close()
				return err
			}
			rd.Close()
		}
		return nil
	}

	for _, pathname := range dl {
		fi, err := fs.Stat(pathname)
		if err != nil {
			return err
		}

		if fi.Stat().IsDir() {
			c, err := fs.PathnamesFrom(pathname)
			if err != nil {
				return err
			}
			for childpath := range c {
				subfi, err := fs.Stat(childpath)
				if err != nil {
					return err
				}
				if !subfi.Stat().IsDir() {
					if err := addFile(childpath, subfi); err != nil {
						return err
					}
				}
			}
		} else {
			if err := addFile(pathname, fi); err != nil {
				return err
			}
		}
	}

	return nil
}
