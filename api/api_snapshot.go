package api

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strconv"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/search"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
	"github.com/gorilla/mux"
)

func snapshotHeader(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	snapshotIDstr := vars["snapshot"]

	snapshotID, err := hex.DecodeString(snapshotIDstr)
	if err != nil {
		return parameterError("snapshot", InvalidArgument, err)
	}
	if len(snapshotID) != 32 {
		return parameterError("snapshot", InvalidArgument, ErrInvalidID)
	}
	snapshotID32 := [32]byte{}
	copy(snapshotID32[:], snapshotID)

	snap, err := snapshot.Load(lrepository, snapshotID32)
	if err != nil {
		return err
	}

	return json.NewEncoder(w).Encode(Item{Item: snap.Header})
}

func snapshotReader(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	snapshotIDstr := vars["snapshot"]
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

	snapshotID, err := hex.DecodeString(snapshotIDstr)
	if err != nil {
		return parameterError("snapshot", InvalidArgument, err)
	}
	if len(snapshotID) != 32 {
		return parameterError("snapshot", InvalidArgument, ErrInvalidID)
	}
	snapshotID32 := [32]byte{}
	copy(snapshotID32[:], snapshotID)

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

func snapshotVFSBrowse(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	snapshotIDstr := vars["snapshot"]
	path := vars["path"]

	snapshotID, err := hex.DecodeString(snapshotIDstr)
	if err != nil {
		return parameterError("snapshot", InvalidArgument, err)
	}
	if len(snapshotID) != 32 {
		return parameterError("snapshot", InvalidArgument, ErrInvalidID)
	}
	snapshotID32 := [32]byte{}
	copy(snapshotID32[:], snapshotID)

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
	snapshotIDstr := vars["snapshot"]
	path := vars["path"]

	var err error
	var sortKeys []string
	var offset int64
	var limit int64

	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	sortKeysStr := r.URL.Query().Get("sort")
	if sortKeysStr == "" {
		sortKeysStr = "Name"
	}

	sortKeys, err = objects.ParseFileInfoSortKeys(sortKeysStr)
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

	snapshotID, err := hex.DecodeString(snapshotIDstr)
	if err != nil {
		return parameterError("snapshot", InvalidArgument, err)
	}
	if len(snapshotID) != 32 {
		return parameterError("snapshot", InvalidArgument, ErrInvalidID)
	}
	snapshotID32 := [32]byte{}
	copy(snapshotID32[:], snapshotID)

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
		fileInfos := make([]objects.FileInfo, 0, len(dirEntry.Children))
		children := make(map[string]vfs.ChildEntry)
		for _, child := range dirEntry.Children {
			fileInfos = append(fileInfos, child.Stat())
			children[child.Stat().Name()] = child
		}

		if limit == 0 {
			limit = int64(len(dirEntry.Children))
		}
		if err := objects.SortFileInfos(fileInfos, sortKeys); err != nil {
			return parameterError("sort", InvalidArgument, err)
		}

		if offset > int64(len(dirEntry.Children)) {
			fileInfos = []objects.FileInfo{}
		} else if offset+limit > int64(len(dirEntry.Children)) {
			fileInfos = fileInfos[offset:]
		} else {
			fileInfos = fileInfos[offset : offset+limit]
		}

		childEntries := make([]vfs.ChildEntry, 0, len(fileInfos))
		for _, fileInfo := range fileInfos {
			childEntries = append(childEntries, children[fileInfo.Name()])
		}

		items := Items{
			Total: len(dirEntry.Children),
			Items: make([]interface{}, len(childEntries)),
		}
		for i, child := range childEntries {
			items.Items[i] = child
		}
		return json.NewEncoder(w).Encode(items)
	}
}

func snapshotVFSErrors(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	snapshotIDstr := vars["snapshot"]
	path := vars["path"]

	var err error
	var offset int64
	var limit int64

	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")

	sortKeysStr := r.URL.Query().Get("sort")
	if sortKeysStr == "" {
		sortKeysStr = "Name"
	}
	if sortKeysStr != "Name" && sortKeysStr != "-Name" {
		return parameterError("sort", InvalidArgument, ErrInvalidSortKey)
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

	snapshotID, err := hex.DecodeString(snapshotIDstr)
	if err != nil {
		return parameterError("snapshot", InvalidArgument, err)
	}
	if len(snapshotID) != 32 {
		return parameterError("snapshot", InvalidArgument, ErrInvalidID)
	}
	snapshotID32 := [32]byte{}
	copy(snapshotID32[:], snapshotID)

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
			Total: 0,
			Items: make([]interface{}, 0),
		}
		if dirEntry.ErrorFirst != nil {

			if sortKeysStr == "Name" {
				iter := dirEntry.ErrorFirst
				for i := int64(0); i < limit+offset && iter != nil; i++ {
					errorEntryBytes, err := snap.GetBlob(packfile.TYPE_ERROR, *iter)
					if err != nil {
						return err
					}

					errorEntry, err := vfs.ErrorEntryFromBytes(errorEntryBytes)
					if err != nil {
						return err
					}
					iter = errorEntry.Successor

					if i < offset {
						continue
					}

					items.Total += 1
					errorEntry.Predecessor = nil
					errorEntry.Successor = nil
					items.Items = append(items.Items, errorEntry)
				}
			} else if sortKeysStr == "-Name" {
				iter := dirEntry.ErrorLast
				for i := int64(0); i < limit+offset && iter != nil; i++ {
					errorEntryBytes, err := snap.GetBlob(packfile.TYPE_ERROR, *iter)
					if err != nil {
						return err
					}

					errorEntry, err := vfs.ErrorEntryFromBytes(errorEntryBytes)
					if err != nil {
						return err
					}
					iter = errorEntry.Predecessor
					if i < offset {
						continue
					}
					items.Total += 1
					errorEntry.Predecessor = nil
					errorEntry.Successor = nil
					items.Items = append(items.Items, errorEntry)
				}
			}
		}
		return json.NewEncoder(w).Encode(items)
	}
}

func snapshotSearch(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	snapshotIDstr := vars["snapshot"]
	path := vars["path"]

	var err error
	var offset int64
	var limit int64

	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")
	queryStr := r.URL.Query().Get("q")

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

	snapshotID, err := hex.DecodeString(snapshotIDstr)
	if err != nil {
		return parameterError("snapshot", InvalidArgument, err)
	}
	if len(snapshotID) != 32 {
		return parameterError("snapshot", InvalidArgument, ErrInvalidID)
	}
	snapshotID32 := [32]byte{}
	copy(snapshotID32[:], snapshotID)

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
