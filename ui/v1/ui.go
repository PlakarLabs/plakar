/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package v1

import (
	_ "embed"
	"encoding/hex"
	"fmt"
	"html/template"
	"math"
	"math/rand"
	"mime"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"

	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
)

var lrepository *repository.Repository
var lcache *snapshot.Snapshot

//go:embed base.tmpl
var baseTemplate string

//go:embed repository.tmpl
var repositoryTemplate string

//go:embed browse.tmpl
var browseTemplate string

//go:embed object.tmpl
var objectTemplate string

//go:embed search.tmpl
var searchTemplate string

var templates map[string]*template.Template

type SnapshotSummary struct {
	Header      *header.Header
	Roots       uint64
	Directories uint64
	Files       uint64
	Pathnames   uint64
	Objects     uint64
	Chunks      uint64
	Size        uint64
}

type TemplateFunctions struct {
	HumanizeBytes func(uint64) string
}

func getSnapshots(repo *repository.Repository) ([]*header.Header, error) {
	snapshotsList, err := repo.GetSnapshots()
	if err != nil {
		return nil, err
	}

	result := make([]*header.Header, 0)

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	for _, snapshotUuid := range snapshotsList {
		wg.Add(1)
		go func(snapshotUuid [32]byte) {
			defer wg.Done()
			hdr, _, err := snapshot.GetSnapshot(repo, snapshotUuid)
			if err != nil {
				return
			}
			mu.Lock()
			result = append(result, hdr)
			mu.Unlock()
		}(snapshotUuid)
	}
	wg.Wait()
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreationTime.Before(result[j].CreationTime)
	})
	return result, nil

}

func (summary *SnapshotSummary) HumanSize() string {
	return humanize.Bytes(summary.Size)
}

func viewRepository(w http.ResponseWriter, r *http.Request) {

	hdrs, _ := getSnapshots(lrepository)

	totalFiles := uint64(0)

	kinds := make(map[string]uint64)
	types := make(map[string]uint64)
	extensions := make(map[string]uint64)

	kindsPct := make(map[string]float64)
	typesPct := make(map[string]float64)
	extensionsPct := make(map[string]float64)

	res := make([]*header.Header, 0)
	for _, hdr := range hdrs {
		res = append(res, hdr)
		totalFiles += hdr.FilesCount

		/*
			for key, value := range hdr.FileKind {
				if _, exists := kinds[key]; !exists {
					kinds[key] = 0
				}
				kinds[key] += value
			}

			for key, value := range hdr.FileType {
				if _, exists := types[key]; !exists {
					types[key] = 0
				}
				types[key] += value
			}

			for key, value := range hdr.FileExtension {
				if _, exists := extensions[key]; !exists {
					extensions[key] = 0
				}
				extensions[key] += value
			}
		*/
	}

	for key, value := range kinds {
		kindsPct[key] = math.Round((float64(value)/float64(totalFiles)*100)*100) / 100
	}

	for key, value := range types {
		typesPct[key] = math.Round((float64(value)/float64(totalFiles)*100)*100) / 100
	}

	for key, value := range extensions {
		extensionsPct[key] = math.Round((float64(value)/float64(totalFiles)*100)*100) / 100
	}

	ctx := &struct {
		Repository    storage.Configuration
		Headers       []*header.Header
		MajorTypes    map[string]uint64
		MimeTypes     map[string]uint64
		Extensions    map[string]uint64
		MajorTypesPct map[string]float64
		MimeTypesPct  map[string]float64
		ExtensionsPct map[string]float64
	}{
		lrepository.Configuration(),
		res,
		kinds,
		types,
		extensions,
		kindsPct,
		typesPct,
		extensionsPct,
	}

	templates["repository"].Execute(w, ctx)
}

func browse(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	var snap *snapshot.Snapshot
	if lcache == nil || hex.EncodeToString(lcache.Header.SnapshotID[:]) != id {
		decodedID, err := hex.DecodeString(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(decodedID) != 32 {
			http.Error(w, "invalid snapshot id", http.StatusInternalServerError)
			return
		}
		newIndexID := [32]byte{}
		copy(newIndexID[:], decodedID)

		tmp, err := snapshot.Load(lrepository, newIndexID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		snap = tmp
		lcache = snap
	} else {
		snap = lcache
	}

	if path == "" {
		path = "/"
	}

	fs, err := snap.Filesystem()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = fs.Stat(path)
	if err != nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	directories := make([]*objects.FileInfo, 0)
	files := make([]*objects.FileInfo, 0)
	symlinks := make([]*objects.FileInfo, 0)
	symlinksResolve := make(map[string]string)
	others := make([]*objects.FileInfo, 0)

	children, err := fs.Children(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for childname := range children {
		info, _ := fs.Stat(filepath.Clean(fmt.Sprintf("%s/%s", path, childname)))

		var fileinfo *objects.FileInfo
		switch info := info.(type) {
		case *vfs.DirEntry:
			fileinfo = info.FileInfo()
		case *vfs.FileEntry:
			fileinfo = info.FileInfo()
		}

		if fileinfo.Mode().IsDir() {
			directories = append(directories, fileinfo)
		} else if fileinfo.Mode().IsRegular() {
			files = append(files, fileinfo)
		} else if info.(*vfs.FileEntry).SymlinkTarget != "" {
			symlinks = append(symlinks, fileinfo)
			symlinksResolve[fileinfo.Name()] = info.(*vfs.FileEntry).SymlinkTarget
		} else {
			others = append(others, fileinfo)
		}

	}

	sort.Slice(directories, func(i, j int) bool {
		return strings.Compare(directories[i].Name(), directories[j].Name()) < 0
	})
	sort.Slice(files, func(i, j int) bool {
		return strings.Compare(files[i].Name(), files[j].Name()) < 0
	})
	sort.Slice(symlinks, func(i, j int) bool {
		return strings.Compare(symlinks[i].Name(), symlinks[j].Name()) < 0
	})
	sort.Slice(others, func(i, j int) bool {
		return strings.Compare(others[i].Name(), others[j].Name()) < 0
	})

	nav := make([]string, 0)
	navLinks := make(map[string]string)
	atoms := strings.Split(path, "/")[1:]
	for offset, atom := range atoms {
		nav = append(nav, atom)
		navLinks[atom] = "/" + strings.Join(atoms[:offset+1], "/")
	}

	ctx := &struct {
		Snapshot        *snapshot.Snapshot
		Directories     []*objects.FileInfo
		Files           []*objects.FileInfo
		Symlinks        []*objects.FileInfo
		SymlinksResolve map[string]string
		Others          []*objects.FileInfo
		Path            string
		Scanned         []string
		Navigation      []string
		NavigationLinks map[string]string
	}{snap, directories, files, symlinks, symlinksResolve, others, path, []string{snap.Header.ScannedDirectory}, nav, navLinks}
	templates["browse"].Execute(w, ctx)

}

func object(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]

	var snap *snapshot.Snapshot
	if lcache == nil || hex.EncodeToString(lcache.Header.SnapshotID[:]) != id {
		decodedID, err := hex.DecodeString(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(decodedID) != 32 {
			http.Error(w, "invalid snapshot id", http.StatusInternalServerError)
			return
		}
		newIndexID := [32]byte{}
		copy(newIndexID[:], decodedID)

		tmp, err := snapshot.Load(lrepository, newIndexID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		snap = tmp
		lcache = snap
	} else {
		snap = lcache
	}

	fs, err := snap.Filesystem()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fsinfo, err := fs.Stat(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, isDir := fsinfo.(*vfs.DirEntry); isDir {
		http.Error(w, "is directory", http.StatusInternalServerError)
		return
	}

	info := fsinfo.(*vfs.FileEntry).FileInfo()
	if !info.Mode().IsRegular() {
		http.Error(w, "not regular", http.StatusInternalServerError)
		return
	}

	checksum := fsinfo.(*vfs.FileEntry).Checksum
	object, err := snap.LookupObject(checksum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if object == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	chunks := make([]*objects.Chunk, 0)
	for _, chunk := range object.Chunks {
		chunks = append(chunks, &chunk)
	}

	root := ""
	for _, atom := range strings.Split(path, "/") {
		root = root + atom + "/"
		if st, err := fs.Stat(root); err != nil {
			break
		} else if _, isDir := st.(*vfs.DirEntry); isDir {
			break
		}
	}

	nav := make([]string, 0)
	navLinks := make(map[string]string)
	atoms := strings.Split(path, "/")[1:]
	for offset, atom := range atoms {
		nav = append(nav, atom)
		navLinks[atom] = "/" + strings.Join(atoms[:offset+1], "/")
	}

	enableViewer := false
	if strings.HasPrefix(object.ContentType, "text/") ||
		strings.HasPrefix(object.ContentType, "image/") ||
		strings.HasPrefix(object.ContentType, "audio/") ||
		strings.HasPrefix(object.ContentType, "video/") ||
		object.ContentType == "application/pdf" ||
		object.ContentType == "application/javascript" ||
		object.ContentType == "application/x-sql" ||
		object.ContentType == "application/x-tex" {
		enableViewer = true
	}

	ctx := &struct {
		Snapshot        *snapshot.Snapshot
		Object          *objects.Object
		ObjectChecksum  string
		Chunks          []*objects.Chunk
		Info            *objects.FileInfo
		Root            string
		Path            string
		Navigation      []string
		NavigationLinks map[string]string
		EnableViewer    bool
	}{snap, object, fmt.Sprintf("%016x", object.Checksum), chunks, info, root, path, nav, navLinks, enableViewer}
	templates["object"].Execute(w, ctx)
}

func raw(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["snapshot"]
	path := vars["path"]
	download := r.URL.Query().Get("download")
	highlight := r.URL.Query().Get("highlight")

	var snap *snapshot.Snapshot
	if lcache == nil || hex.EncodeToString(lcache.Header.SnapshotID[:]) != id {
		decodedID, err := hex.DecodeString(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(decodedID) != 32 {
			http.Error(w, "invalid snapshot id", http.StatusInternalServerError)
			return
		}
		newIndexID := [32]byte{}
		copy(newIndexID[:], decodedID)

		tmp, err := snapshot.Load(lrepository, newIndexID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		snap = tmp
		lcache = snap
	} else {
		snap = lcache
	}

	fs, err := snap.Filesystem()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fsinfo, err := fs.Stat(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, isDir := fsinfo.(*vfs.DirEntry); isDir {
		http.Error(w, "is directory", http.StatusInternalServerError)
		return
	}

	info := fsinfo.(*vfs.FileEntry).FileInfo()
	if !info.Mode().IsRegular() {
		http.Error(w, "not regular", http.StatusInternalServerError)
		return
	}

	checksum := fsinfo.(*vfs.FileEntry).Checksum
	object, err := snap.LookupObject(checksum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if object == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	contentType := mime.TypeByExtension(filepath.Ext(path))
	if contentType == "" {
		contentType = object.ContentType
	}

	if contentType == "application/x-tex" {
		contentType = "text/plain"
	}

	if !strings.HasPrefix(object.ContentType, "text/") || highlight == "" {
		w.Header().Add("Content-Type", contentType)
		if download != "" {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filepath.Base(path)))
		}
		for _, chunk := range object.Chunks {
			data, err := snap.GetChunk(chunk.Checksum)
			if err != nil {
			}
			w.Write(data)
		}
		return
	}

	content := []byte("")
	for _, chunk := range object.Chunks {
		data, err := snap.GetChunk(chunk.Checksum)
		if err != nil {
		}
		content = append(content, data...)
	}

	lexer := lexers.Match(path)
	if lexer == nil {
		lexer = lexers.Analyse(string(content))
	}
	if lexer == nil {
		w.Header().Add("Content-Type", contentType)
		w.Write(content)
		return
	}

	w.Header().Add("Content-Type", "text/html")
	style := styles.Get("dracula")
	if style == nil {
		style = styles.Fallback
	}
	formatter := formatters.Get("html")
	if formatter == nil {
		formatter = formatters.Fallback
	}
	iterator, err := lexer.Tokenise(nil, string(content))
	err = formatter.Format(w, style, iterator)
	if err != nil {
		w.Header().Add("Content-Type", contentType)
		w.Write(content)
	}
	return
}

func search_snapshots(w http.ResponseWriter, r *http.Request) {
	urlParams := r.URL.Query()
	q := urlParams["q"][0]
	queryKind := urlParams["kind"]
	queryMime := urlParams["mime"]
	queryExt := urlParams["ext"]

	kind := ""
	if queryKind != nil {
		kind = queryKind[0]
	} else {
		kind = ""
	}
	mime := ""
	if queryMime != nil {
		mime = queryMime[0]
	} else {
		mime = ""
	}
	ext := ""
	if queryExt != nil {
		ext = queryExt[0]
	} else {
		ext = ""
	}

	snapshots, err := lrepository.GetSnapshots()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	snapshotsList := make([]*snapshot.Snapshot, 0)
	for _, indexID := range snapshots {
		snapshot, err := snapshot.Load(lrepository, indexID)
		if err != nil {
			/* failed to lookup snapshot */
			continue
		}
		snapshotsList = append(snapshotsList, snapshot)
	}
	sort.Slice(snapshotsList, func(i, j int) bool {
		return snapshotsList[i].Header.CreationTime.Before(snapshotsList[j].Header.CreationTime)
	})

	directories := make([]struct {
		Snapshot string
		Date     string
		Path     string
	}, 0)
	files := make([]struct {
		Snapshot string
		Date     string
		Mime     string
		Path     string
	}, 0)
	for _, snap := range snapshotsList {
		fs, err := snap.Filesystem()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if kind == "" && mime == "" && ext == "" {
			for directory := range fs.Directories() {
				if strings.Contains(directory, q) {
					directories = append(directories, struct {
						Snapshot string
						Date     string
						Path     string
					}{hex.EncodeToString(snap.Header.SnapshotID[:]), snap.Header.CreationTime.String(), directory})
				}
			}
		}
		for file := range fs.Pathnames() {
			if strings.Contains(file, q) {
				fsinfo, err := fs.Stat(file)
				if err != nil {
					continue
				}
				if _, isDir := fsinfo.(*vfs.DirEntry); isDir {
					continue
				}
				object, err := snap.LookupObject(fsinfo.(*vfs.FileEntry).Checksum)
				if err != nil {
					continue
				}
				if kind != "" && !strings.HasPrefix(object.ContentType, kind+"/") {
					continue
				}
				if mime != "" && !strings.HasPrefix(object.ContentType, mime) {
					continue
				}
				if ext != "" && filepath.Ext(file) != ext {
					continue
				}

				files = append(files, struct {
					Snapshot string
					Date     string
					Mime     string
					Path     string
				}{hex.EncodeToString(snap.Header.SnapshotID[:]), snap.Header.CreationTime.String(), object.ContentType, file})
			}
		}
	}
	sort.Slice(directories, func(i, j int) bool {
		return directories[i].Date < directories[j].Date && strings.Compare(directories[i].Path, directories[j].Path) < 0
	})
	sort.Slice(files, func(i, j int) bool {
		return files[i].Date < files[j].Date && strings.Compare(files[i].Path, files[j].Path) < 0
	})

	ctx := &struct {
		SearchTerms string
		Directories []struct {
			Snapshot string
			Date     string
			Path     string
		}
		Files []struct {
			Snapshot string
			Date     string
			Mime     string
			Path     string
		}
	}{q, directories, files}
	templates["search"].Execute(w, ctx)
}

func Ui(repo *repository.Repository, addr string, spawn bool) error {
	lrepository = repo
	lcache = nil

	templates = make(map[string]*template.Template)

	t, err := template.New("repository").Funcs(template.FuncMap{
		"humanizeBytes": humanize.Bytes,
		"IDtoHex":       func(b [32]byte) string { return hex.EncodeToString(b[:]) },
		"ShortIDtoHex":  func(b []byte) string { return hex.EncodeToString(b) },
	}).Parse(baseTemplate + repositoryTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("browse").Funcs(template.FuncMap{
		"humanizeBytes": humanize.Bytes,
		"IDtoHex":       func(b [32]byte) string { return hex.EncodeToString(b[:]) },
		"ShortIDtoHex":  func(b []byte) string { return hex.EncodeToString(b) },
	}).Parse(baseTemplate + browseTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("object").Funcs(template.FuncMap{
		"humanizeBytes": humanize.Bytes,
		"IDtoHex":       func(b [32]byte) string { return hex.EncodeToString(b[:]) },
		"ShortIDtoHex":  func(b []byte) string { return hex.EncodeToString(b) },
	}).Parse(baseTemplate + objectTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	t, err = template.New("search").Funcs(template.FuncMap{
		"humanizeBytes": humanize.Bytes,
		"IDtoHex":       func(b [32]byte) string { return hex.EncodeToString(b[:]) },
		"ShortIDtoHex":  func(b []byte) string { return hex.EncodeToString(b) },
	}).Parse(baseTemplate + searchTemplate)
	if err != nil {
		panic(err)
	}
	templates[t.Name()] = t

	var url string
	if addr != "" {
		url = fmt.Sprintf("http://%s", addr)
	} else {
		var port uint16
		for {
			port = uint16(rand.Uint32() % 0xffff)
			if port >= 1024 {
				break
			}
		}
		addr = fmt.Sprintf("localhost:%d", port)
		url = fmt.Sprintf("http://%s", addr)
	}
	fmt.Println("lauching browser UI pointing at", url)
	if spawn {
		switch runtime.GOOS {
		case "windows":
			err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
		case "darwin":
			err = exec.Command("open", url).Start()
		default: // "linux", "freebsd", "openbsd", "netbsd"
			err = exec.Command("xdg-open", url).Start()
		}
		if err != nil {
			return err
		}
	}

	r := mux.NewRouter()
	r.HandleFunc("/", viewRepository)
	r.HandleFunc("/snapshot/{snapshot}:/", browse)
	r.HandleFunc("/snapshot/{snapshot}:{path:.+}/", browse)
	r.HandleFunc("/raw/{snapshot}:{path:.+}", raw)
	r.HandleFunc("/snapshot/{snapshot}:{path:.+}", object)

	r.HandleFunc("/search", search_snapshots)

	return http.ListenAndServe(addr, r)
}
