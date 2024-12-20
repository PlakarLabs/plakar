package vfs

import (
	"errors"
	"io"
	"io/fs"
	"iter"
	"log"
	"path/filepath"
	"sort"
	"strings"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/vmihailenco/msgpack/v5"
)

// Entry implements FSEntry and fs.DirEntry, as well as some other
// helper methods.
type Entry struct {
	Version    uint32              `msgpack:"version" json:"version"`
	ParentPath string              `msgpack:"parent_path" json:"parent_path"`
	RecordType importer.RecordType `msgpack:"type" json:"type"`
	FileInfo   objects.FileInfo    `msgpack:"file_info" json:"file_info"`

	/* Directory specific fields */
	Summary *Summary `msgpack:"summary" json:"summary"`

	/* File specific fields */
	SymlinkTarget string          `msgpack:"symlinkTarget,omitempty" json:"symlinkTarget"`
	Object        *objects.Object `msgpack:"object,omitempty" json:"object,omitempty"` // nil for !regular files

	/* Windows specific fields */
	AlternateDataStreams []AlternateDataStream `msgpack:"alternate_data_streams,omitempty" json:"alternate_data_streams"`
	SecurityDescriptor   []byte                `msgpack:"security_descriptor,omitempty" json:"security_descriptor"`
	FileAttributes       uint32                `msgpack:"file_attributes,omitempty" json:"file_attributes"`

	/* Unix fields */
	ExtendedAttributes []ExtendedAttribute `msgpack:"extended_attributes,omitempty" json:"extended_attributes"`

	/* Custom metadata and tags */
	Classifications []Classification `msgpack:"classifications,omitempty" json:"classifications"`
	CustomMetadata  []CustomMetadata `msgpack:"custom_metadata,omitempty" json:"custom_metadata"`
	Tags            []string         `msgpack:"tags,omitempty" json:"tags"`

	/* Errors */
	Errors *objects.Checksum `msgpack:"errors,omitempty" json:"errors,omitempty"`
}

func NewEntry(parentPath string, record *importer.ScanRecord) *Entry {
	target := ""
	if record.Target != "" {
		target = record.Target
	}

	ExtendedAttributes := make([]ExtendedAttribute, 0)
	for name, value := range record.ExtendedAttributes {
		ExtendedAttributes = append(ExtendedAttributes, ExtendedAttribute{
			Name:  name,
			Value: value,
		})
	}

	sort.Slice(ExtendedAttributes, func(i, j int) bool {
		return ExtendedAttributes[i].Name < ExtendedAttributes[j].Name
	})

	entry := &Entry{
		Version:            VERSION,
		RecordType:         record.Type,
		FileInfo:           record.FileInfo,
		SymlinkTarget:      target,
		ExtendedAttributes: ExtendedAttributes,
		Tags:               []string{},
		ParentPath:         parentPath,
	}

	if record.Type == importer.RecordTypeDirectory {
		entry.Summary = &Summary{}
	}

	return entry
}

func EntryFromBytes(bytes []byte) (*Entry, error) {
	entry := Entry{}
	err := msgpack.Unmarshal(bytes, &entry)
	return &entry, err
}

func (e *Entry) ToBytes() ([]byte, error) {
	return msgpack.Marshal(e)
}

func (e *Entry) ContentType() string {
	if e.Object == nil {
		return ""
	}
	return e.Object.ContentType
}

func (e *Entry) Entropy() float64 {
	if e.Object == nil {
		return 0
	}
	return e.Object.Entropy
}

func (e *Entry) AddClassification(analyzer string, classes []string) {
	e.Classifications = append(e.Classifications, Classification{
		Analyzer: analyzer,
		Classes:  classes,
	})
}

func (e *Entry) Open(fs *Filesystem, path string) fs.File {
	if e.FileInfo.IsDir() {
		return &vdir{
			path:  path,
			entry: e,
			fs:    fs,
		}
	}

	return &vfile{
		path:  path,
		entry: e,
		repo:  fs.repo,
	}
}

func (e *Entry) Getdents(fsc *Filesystem) (iter.Seq2[*Entry, error], error) {
	path := filepath.Join(e.ParentPath, e.FileInfo.Name())
	iter, err := fsc.tree.ScanFrom(path)
	if err != nil {
		return nil, err
	}

	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	prefix := path
	return func(yield func(*Entry, error) bool) {
		for iter.Next() {
			path, entry := iter.Current()
			if !strings.HasPrefix(path, prefix) {
				break
			}
			if strings.Index(path[:len(prefix)], "/") != -1 {
				break
			}
			if !yield(&entry, nil) {
				return
			}
		}
		if err := iter.Err(); err != nil {
			yield(nil, err)
		}
	}, nil
}

func (e *Entry) Stat() *objects.FileInfo {
	return &e.FileInfo
}

func (e *Entry) Name() string {
	return e.FileInfo.Name()
}

func (e *Entry) Size() int64 {
	return e.FileInfo.Size()
}

func (e *Entry) Path() string {
	return filepath.Join(e.ParentPath, e.FileInfo.Name())
}

func (e *Entry) IsDir() bool {
	return e.FileInfo.IsDir()
}

func (e *Entry) Type() fs.FileMode {
	return e.Stat().Mode()
}

func (e *Entry) Info() (fs.FileInfo, error) {
	return e.FileInfo, nil
}

// FileEntry implements fs.File and FSEntry
type vfile struct {
	path   string
	entry  *Entry
	repo   *repository.Repository
	closed bool
	objoff int
	rd     io.Reader
}

func (vf *vfile) Stat() (fs.FileInfo, error) {
	if vf.closed {
		return nil, fs.ErrClosed
	}
	return vf.entry.FileInfo, nil
}

func (vf *vfile) Name() string {
	return vf.entry.FileInfo.Name()
}

func (vf *vfile) Size() int64 {
	return vf.entry.FileInfo.Size()
}

func (vf *vfile) Path() string {
	return vf.path
}

func (vf *vfile) Read(p []byte) (int, error) {
	if vf.closed {
		return 0, fs.ErrClosed
	}

	if vf.entry.Object == nil {
		return 0, fs.ErrInvalid
	}

	for vf.objoff < len(vf.entry.Object.Chunks) {
		if vf.rd == nil {
			rd, _, err := vf.repo.GetBlob(packfile.TYPE_CHUNK,
				vf.entry.Object.Chunks[vf.objoff].Checksum)
			if err != nil {
				return -1, err
			}
			vf.rd = rd
		}

		n, err := vf.rd.Read(p)
		if errors.Is(err, io.EOF) {
			vf.objoff++
			vf.rd = nil
			continue
		}
		return n, err
	}

	return 0, io.EOF
}

func (vf *vfile) Seek(offset int64, whence int) (int64, error) {
	if vf.closed {
		return 0, fs.ErrClosed
	}
	return 0, nil
}

func (vf *vfile) Close() error {
	if vf.closed {
		return fs.ErrClosed
	}
	vf.closed = true
	return nil
}

type vdir struct {
	path   string
	entry  *Entry
	fs     *Filesystem
	iter   btree.Iterator[string, objects.Checksum, Entry]
	closed bool
}

func (vf *vdir) Stat() (fs.FileInfo, error) {
	if vf.closed {
		return nil, fs.ErrClosed
	}
	return vf.entry.FileInfo, nil
}

func (vf *vdir) Read(p []byte) (int, error) {
	if vf.closed {
		return 0, fs.ErrClosed
	}
	return 0, fs.ErrInvalid
}

func (vf *vdir) Seek(offset int64, whence int) (int64, error) {
	if vf.closed {
		return 0, fs.ErrClosed
	}
	return 0, fs.ErrInvalid
}

func (vf *vdir) Close() error {
	if vf.closed {
		return fs.ErrClosed
	}
	vf.closed = true
	return nil
}

func (vf *vdir) ReadDir(n int) (entries []fs.DirEntry, err error) {
	if vf.closed {
		return entries, fs.ErrClosed
	}

	prefix := vf.path
	if prefix != "/" {
		prefix += "/"
	}

	if vf.iter == nil {
		log.Println("reading dir", prefix, "with batch", n)
		vf.iter, err = vf.fs.tree.ScanFrom(prefix)
		if err != nil {
			return
		}
	}

	for vf.iter.Next() {
		if n == 0 {
			break
		}
		if n > 0 {
			n--
		}
		path, dirent := vf.iter.Current()

		log.Println("considering entry", path)
		if path == prefix {
			log.Println("it's the same as prefix; next")
			continue
		}

		if !strings.HasPrefix(path, prefix) {
			log.Println("is not under", prefix, "; next")
			break
		}
		if strings.Index(path[len(prefix):], "/") != -1 {
			log.Println("it's too deep;", prefix)
			break
		}

		entries = append(entries, &vdirent{dirent})
	}

	if len(entries) == 0 && n != -1 {
		err = io.EOF
	}
	if e := vf.iter.Err(); e != nil {
		err = e
	}
	return
}

type vdirent struct {
	Entry
}

func (dirent *vdirent) Name() string {
	return dirent.FileInfo.Lname
}

func (dirent *vdirent) IsDir() bool {
	return dirent.FileInfo.IsDir()
}

func (dirent *vdirent) Type() fs.FileMode {
	return dirent.FileInfo.Lmode
}

func (dirent *vdirent) Info() (fs.FileInfo, error) {
	return dirent.FileInfo, nil
}
