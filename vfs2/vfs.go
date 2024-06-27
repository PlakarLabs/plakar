package vfs2

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/gob"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/syndtr/goleveldb/leveldb"
)

type Filesystem struct {
	dirname string
	db      *leveldb.DB
	hasher  hash.Hash
}

func NewFilesystem() (*Filesystem, error) {
	dname, err := os.MkdirTemp("", "plakar-vfs-")
	if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(dname, nil)
	if err != nil {
		return nil, err
	}
	return &Filesystem{
		dirname: dname,
		db:      db,
		hasher:  sha256.New(),
	}, err
}

func FromBytes(data []byte) (*Filesystem, error) {
	dname, err := os.MkdirTemp("", "plakar-vfs-")
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(data)
	gr, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	tr := tar.NewReader(gr)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return nil, err
		}

		target := filepath.Join(dname, hdr.Name)

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(hdr.Mode)); err != nil {
				return nil, err

			}
		case tar.TypeReg:
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(hdr.Mode))
			if err != nil {
				return nil, err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return nil, err
			}
			f.Close()
		}
	}

	db, err := leveldb.OpenFile(dname, nil)
	if err != nil {
		return nil, err
	}
	return &Filesystem{
		dirname: dname,
		db:      db,
		hasher:  sha256.New(),
	}, err
}

func (fsc *Filesystem) Checksum() []byte {
	return fsc.hasher.Sum(nil)
}

func (fsc *Filesystem) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	err := filepath.Walk(fsc.dirname, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		hdr, err := tar.FileInfoHeader(fi, file)
		if err != nil {
			return err
		}

		hdr.Name = filepath.ToSlash(file[len(fsc.dirname):])

		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		if !fi.Mode().IsRegular() {
			return nil
		}

		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := io.Copy(tw, f); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

func (fsc *Filesystem) Close() error {
	if err := fsc.db.Close(); err != nil {
		return err
	}
	return os.RemoveAll(fsc.dirname)
}

func (fsc *Filesystem) Record(path string, fileinfo objects.FileInfo) error {
	var fibuf bytes.Buffer
	err := gob.NewEncoder(&fibuf).Encode(fileinfo)
	if err != nil {
		return err
	}

	storedPath := path
	if fileinfo.Mode().IsDir() {
		if path != "/" {
			storedPath = path + string(filepath.Separator)
		}
	}

	fsc.hasher.Write([]byte(storedPath))
	fsc.hasher.Write(fibuf.Bytes())

	return fsc.db.Put([]byte(storedPath), fibuf.Bytes(), nil)
}

func (fsc *Filesystem) Scan() <-chan string {
	ch := make(chan string)
	go func() {
		iter := fsc.db.NewIterator(nil, nil)
		for iter.Next() {
			var key string
			if iter.Key()[len(iter.Key())-1] == '/' {
				if string(iter.Key()) != "/" {
					key = string(iter.Key()[:len(iter.Key())-1])
				} else {
					key = "/"
				}
			} else {
				key = string(iter.Key())
			}
			ch <- key
		}
		close(ch)
	}()
	return ch
}

func (fsc *Filesystem) Directories() <-chan string {
	ch := make(chan string)
	go func() {
		iter := fsc.db.NewIterator(nil, nil)
		for iter.Next() {
			if iter.Key()[len(iter.Key())-1] == '/' {
				var key string
				if string(iter.Key()) != "/" {
					key = string(iter.Key()[:len(iter.Key())-1])
				} else {
					key = "/"
				}
				ch <- key
			}
		}
		close(ch)
	}()
	return ch
}

func (fsc *Filesystem) Children(path string) (<-chan string, error) {
	storedPath := path
	if path != "/" {
		storedPath = path + string(filepath.Separator)
	}
	_, err := fsc.db.Get([]byte(storedPath), nil)
	if err != nil {
		return nil, err
	}

	ch := make(chan string)

	go func() {
		defer close(ch)
		prefix := []byte(storedPath)
		iter := fsc.db.NewIterator(nil, nil)
		defer iter.Release()
		for iter.Seek(prefix); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
			key := iter.Key()
			relativePath := string(key[len(prefix):])
			atoms := strings.Split(relativePath, string(filepath.Separator))
			if len(atoms) == 1 && atoms[0] == "" {
				continue
			} else if len(atoms) == 1 || (len(atoms) == 2 && atoms[1] == "") {
				ch <- atoms[0]
			} else {
				continue
			}
		}
	}()

	return ch, nil
}

func (fsc *Filesystem) Files() <-chan string {
	ch := make(chan string)
	go func() {
		iter := fsc.db.NewIterator(nil, nil)
		for iter.Next() {
			if iter.Key()[len(iter.Key())-1] != '/' {
				ch <- string(iter.Key())
			}
		}
		close(ch)
	}()
	return ch
}

func (fsc *Filesystem) Pathnames() <-chan string {
	ch := make(chan string)
	go func() {
		iter := fsc.db.NewIterator(nil, nil)
		for iter.Next() {
			var key string
			if iter.Key()[len(iter.Key())-1] == '/' {
				if string(iter.Key()) != "/" {
					key = string(iter.Key()[:len(iter.Key())-1])
				} else {
					key = "/"
				}
			} else {
				key = string(iter.Key())
			}
			ch <- key
		}
		close(ch)
	}()
	return ch
}

func (fsc *Filesystem) Stat(path string) (*objects.FileInfo, error) {
	storedPath := path
	if path != "/" {
		storedPath = path + string(filepath.Separator)
	}

	// first check if the path is a directory
	data, err := fsc.db.Get([]byte(storedPath), nil)
	if err != nil {
		// then check if the path is a file
		data, err = fsc.db.Get([]byte(path), nil)
		if err != nil {
			return nil, err
		}
	}

	var fileinfo *objects.FileInfo
	err = gob.NewDecoder(bytes.NewBuffer(data)).Decode(&fileinfo)
	if err != nil {
		return nil, err
	}
	return fileinfo, nil
}
