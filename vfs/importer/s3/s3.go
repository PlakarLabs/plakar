/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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

package s3

import (
	"context"
	"io"
	"io/fs"
	"log"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/PlakarLabs/plakar/vfs"
	"github.com/PlakarLabs/plakar/vfs/importer"
)

type S3Importer struct {
	importer.ImporterBackend

	location    string
	minioClient *minio.Client
	bucketName  string
}

func init() {
	importer.Register("s3", NewS3Importer)
}

func NewS3Importer() importer.ImporterBackend {
	return &S3Importer{}
}

func (provider *S3Importer) connect(location *url.URL) error {
	endpoint := location.Host
	accessKeyID := location.User.Username()
	secretAccessKey, _ := location.User.Password()
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	provider.minioClient = minioClient
	return nil
}

func (p *S3Importer) Scan() (<-chan importer.ImporterRecord, <-chan error, error) {
	parsed, err := url.Parse(p.location)
	if err != nil {
		return nil, nil, err
	}

	err = p.connect(parsed)
	if err != nil {
		return nil, nil, err
	}
	p.bucketName = parsed.RequestURI()[1:]

	c := make(chan importer.ImporterRecord)
	cerr := make(chan error)

	go func() {
		directories := make(map[string]vfs.FileInfo)
		files := make(map[string]vfs.FileInfo)
		ino := uint64(0)
		fi := vfs.NewFileInfo(
			"/",
			0,
			0700|fs.ModeDir,
			time.Now(),
			0,
			ino,
			0,
			0,
		)
		directories["/"] = fi
		ino++

		for object := range p.minioClient.ListObjects(context.Background(), p.bucketName, minio.ListObjectsOptions{Prefix: "", Recursive: true}) {
			atoms := strings.Split(object.Key, "/")

			for i := 0; i < len(atoms)-1; i++ {
				dir := strings.Join(atoms[0:i+1], "/")
				if _, exists := directories[dir]; !exists {
					fi := vfs.NewFileInfo(
						atoms[i],
						0,
						0700|fs.ModeDir,
						time.Now(),
						0,
						ino,
						0,
						0,
					)
					directories["/"+dir] = fi
					ino++
				}
			}

			stat := vfs.NewFileInfo(
				atoms[len(atoms)-1],
				object.Size,
				0700,
				object.LastModified,
				0,
				ino,
				0,
				0,
			)
			ino++
			files["/"+object.Key] = stat
		}

		directoryNames := make([]string, 0)
		for name := range directories {
			directoryNames = append(directoryNames, name)
		}

		fileNames := make([]string, 0)
		for name := range files {
			fileNames = append(fileNames, name)
		}

		sort.Slice(directoryNames, func(i, j int) bool {
			return len(directoryNames[i]) < len(directoryNames[j])
		})
		sort.Slice(fileNames, func(i, j int) bool {
			return len(fileNames[i]) < len(fileNames[j])
		})

		for _, directory := range directoryNames {
			c <- importer.ImporterRecord{Pathname: directory, Stat: directories[directory]}
		}
		for _, filename := range fileNames {
			c <- importer.ImporterRecord{Pathname: filename, Stat: files[filename]}
		}

		//fmt.Println(files)
		close(cerr)
		close(c)
	}()
	return c, cerr, nil
}

func (p *S3Importer) Open(pathname string) (io.ReadCloser, error) {
	obj, err := p.minioClient.GetObject(context.Background(), p.bucketName, pathname,
		minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (p *S3Importer) Begin(location string) error {
	p.location = location
	return nil
}

func (p *S3Importer) End() error {
	return nil
}
