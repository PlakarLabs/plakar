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

package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Repository struct {
	config      storage.Configuration
	Repository  string
	minioClient *minio.Client
	bucketName  string
}

func init() {
	network.ProtocolRegister()
	storage.Register("s3", NewRepository)
}

func NewRepository() storage.Backend {
	return &Repository{}
}

func (repository *Repository) connect(location *url.URL) error {
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

	repository.minioClient = minioClient
	return nil
}

func (repository *Repository) Create(location string, config storage.Configuration) error {
	parsed, err := url.Parse(location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}
	repository.bucketName = parsed.RequestURI()[1:]

	err = repository.minioClient.MakeBucket(context.Background(), repository.bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return err
	}

	jconfig, err := msgpack.Marshal(config)
	if err != nil {
		return err
	}

	compressedConfig, err := compression.Deflate("gzip", jconfig)
	if err != nil {
		return err
	}

	_, err = repository.minioClient.PutObject(context.Background(), repository.bucketName, "CONFIG", bytes.NewReader(compressedConfig), int64(len(compressedConfig)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	repository.config = config
	return nil
}

func (repository *Repository) Open(location string) error {
	parsed, err := url.Parse(location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}

	repository.bucketName = parsed.RequestURI()[1:]

	exists, err := repository.minioClient.BucketExists(context.Background(), repository.bucketName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("bucket does not exist")
	}

	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, "CONFIG", minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	stat, err := object.Stat()
	if err != nil {
		return err
	}

	compressed := make([]byte, stat.Size)
	_, err = object.Read(compressed)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}
	object.Close()

	jconfig, err := compression.Inflate("gzip", compressed)
	if err != nil {
		return err
	}

	var config storage.Configuration
	err = msgpack.Unmarshal(jconfig, &config)
	if err != nil {
		return err
	}

	repository.config = config

	return nil
}

func (repository *Repository) Close() error {
	return nil
}

func (repository *Repository) Configuration() storage.Configuration {
	return repository.config
}

// snapshots
func (repository *Repository) GetSnapshots() ([][32]byte, error) {
	ret := make([][32]byte, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "snapshots/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "snapshots/") && len(object.Key) == 13 {
			snapshotIDhex, err := hex.DecodeString(object.Key[13:])
			if err != nil {
				continue
			}
			if len(snapshotIDhex) != 64 {
				continue
			}
			var snapshotID [32]byte
			copy(snapshotID[:], snapshotIDhex)
			ret = append(ret, snapshotID)
		}
	}
	return ret, nil
}

func (repository *Repository) PutSnapshot(snapshotID [32]byte, data []byte) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("snapshots/%x/%s", snapshotID[0], hex.EncodeToString(snapshotID[:])), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetSnapshot(snapshotID [32]byte) ([]byte, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("snapshots/%x/%s", snapshotID[0], hex.EncodeToString(snapshotID[:])), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, err
	}

	dataBytes := make([]byte, stat.Size)
	_, err = object.Read(dataBytes)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}
	object.Close()

	return dataBytes, nil
}

func (repository *Repository) DeleteSnapshot(snapshotID [32]byte) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("snapshots/%x/%s", snapshotID[0], hex.EncodeToString(snapshotID[:])), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

// states
func (repository *Repository) GetStates() ([][32]byte, error) {
	ret := make([][32]byte, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "states/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "states/") && len(object.Key) >= 11 {
			t, err := hex.DecodeString(object.Key[11:])
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			var t32 [32]byte
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *Repository) PutState(checksum [32]byte, rd io.Reader, size int64) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", checksum[0], checksum), rd, size, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetState(checksum [32]byte) (io.Reader, int64, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, err
	}

	stat, err := object.Stat()
	if err != nil {
		return nil, 0, err
	}

	return object, stat.Size, nil
}

func (repository *Repository) DeleteState(checksum [32]byte) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", checksum[0], checksum), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([][32]byte, error) {
	ret := make([][32]byte, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "packfiles/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "packfiles/") && len(object.Key) >= 13 {
			t, err := hex.DecodeString(object.Key[13:])
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			var t32 [32]byte
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *Repository) PutPackfile(checksum [32]byte, rd io.Reader, size int64) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", checksum[0], checksum), rd, size, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetPackfile(checksum [32]byte) (io.Reader, int64, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, err
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, 0, err
	}
	return object, stat.Size, nil
}

func (repository *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) (io.Reader, int64, error) {
	opts := minio.GetObjectOptions{}
	opts.SetRange(int64(offset), int64(offset+length))
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", checksum[0], checksum), opts)
	if err != nil {
		return nil, 0, err
	}
	return object, int64(length), nil
}

func (repository *Repository) DeletePackfile(checksum [32]byte) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", checksum[0], checksum), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

//////

func (repository *Repository) Commit(snapshotID [32]byte, data []byte) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("snapshots/%x/%s", snapshotID[0], hex.EncodeToString(snapshotID[:])), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}
