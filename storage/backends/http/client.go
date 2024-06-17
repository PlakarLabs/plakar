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

package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
)

type Repository struct {
	config     storage.RepositoryConfig
	Repository string
}

func init() {
	network.ProtocolRegister()
	storage.Register("http", NewRepository)
}

func NewRepository() storage.RepositoryBackend {
	return &Repository{}
}

func (r *Repository) sendRequest(method string, url string, requestType string, payload interface{}) (*http.Response, error) {
	requestBody, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, url+requestType, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	return client.Do(req)
}

func (repository *Repository) Create(location string, config storage.RepositoryConfig) error {
	return nil
}

func (repository *Repository) Open(location string) error {
	repository.Repository = location
	r, err := repository.sendRequest("GET", location, "/", network.ReqOpen{
		Repository: "",
	})
	if err != nil {
		return err
	}

	var resOpen network.ResOpen
	if err := json.NewDecoder(r.Body).Decode(&resOpen); err != nil {
		return err
	}
	if resOpen.Err != "" {
		return fmt.Errorf("%s", resOpen.Err)
	}

	repository.config = *resOpen.RepositoryConfig
	return nil
}

func (repository *Repository) Close() error {
	r, err := repository.sendRequest("POST", repository.Repository, "/", network.ReqClose{
		Uuid: repository.config.RepositoryID.String(),
	})
	if err != nil {
		return err
	}

	var resClose network.ResClose
	if err := json.NewDecoder(r.Body).Decode(&resClose); err != nil {
		return err
	}
	if resClose.Err != "" {
		return fmt.Errorf("%s", resClose.Err)
	}

	return nil
}

func (repository *Repository) Configuration() storage.RepositoryConfig {
	return repository.config
}

// snapshots
func (repository *Repository) GetSnapshots() ([]uuid.UUID, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/snapshots", network.ReqGetSnapshots{})
	if err != nil {
		return nil, err
	}

	var resGetSnapshots network.ResGetSnapshots
	if err := json.NewDecoder(r.Body).Decode(&resGetSnapshots); err != nil {
		return nil, err
	}
	if resGetSnapshots.Err != "" {
		return nil, fmt.Errorf("%s", resGetSnapshots.Err)
	}
	return resGetSnapshots.Snapshots, nil
}

func (repository *Repository) PutSnapshot(indexID uuid.UUID, data []byte) error {
	r, err := repository.sendRequest("PUT", repository.Repository, "/snapshot", network.ReqPutSnapshot{
		IndexID: indexID,
		Data:    data,
	})
	if err != nil {
		return err
	}

	var resPutSnapshot network.ResPutSnapshot
	if err := json.NewDecoder(r.Body).Decode(&resPutSnapshot); err != nil {
		return err
	}
	if resPutSnapshot.Err != "" {
		return fmt.Errorf("%s", resPutSnapshot.Err)
	}
	return nil
}

func (repository *Repository) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/snapshot", network.ReqGetSnapshot{
		IndexID: indexID,
	})
	if err != nil {
		return nil, err
	}

	var resGetSnapshot network.ResGetSnapshot
	if err := json.NewDecoder(r.Body).Decode(&resGetSnapshot); err != nil {
		return nil, err
	}
	if resGetSnapshot.Err != "" {
		return nil, fmt.Errorf("%s", resGetSnapshot.Err)
	}
	return resGetSnapshot.Data, nil
}

func (repository *Repository) DeleteSnapshot(indexID uuid.UUID) error {
	r, err := repository.sendRequest("DELETE", repository.Repository, "/snapshot", network.ReqDeleteSnapshot{
		IndexID: indexID,
	})
	if err != nil {
		return err
	}

	var resDeleteSnapshot network.ResDeleteSnapshot
	if err := json.NewDecoder(r.Body).Decode(&resDeleteSnapshot); err != nil {
		return err
	}
	if resDeleteSnapshot.Err != "" {
		return fmt.Errorf("%s", resDeleteSnapshot.Err)
	}
	return nil
}

// locks
func (repository *Repository) GetLocks() ([]uuid.UUID, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/locks", network.ReqGetLocks{})
	if err != nil {
		return nil, err
	}

	var resGetLocks network.ResGetLocks
	if err := json.NewDecoder(r.Body).Decode(&resGetLocks); err != nil {
		return nil, err
	}
	if resGetLocks.Err != "" {
		return nil, fmt.Errorf("%s", resGetLocks.Err)
	}
	return resGetLocks.Locks, nil
}

func (repository *Repository) PutLock(indexID uuid.UUID, data []byte) error {
	r, err := repository.sendRequest("PUT", repository.Repository, "/lock", network.ReqPutLock{
		IndexID: indexID,
		Data:    data,
	})
	if err != nil {
		return err
	}

	var resPutLock network.ResPutLock
	if err := json.NewDecoder(r.Body).Decode(&resPutLock); err != nil {
		return err
	}
	if resPutLock.Err != "" {
		return fmt.Errorf("%s", resPutLock.Err)
	}
	return nil
}

func (repository *Repository) GetLock(indexID uuid.UUID) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/lock", network.ReqGetLock{
		IndexID: indexID,
	})
	if err != nil {
		return nil, err
	}

	var resGetLock network.ResGetLock
	if err := json.NewDecoder(r.Body).Decode(&resGetLock); err != nil {
		return nil, err
	}
	if resGetLock.Err != "" {
		return nil, fmt.Errorf("%s", resGetLock.Err)
	}
	return resGetLock.Data, nil
}

func (repository *Repository) DeleteLock(indexID uuid.UUID) error {
	r, err := repository.sendRequest("DELETE", repository.Repository, "/lock", network.ReqDeleteLock{
		IndexID: indexID,
	})
	if err != nil {
		return err
	}

	var resDeleteLock network.ResDeleteLock
	if err := json.NewDecoder(r.Body).Decode(&resDeleteLock); err != nil {
		return err
	}
	if resDeleteLock.Err != "" {
		return fmt.Errorf("%s", resDeleteLock.Err)
	}
	return nil
}

// blobs
func (repository *Repository) GetBlobs() ([][32]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/blobs", network.ReqGetBlobs{})
	if err != nil {
		return nil, err
	}

	var resGetBlobs network.ResGetBlobs
	if err := json.NewDecoder(r.Body).Decode(&resGetBlobs); err != nil {
		return nil, err
	}
	if resGetBlobs.Err != "" {
		return nil, fmt.Errorf("%s", resGetBlobs.Err)
	}
	return resGetBlobs.Checksums, nil
}

func (repository *Repository) PutBlob(checksum [32]byte, data []byte) error {
	r, err := repository.sendRequest("PUT", repository.Repository, "/blob", network.ReqPutBlob{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}

	var resPutBlob network.ResPutBlob
	if err := json.NewDecoder(r.Body).Decode(&resPutBlob); err != nil {
		return err
	}
	if resPutBlob.Err != "" {
		return fmt.Errorf("%s", resPutBlob.Err)
	}
	return nil
}

func (repository *Repository) CheckBlob(checksum [32]byte) (bool, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/blob/check", network.ReqCheckBlob{
		Checksum: checksum,
	})
	if err != nil {
		return false, err
	}

	var resCheckBlob network.ResCheckBlob
	if err := json.NewDecoder(r.Body).Decode(&resCheckBlob); err != nil {
		return false, err
	}
	if resCheckBlob.Err != "" {
		return false, fmt.Errorf("%s", resCheckBlob.Err)
	}
	return resCheckBlob.Exists, nil
}

func (repository *Repository) GetBlob(checksum [32]byte) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/blob", network.ReqGetBlob{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}

	var resGetBlob network.ResGetBlob
	if err := json.NewDecoder(r.Body).Decode(&resGetBlob); err != nil {
		return nil, err
	}
	if resGetBlob.Err != "" {
		return nil, fmt.Errorf("%s", resGetBlob.Err)
	}
	return resGetBlob.Data, nil
}

func (repository *Repository) DeleteBlob(checksum [32]byte) error {
	r, err := repository.sendRequest("DELETE", repository.Repository, "/blob", network.ReqDeleteBlob{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}

	var resDeleteBlob network.ResDeleteBlob
	if err := json.NewDecoder(r.Body).Decode(&resDeleteBlob); err != nil {
		return err
	}
	if resDeleteBlob.Err != "" {
		return fmt.Errorf("%s", resDeleteBlob.Err)
	}
	return nil
}

// indexes
func (repository *Repository) GetIndexes() ([][32]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/indexes", network.ReqGetIndexes{})
	if err != nil {
		return nil, err
	}

	var resGetIndexes network.ResGetIndexes
	if err := json.NewDecoder(r.Body).Decode(&resGetIndexes); err != nil {
		return nil, err
	}
	if resGetIndexes.Err != "" {
		return nil, fmt.Errorf("%s", resGetIndexes.Err)
	}
	return resGetIndexes.Checksums, nil
}

func (repository *Repository) PutIndex(checksum [32]byte, data []byte) error {
	r, err := repository.sendRequest("PUT", repository.Repository, "/index", network.ReqPutIndex{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}

	var resPutIndex network.ResPutIndex
	if err := json.NewDecoder(r.Body).Decode(&resPutIndex); err != nil {
		return err
	}
	if resPutIndex.Err != "" {
		return fmt.Errorf("%s", resPutIndex.Err)
	}
	return nil
}

func (repository *Repository) GetIndex(checksum [32]byte) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/index", network.ReqGetIndex{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}

	var resGetIndex network.ResGetIndex
	if err := json.NewDecoder(r.Body).Decode(&resGetIndex); err != nil {
		return nil, err
	}
	if resGetIndex.Err != "" {
		return nil, fmt.Errorf("%s", resGetIndex.Err)
	}
	return resGetIndex.Data, nil
}

func (repository *Repository) DeleteIndex(checksum [32]byte) error {
	r, err := repository.sendRequest("DELETE", repository.Repository, "/index", network.ReqDeleteIndex{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}

	var resDeleteIndex network.ResDeleteIndex
	if err := json.NewDecoder(r.Body).Decode(&resDeleteIndex); err != nil {
		return err
	}
	if resDeleteIndex.Err != "" {
		return fmt.Errorf("%s", resDeleteIndex.Err)
	}
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([][32]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/packfiles", network.ReqGetPackfiles{})
	if err != nil {
		return nil, err
	}

	var resGetPackfiles network.ResGetPackfiles
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfiles); err != nil {
		return nil, err
	}
	if resGetPackfiles.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfiles.Err)
	}
	return resGetPackfiles.Checksums, nil
}

func (repository *Repository) PutPackfile(checksum [32]byte, data []byte) error {
	r, err := repository.sendRequest("PUT", repository.Repository, "/packfile", network.ReqPutPackfile{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}

	var resPutPackfile network.ResPutPackfile
	if err := json.NewDecoder(r.Body).Decode(&resPutPackfile); err != nil {
		return err
	}
	if resPutPackfile.Err != "" {
		return fmt.Errorf("%s", resPutPackfile.Err)
	}
	return nil
}

func (repository *Repository) GetPackfile(checksum [32]byte) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/packfile", network.ReqGetPackfile{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}

	var resGetPackfile network.ResGetPackfile
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfile); err != nil {
		return nil, err
	}
	if resGetPackfile.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfile.Err)
	}
	return resGetPackfile.Data, nil
}

func (repository *Repository) GetPackfileSubpart(checksum [32]byte, offset uint32, length uint32) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/packfile/subpart", network.ReqGetPackfileSubpart{
		Checksum: checksum,
		Offset:   offset,
		Length:   length,
	})
	if err != nil {
		return nil, err
	}

	var resGetPackfileSubpart network.ResGetPackfileSubpart
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfileSubpart); err != nil {
		return nil, err
	}
	if resGetPackfileSubpart.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfileSubpart.Err)
	}
	return resGetPackfileSubpart.Data, nil
}

func (repository *Repository) DeletePackfile(checksum [32]byte) error {
	r, err := repository.sendRequest("DELETE", repository.Repository, "/packfile", network.ReqDeletePackfile{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}

	var resDeletePackfile network.ResDeletePackfile
	if err := json.NewDecoder(r.Body).Decode(&resDeletePackfile); err != nil {
		return err
	}
	if resDeletePackfile.Err != "" {
		return fmt.Errorf("%s", resDeletePackfile.Err)
	}
	return nil
}

func (repository *Repository) Commit(indexID uuid.UUID, data []byte) error {
	r, err := repository.sendRequest("POST", repository.Repository, "/snapshot", network.ReqCommit{
		IndexID: indexID,
		Data:    data,
	})
	if err != nil {
		return err
	}

	var ResCommit network.ResCommit
	if err := json.NewDecoder(r.Body).Decode(&ResCommit); err != nil {
		return err
	}
	if ResCommit.Err != "" {
		return fmt.Errorf("%s", ResCommit.Err)
	}
	return nil
}
