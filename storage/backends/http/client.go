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
)

type Repository struct {
	config     storage.Configuration
	Repository string
}

func init() {
	network.ProtocolRegister()
	storage.Register("http", NewRepository)
}

func NewRepository() storage.Backend {
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

func (repository *Repository) Create(location string, config storage.Configuration) error {
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

	repository.config = *resOpen.Configuration
	return nil
}

func (repository *Repository) Close() error {
	r, err := repository.sendRequest("POST", repository.Repository, "/", network.ReqClose{
		Uuid: repository.config.StoreID.String(),
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

func (repository *Repository) Configuration() storage.Configuration {
	return repository.config
}

// snapshots
func (repository *Repository) GetSnapshots() ([][32]byte, error) {
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

func (repository *Repository) PutSnapshot(snapshotID [32]byte, data []byte) error {
	r, err := repository.sendRequest("PUT", repository.Repository, "/snapshot", network.ReqPutSnapshot{
		SnapshotID: snapshotID,
		Data:       data,
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

func (repository *Repository) GetSnapshot(snapshotID [32]byte) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/snapshot", network.ReqGetSnapshot{
		SnapshotID: snapshotID,
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

func (repository *Repository) DeleteSnapshot(snapshotID [32]byte) error {
	r, err := repository.sendRequest("DELETE", repository.Repository, "/snapshot", network.ReqDeleteSnapshot{
		SnapshotID: snapshotID,
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

// states
func (repository *Repository) GetStates() ([][32]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/states", network.ReqGetStates{})
	if err != nil {
		return nil, err
	}

	var resGetStates network.ResGetStates
	if err := json.NewDecoder(r.Body).Decode(&resGetStates); err != nil {
		return nil, err
	}
	if resGetStates.Err != "" {
		return nil, fmt.Errorf("%s", resGetStates.Err)
	}
	return resGetStates.Checksums, nil
}

func (repository *Repository) PutState(checksum [32]byte, data []byte) error {
	r, err := repository.sendRequest("PUT", repository.Repository, "/state", network.ReqPutState{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}

	var resPutState network.ResPutState
	if err := json.NewDecoder(r.Body).Decode(&resPutState); err != nil {
		return err
	}
	if resPutState.Err != "" {
		return fmt.Errorf("%s", resPutState.Err)
	}
	return nil
}

func (repository *Repository) GetState(checksum [32]byte) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/state", network.ReqGetState{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}

	var resGetState network.ResGetState
	if err := json.NewDecoder(r.Body).Decode(&resGetState); err != nil {
		return nil, err
	}
	if resGetState.Err != "" {
		return nil, fmt.Errorf("%s", resGetState.Err)
	}
	return resGetState.Data, nil
}

func (repository *Repository) DeleteState(checksum [32]byte) error {
	r, err := repository.sendRequest("DELETE", repository.Repository, "/state", network.ReqDeleteState{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}

	var resDeleteState network.ResDeleteState
	if err := json.NewDecoder(r.Body).Decode(&resDeleteState); err != nil {
		return err
	}
	if resDeleteState.Err != "" {
		return fmt.Errorf("%s", resDeleteState.Err)
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

func (repository *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) ([]byte, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/packfile/subpart", network.ReqGetPackfileBlob{
		Checksum: checksum,
		Offset:   offset,
		Length:   length,
	})
	if err != nil {
		return nil, err
	}

	var resGetPackfileBlob network.ResGetPackfileBlob
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfileBlob); err != nil {
		return nil, err
	}
	if resGetPackfileBlob.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfileBlob.Err)
	}
	return resGetPackfileBlob.Data, nil
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

func (repository *Repository) Commit(snapshotID [32]byte, data []byte) error {
	r, err := repository.sendRequest("POST", repository.Repository, "/snapshot", network.ReqCommit{
		SnapshotID: snapshotID,
		Data:       data,
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
