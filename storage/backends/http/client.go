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
	"io"
	"net/http"

	"github.com/PlakarKorp/plakar/network"
	"github.com/PlakarKorp/plakar/storage"
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

func (repository *Repository) Configuration() storage.Configuration {
	return repository.config
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

	ret := make([][32]byte, len(resGetStates.Checksums))
	for i, checksum := range resGetStates.Checksums {
		ret[i] = checksum
	}
	return ret, nil
}

func (repository *Repository) PutState(checksum [32]byte, rd io.Reader, size uint64) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}

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

func (repository *Repository) GetState(checksum [32]byte) (io.Reader, uint64, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/state", network.ReqGetState{
		Checksum: checksum,
	})
	if err != nil {
		return nil, 0, err
	}

	var resGetState network.ResGetState
	if err := json.NewDecoder(r.Body).Decode(&resGetState); err != nil {
		return nil, 0, err
	}
	if resGetState.Err != "" {
		return nil, 0, fmt.Errorf("%s", resGetState.Err)
	}
	return bytes.NewBuffer(resGetState.Data), uint64(len(resGetState.Data)), nil
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

	ret := make([][32]byte, len(resGetPackfiles.Checksums))
	for i, checksum := range resGetPackfiles.Checksums {
		ret[i] = checksum
	}
	return ret, nil
}

func (repository *Repository) PutPackfile(checksum [32]byte, rd io.Reader, size uint64) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}
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

func (repository *Repository) GetPackfile(checksum [32]byte) (io.Reader, uint64, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/packfile", network.ReqGetPackfile{
		Checksum: checksum,
	})
	if err != nil {
		return nil, 0, err
	}

	var resGetPackfile network.ResGetPackfile
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfile); err != nil {
		return nil, 0, err
	}
	if resGetPackfile.Err != "" {
		return nil, 0, fmt.Errorf("%s", resGetPackfile.Err)
	}
	return bytes.NewBuffer(resGetPackfile.Data), uint64(len(resGetPackfile.Data)), nil
}

func (repository *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) (io.Reader, uint32, error) {
	r, err := repository.sendRequest("GET", repository.Repository, "/packfile/blob", network.ReqGetPackfileBlob{
		Checksum: checksum,
		Offset:   offset,
		Length:   length,
	})
	if err != nil {
		return nil, 0, err
	}

	var resGetPackfileBlob network.ResGetPackfileBlob
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfileBlob); err != nil {
		return nil, 0, err
	}
	if resGetPackfileBlob.Err != "" {
		return nil, 0, fmt.Errorf("%s", resGetPackfileBlob.Err)
	}
	return bytes.NewBuffer(resGetPackfileBlob.Data), uint32(len(resGetPackfileBlob.Data)), nil
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
