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

package plakard

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"

	"sync"

	"github.com/PlakarLabs/plakar/cache"
)

type Repository struct {
	config storage.RepositoryConfig

	Cache *cache.Cache

	encoder *gob.Encoder
	decoder *gob.Decoder
	mu      sync.Mutex

	Repository string

	inflightRequests map[uuid.UUID]chan network.Request
	notifications    chan network.Request
}

func init() {
	network.ProtocolRegister()
	storage.Register("plakard", NewRepository)
}

func NewRepository() storage.RepositoryBackend {
	return &Repository{}
}

func (repository *Repository) connect(location *url.URL) error {
	scheme := location.Scheme
	switch scheme {
	case "plakar":
		err := repository.connectTCP(location)
		if err != nil {
			return err
		}
	case "ssh":
		err := repository.connectSSH(location)
		if err != nil {
			return err
		}
	case "stdio":
		err := repository.connectStdio(location)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported protocol")
	}

	return nil
}

func (repository *Repository) connectTCP(location *url.URL) error {
	port := location.Port()
	if port == "" {
		port = "9876"
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", location.Hostname()+":"+port)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Fatal(err)
	}

	repository.encoder = gob.NewEncoder(conn)
	repository.decoder = gob.NewDecoder(conn)

	repository.inflightRequests = make(map[uuid.UUID]chan network.Request)
	repository.notifications = make(chan network.Request)

	//repository.maxConcurrentRequest = make(chan bool, 1024)

	go func() {
		for m := range repository.notifications {
			repository.mu.Lock()
			notify := repository.inflightRequests[m.Uuid]
			repository.mu.Unlock()
			notify <- m
		}
	}()

	go func() {
		for {
			result := network.Request{}
			err = repository.decoder.Decode(&result)
			if err != nil {
				conn.Close()
				return
			}
			repository.notifications <- result
		}
	}()

	return err
}

func (repository *Repository) connectStdio(location *url.URL) error {
	subProcess := exec.Command("plakar", "-no-cache", "stdio")

	stdin, err := subProcess.StdinPipe()
	if err != nil {
		return err
	}

	stdout, err := subProcess.StdoutPipe()
	if err != nil {
		return err
	}
	subProcess.Stderr = os.Stderr

	repository.encoder = gob.NewEncoder(stdin)
	repository.decoder = gob.NewDecoder(stdout)

	if err = subProcess.Start(); err != nil {
		return err
	}

	repository.inflightRequests = make(map[uuid.UUID]chan network.Request)
	repository.notifications = make(chan network.Request)

	go func() {
		for m := range repository.notifications {
			repository.mu.Lock()
			notify := repository.inflightRequests[m.Uuid]
			repository.mu.Unlock()
			notify <- m
		}
	}()

	go func() {
		for {
			result := network.Request{}
			err = repository.decoder.Decode(&result)
			if err != nil {
				stdin.Close()
				subProcess.Wait()
				return
			}
			repository.notifications <- result
		}
	}()

	return nil
}

func (repository *Repository) connectSSH(location *url.URL) error {
	connectUrl := "ssh://"
	if location.User != nil {
		connectUrl += location.User.Username() + "@"
	}
	connectUrl += location.Hostname()
	if location.Port() != "" {
		connectUrl += ":" + location.Port()
	}

	subProcess := exec.Command("ssh", connectUrl, "plakar -no-cache stdio")

	stdin, err := subProcess.StdinPipe()
	if err != nil {
		return err
	}

	stdout, err := subProcess.StdoutPipe()
	if err != nil {
		return err
	}

	subProcess.Stderr = os.Stderr

	repository.encoder = gob.NewEncoder(stdin)
	repository.decoder = gob.NewDecoder(stdout)

	if err = subProcess.Start(); err != nil {
		return err
	}

	repository.inflightRequests = make(map[uuid.UUID]chan network.Request)
	repository.notifications = make(chan network.Request)

	go func() {
		for m := range repository.notifications {
			repository.mu.Lock()
			notify := repository.inflightRequests[m.Uuid]
			repository.mu.Unlock()
			notify <- m
		}
	}()

	go func() {
		for {
			result := network.Request{}
			err = repository.decoder.Decode(&result)
			if err != nil {
				stdin.Close()
				subProcess.Wait()
				return
			}
			repository.notifications <- result
		}
	}()

	return nil
}

func (repository *Repository) sendRequest(Type string, Payload interface{}) (*network.Request, error) {
	Uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	request := network.Request{
		Uuid:    Uuid,
		Type:    Type,
		Payload: Payload,
	}

	notify := make(chan network.Request)
	repository.mu.Lock()
	repository.inflightRequests[request.Uuid] = notify
	repository.mu.Unlock()

	err = repository.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := <-notify

	repository.mu.Lock()
	delete(repository.inflightRequests, request.Uuid)
	repository.mu.Unlock()
	close(notify)

	return &result, nil
}

func (repository *Repository) Create(location string, config storage.RepositoryConfig) error {
	parsed, err := url.Parse(location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}

	result, err := repository.sendRequest("ReqCreate", network.ReqCreate{
		Repository:       parsed.Path,
		RepositoryConfig: config,
	})
	if err != nil {
		return err
	}

	if result.Payload.(network.ResCreate).Err != nil {
		return result.Payload.(network.ResCreate).Err
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

	result, err := repository.sendRequest("ReqOpen", network.ReqOpen{
		Repository: parsed.Path,
	})
	if err != nil {
		return err
	}

	if result.Payload.(network.ResOpen).Err != nil {
		return result.Payload.(network.ResOpen).Err
	}

	repository.config = *result.Payload.(network.ResOpen).RepositoryConfig
	return nil
}

func (repository *Repository) Close() error {
	result, err := repository.sendRequest("ReqClose", nil)
	if err != nil {
		return err
	}

	return result.Payload.(network.ResClose).Err
}

func (repository *Repository) Configuration() storage.RepositoryConfig {
	return repository.config
}

// snapshots
func (repository *Repository) GetSnapshots() ([]uuid.UUID, error) {
	result, err := repository.sendRequest("ReqGetSnapshots", nil)
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetSnapshots).Snapshots, result.Payload.(network.ResGetSnapshots).Err
}

func (repository *Repository) PutSnapshot(indexID uuid.UUID, data []byte) error {
	result, err := repository.sendRequest("ReqPutSnapshot", network.ReqPutSnapshot{
		IndexID: indexID,
		Data:    data,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResPutSnapshot).Err
}

func (repository *Repository) GetSnapshot(indexID uuid.UUID) ([]byte, error) {
	result, err := repository.sendRequest("ReqGetSnapshot", network.ReqGetSnapshot{
		IndexID: indexID,
	})
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetSnapshot).Data, result.Payload.(network.ResGetSnapshot).Err
}

func (repository *Repository) DeleteSnapshot(indexID uuid.UUID) error {
	result, err := repository.sendRequest("ReqDeleteSnapshot", network.ReqDeleteSnapshot{
		IndexID: indexID,
	})
	if err != nil {
		return err
	}

	return result.Payload.(network.ResDeleteSnapshot).Err
}

// locks
func (repository *Repository) GetLocks() ([]uuid.UUID, error) {
	result, err := repository.sendRequest("ReqGetLocks", nil)
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetLocks).Locks, result.Payload.(network.ResGetLocks).Err
}

func (repository *Repository) PutLock(indexID uuid.UUID, data []byte) error {
	result, err := repository.sendRequest("ReqPutLock", network.ReqPutLock{
		IndexID: indexID,
		Data:    data,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResPutLock).Err
}

func (repository *Repository) GetLock(indexID uuid.UUID) ([]byte, error) {
	result, err := repository.sendRequest("ReqGetLock", network.ReqGetLock{
		IndexID: indexID,
	})
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetLock).Data, result.Payload.(network.ResGetLock).Err
}

func (repository *Repository) DeleteLock(indexID uuid.UUID) error {
	result, err := repository.sendRequest("ReqDeleteLock", network.ReqDeleteLock{
		IndexID: indexID,
	})
	if err != nil {
		return err
	}

	return result.Payload.(network.ResDeleteLock).Err
}

// blobs
func (repository *Repository) GetBlobs() ([][32]byte, error) {
	result, err := repository.sendRequest("ReqGetBlobs", nil)
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetBlobs).Checksums, result.Payload.(network.ResGetBlobs).Err
}

func (repository *Repository) PutBlob(checksum [32]byte, data []byte) error {
	result, err := repository.sendRequest("ReqPutBlob", network.ReqPutBlob{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResPutBlob).Err
}

func (repository *Repository) GetBlob(checksum [32]byte) ([]byte, error) {
	result, err := repository.sendRequest("ReqGetBlob", network.ReqGetBlob{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetBlob).Data, result.Payload.(network.ResGetBlob).Err
}

func (repository *Repository) DeleteBlob(checksum [32]byte) error {
	result, err := repository.sendRequest("ReqDeleteBlob", network.ReqDeleteBlob{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResDeleteBlob).Err
}

// indexes
func (repository *Repository) GetIndexes() ([][32]byte, error) {
	result, err := repository.sendRequest("ReqGetIndexes", nil)
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetIndexes).Checksums, result.Payload.(network.ResGetIndexes).Err
}

func (repository *Repository) PutIndex(checksum [32]byte, data []byte) error {
	result, err := repository.sendRequest("ReqPutIndex", network.ReqPutIndex{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResPutIndex).Err
}

func (repository *Repository) GetIndex(checksum [32]byte) ([]byte, error) {
	result, err := repository.sendRequest("ReqGetIndex", network.ReqGetIndex{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetIndex).Data, result.Payload.(network.ResGetIndex).Err
}

func (repository *Repository) DeleteIndex(checksum [32]byte) error {
	result, err := repository.sendRequest("ReqDeleteIndex", network.ReqDeleteIndex{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResDeleteIndex).Err
}

// packfiles
func (repository *Repository) GetPackfiles() ([][32]byte, error) {
	result, err := repository.sendRequest("ReqGetPackfiles", nil)
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetPackfiles).Checksums, result.Payload.(network.ResGetPackfiles).Err
}

func (repository *Repository) PutPackfile(checksum [32]byte, data []byte) error {
	result, err := repository.sendRequest("ReqPutPackfile", network.ReqPutPackfile{
		Checksum: checksum,
		Data:     data,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResPutPackfile).Err
}

func (repository *Repository) GetPackfile(checksum [32]byte) ([]byte, error) {
	result, err := repository.sendRequest("ReqGetPackfile", network.ReqGetPackfile{
		Checksum: checksum,
	})
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetPackfile).Data, result.Payload.(network.ResGetPackfile).Err
}

func (repository *Repository) GetPackfileSubpart(checksum [32]byte, offset uint32, length uint32) ([]byte, error) {
	result, err := repository.sendRequest("ReqGetPackfileSubpart", network.ReqGetPackfileSubpart{
		Checksum: checksum,
		Offset:   offset,
		Length:   length,
	})
	if err != nil {
		return nil, err
	}
	return result.Payload.(network.ResGetPackfileSubpart).Data, result.Payload.(network.ResGetPackfileSubpart).Err
}
func (repository *Repository) DeletePackfile(checksum [32]byte) error {
	result, err := repository.sendRequest("ReqDeletePackfile", network.ReqDeletePackfile{
		Checksum: checksum,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResDeletePackfile).Err
}

func (repository *Repository) Commit(indexID uuid.UUID, data []byte) error {
	result, err := repository.sendRequest("ReqCommit", network.ReqCommit{
		IndexID: indexID,
		Data:    data,
	})
	if err != nil {
		return err
	}
	return result.Payload.(network.ResCommit).Err
}
