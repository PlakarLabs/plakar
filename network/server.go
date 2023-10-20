package network

import (
	"encoding/gob"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
)

func Server(repository *storage.Repository, addr string) {

	ProtocolRegister()

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleConnection(c, c)
	}
}

func Stdio() error {
	ProtocolRegister()
	handleConnection(os.Stdin, os.Stdout)
	return nil
}

func handleConnection(rd io.Reader, wr io.Writer) {
	decoder := gob.NewDecoder(rd)
	encoder := gob.NewEncoder(wr)

	transactions := make(map[uuid.UUID]*storage.Transaction)

	var repository *storage.Repository
	var wg sync.WaitGroup
	Uuid, _ := uuid.NewRandom()
	clientUuid := Uuid.String()

	homeDir := os.Getenv("HOME")

	for {
		request := Request{}
		err := decoder.Decode(&request)
		if err != nil {
			break
		}

		switch request.Type {
		case "ReqCreate":
			wg.Add(1)
			go func() {
				defer wg.Done()

				dirPath := request.Payload.(ReqCreate).Repository
				if dirPath == "" {
					dirPath = filepath.Join(homeDir, ".plakar")
				}

				logger.Trace("%s: Create(%s, %s)", clientUuid, dirPath, request.Payload.(ReqCreate).RepositoryConfig)
				repository, err = storage.Create(dirPath, request.Payload.(ReqCreate).RepositoryConfig)
				result := Request{
					Uuid:    request.Uuid,
					Type:    "ResCreate",
					Payload: ResCreate{Err: err},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqOpen":
			wg.Add(1)
			go func() {
				defer wg.Done()

				dirPath := request.Payload.(ReqOpen).Repository
				if dirPath == "" {
					dirPath = filepath.Join(homeDir, ".plakar")
				}

				logger.Trace("%s: Open(%s)", clientUuid, dirPath)
				repository, err = storage.Open(dirPath)
				var payload ResOpen
				if err != nil {
					payload = ResOpen{RepositoryConfig: nil, Err: err}
				} else {
					config := repository.Configuration()
					payload = ResOpen{RepositoryConfig: &config, Err: nil}
				}
				result := Request{
					Uuid:    request.Uuid,
					Type:    "ResOpen",
					Payload: payload,
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqGetSnapshots":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("%s: GetIndexes", clientUuid)
				snapshots, err := repository.GetSnapshots()
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetIndexes",
					Payload: ResGetSnapshots{
						Snapshots: snapshots,
						Err:       err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqGetSnapshot":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("%s: GetMetadata(%s)", clientUuid, request.Payload.(ReqGetSnapshot).Uuid)
				data, err := repository.GetSnapshot(request.Payload.(ReqGetSnapshot).Uuid)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetSnapshot",
					Payload: ResGetSnapshot{
						Data: data,
						Err:  err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqGetBlob":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("%s: GetBlob(%016x)", clientUuid, request.Payload.(ReqGetBlob).Checksum)
				data, err := repository.GetBlob(request.Payload.(ReqGetBlob).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetBlob",
					Payload: ResGetBlob{
						Data: data,
						Err:  err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqStorePutSnapshot":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("%s: PutSnapshot()", clientUuid, request.Payload.(ReqStorePutSnapshot).IndexID)
				err := repository.PutSnapshot(request.Payload.(ReqStorePutSnapshot).IndexID, request.Payload.(ReqStorePutSnapshot).Data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResStorePutSnapshot",
					Payload: ResStorePutSnapshot{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqStorePutBlob":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("%s: PutBlob(%016x)", clientUuid, request.Payload.(ReqStorePutBlob).Checksum)
				err := repository.PutBlob(request.Payload.(ReqStorePutBlob).Checksum, request.Payload.(ReqStorePutBlob).Data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResStorePutBlob",
					Payload: ResStorePutBlob{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqDeleteSnapshot":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("%s: DeleteSnapshot(%s)", clientUuid, request.Payload.(ReqDeleteSnapshot).Uuid)
				err := repository.DeleteSnapshot(request.Payload.(ReqDeleteSnapshot).Uuid)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResDeleteSnapshot",
					Payload: ResDeleteSnapshot{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqTransaction":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("%s: Transaction(%s)", clientUuid, request.Payload.(ReqTransaction).Uuid)
				tx, err := repository.Transaction(request.Payload.(ReqTransaction).Uuid)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResTransaction",
					Payload: ResTransaction{
						Uuid: tx.GetUuid(),
						Err:  err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
				transactions[tx.GetUuid()] = tx
			}()

		case "ReqCommit":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("%s: Commit()", clientUuid)
				txUuid := request.Payload.(ReqCommit).Transaction
				data := request.Payload.(ReqCommit).Data
				tx := transactions[txUuid]
				err := tx.Commit(data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResCommit",
					Payload: ResCommit{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqClose":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("%s: Close()", clientUuid)
				err := repository.Close()
				repository = nil
				result := Request{
					Uuid: request.Uuid,
					Type: "ResClose",
					Payload: ResClose{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()
		}
	}
	wg.Wait()

	if repository != nil {
		repository.Close()
	}
}
