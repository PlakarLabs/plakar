package network

import (
	"encoding/gob"
	"fmt"
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

				logger.Trace("server", "%s: Create(%s, %s)", clientUuid, dirPath, request.Payload.(ReqCreate).RepositoryConfig)
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

				logger.Trace("server", "%s: Open(%s)", clientUuid, dirPath)
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

		case "ReqClose":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("server", "%s: Close()", clientUuid)
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

		case "ReqCommit":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("server", "%s: Commit()", clientUuid)
				txUuid := request.Payload.(ReqCommit).Transaction
				data := request.Payload.(ReqCommit).Data
				err := repository.Commit(txUuid, data)
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

			// snapshots
		case "ReqGetSnapshots":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetSnapshots", clientUuid)
				snapshots, err := repository.GetSnapshots()
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetSnapshots",
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

		case "ReqPutSnapshot":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: PutSnapshot()", clientUuid, request.Payload.(ReqPutSnapshot).IndexID)
				err := repository.PutSnapshot(request.Payload.(ReqPutSnapshot).IndexID, request.Payload.(ReqPutSnapshot).Data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResPutSnapshot",
					Payload: ResPutSnapshot{
						Err: err,
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
				logger.Trace("server", "%s: GetMetadata(%s)", clientUuid, request.Payload.(ReqGetSnapshot).Uuid)
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

		case "ReqDeleteSnapshot":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("server", "%s: DeleteSnapshot(%s)", clientUuid, request.Payload.(ReqDeleteSnapshot).Uuid)
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

			// blobs
		case "ReqGetBlobs":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetBlobs()", clientUuid)
				checksums, err := repository.GetBlobs()
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetBlobs",
					Payload: ResGetBlobs{
						Checksums: checksums,
						Err:       err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqPutBlob":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: PutBlob(%016x)", clientUuid, request.Payload.(ReqPutBlob).Checksum)
				err := repository.PutBlob(request.Payload.(ReqPutBlob).Checksum, request.Payload.(ReqPutBlob).Data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResPutBlob",
					Payload: ResPutBlob{
						Err: err,
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
				logger.Trace("server", "%s: GetBlob(%016x)", clientUuid, request.Payload.(ReqGetBlob).Checksum)
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

		case "ReqDeleteBlob":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("server", "%s: DeleteBlob(%s)", clientUuid, request.Payload.(ReqDeleteBlob).Checksum)
				err := repository.DeleteBlob(request.Payload.(ReqDeleteBlob).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResDeleteBlob",
					Payload: ResDeleteBlob{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

			// indexes
		case "ReqGetIndexes":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetIndexes()", clientUuid)
				checksums, err := repository.GetIndexes()
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetIndexes",
					Payload: ResGetIndexes{
						Checksums: checksums,
						Err:       err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqPutIndex":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: PutIndex(%016x)", clientUuid, request.Payload.(ReqPutIndex).Checksum)
				err := repository.PutIndex(request.Payload.(ReqPutIndex).Checksum, request.Payload.(ReqPutIndex).Data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResPutIndex",
					Payload: ResPutIndex{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqGetIndex":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetIndex(%016x)", clientUuid, request.Payload.(ReqGetIndex).Checksum)
				data, err := repository.GetIndex(request.Payload.(ReqGetIndex).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetIndex",
					Payload: ResGetIndex{
						Data: data,
						Err:  err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqDeleteIndex":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("server", "%s: DeleteIndex(%s)", clientUuid, request.Payload.(ReqDeleteIndex).Checksum)
				err := repository.DeleteIndex(request.Payload.(ReqDeleteIndex).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResDeleteIndex",
					Payload: ResDeleteIndex{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

			// packfiles
		case "ReqGetPackfiles":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetPackfiles()", clientUuid)
				checksums, err := repository.GetPackfiles()
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetPackfiles",
					Payload: ResGetPackfiles{
						Checksums: checksums,
						Err:       err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqPutPackfile":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: PutPackfile(%016x)", clientUuid, request.Payload.(ReqPutPackfile).Checksum)
				err := repository.PutPackfile(request.Payload.(ReqPutPackfile).Checksum, request.Payload.(ReqPutPackfile).Data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResPutPackfile",
					Payload: ResPutPackfile{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqGetPackfile":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetPackfile(%016x)", clientUuid, request.Payload.(ReqGetPackfile).Checksum)
				data, err := repository.GetPackfile(request.Payload.(ReqGetPackfile).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetPackfile",
					Payload: ResGetPackfile{
						Data: data,
						Err:  err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqDeletePackfile":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("server", "%s: DeletePackfile(%s)", clientUuid, request.Payload.(ReqDeletePackfile).Checksum)
				err := repository.DeletePackfile(request.Payload.(ReqDeletePackfile).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResDeletePackfile",
					Payload: ResDeletePackfile{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		default:
			fmt.Println("Unknown request type", request.Type)
		}
	}
	wg.Wait()

	if repository != nil {
		repository.Close()
	}
}
