package plakard

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
	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
)

type ServerOptions struct {
	NoOpen   bool
	NoCreate bool
	NoDelete bool
}

func Server(repository *storage.Repository, addr string, options *ServerOptions) {

	network.ProtocolRegister()

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
		go handleConnection(repository, c, c, options)
	}
}

func Stdio(options *ServerOptions) error {
	network.ProtocolRegister()

	handleConnection(nil, os.Stdin, os.Stdout, options)
	return nil
}

func handleConnection(repo *storage.Repository, rd io.Reader, wr io.Writer, options *ServerOptions) {
	var lrepository *storage.Repository

	lrepository = repo

	decoder := gob.NewDecoder(rd)
	encoder := gob.NewEncoder(wr)

	var wg sync.WaitGroup
	Uuid, _ := uuid.NewRandom()
	clientUuid := Uuid.String()

	homeDir := os.Getenv("HOME")

	for {
		request := network.Request{}
		err := decoder.Decode(&request)
		if err != nil {
			break
		}

		switch request.Type {
		case "ReqCreate":
			wg.Add(1)
			go func() {
				defer wg.Done()

				dirPath := request.Payload.(network.ReqCreate).Repository
				if dirPath == "" {
					dirPath = filepath.Join(homeDir, ".plakar")
				}

				logger.Trace("server", "%s: Create(%s, %s)", clientUuid, dirPath, request.Payload.(network.ReqCreate).RepositoryConfig)
				repo, err := storage.Create(dirPath, request.Payload.(network.ReqCreate).RepositoryConfig)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid:    request.Uuid,
					Type:    "ResCreate",
					Payload: network.ResCreate{Err: retErr},
				}
				lrepository = repo
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqOpen":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("server", "%s: Open()", clientUuid)

				location := request.Payload.(network.ReqOpen).Repository
				repo, err := storage.Open(location)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				var payload network.ResOpen
				if err != nil {
					payload = network.ResOpen{RepositoryConfig: nil, Err: retErr}
				} else {
					config := repo.Configuration()
					payload = network.ResOpen{RepositoryConfig: &config, Err: retErr}
				}
				lrepository = repo
				result := network.Request{
					Uuid:    request.Uuid,
					Type:    "ResOpen",
					Payload: payload,
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
				txUuid := request.Payload.(network.ReqCommit).IndexID
				data := request.Payload.(network.ReqCommit).Data
				err := lrepository.Commit(txUuid, data)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResCommit",
					Payload: network.ResCommit{
						Err: retErr,
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

				logger.Trace("server", "%s: Close()", clientUuid)

				var err error
				if repo == nil {
					err = lrepository.Close()
				}
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}

				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResClose",
					Payload: network.ResClose{
						Err: retErr,
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
				snapshots, err := lrepository.GetSnapshots()
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetSnapshots",
					Payload: network.ResGetSnapshots{
						Snapshots: snapshots,
						Err:       retErr,
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
				logger.Trace("server", "%s: PutSnapshot()", clientUuid, request.Payload.(network.ReqPutSnapshot).IndexID)
				err := lrepository.PutSnapshot(request.Payload.(network.ReqPutSnapshot).IndexID, request.Payload.(network.ReqPutSnapshot).Data)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutSnapshot",
					Payload: network.ResPutSnapshot{
						Err: retErr,
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
				logger.Trace("server", "%s: GetMetadata(%s)", clientUuid, request.Payload.(network.ReqGetSnapshot).IndexID)
				data, err := lrepository.GetSnapshot(request.Payload.(network.ReqGetSnapshot).IndexID)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetSnapshot",
					Payload: network.ResGetSnapshot{
						Data: data,
						Err:  retErr,
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

				logger.Trace("server", "%s: DeleteSnapshot(%s)", clientUuid, request.Payload.(network.ReqDeleteSnapshot).IndexID)
				var err error
				if options.NoDelete {
					err = fmt.Errorf("not allowed to delete")
				} else {
					err = lrepository.DeleteSnapshot(request.Payload.(network.ReqDeleteSnapshot).IndexID)
				}
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeleteSnapshot",
					Payload: network.ResDeleteSnapshot{
						Err: retErr,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

			// locks
		case "ReqGetLocks":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetLocks", clientUuid)
				locks, err := lrepository.GetLocks()
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetLocks",
					Payload: network.ResGetLocks{
						Locks: locks,
						Err:   retErr,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqPutLock":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: PutLock()", clientUuid, request.Payload.(network.ReqPutLock).IndexID)
				err := lrepository.PutLock(request.Payload.(network.ReqPutLock).IndexID, request.Payload.(network.ReqPutLock).Data)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutLock",
					Payload: network.ResPutLock{
						Err: retErr,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqGetLock":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetMetadata(%s)", clientUuid, request.Payload.(network.ReqGetLock).IndexID)
				data, err := lrepository.GetLock(request.Payload.(network.ReqGetLock).IndexID)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetLock",
					Payload: network.ResGetLock{
						Data: data,
						Err:  retErr,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqDeleteLock":
			wg.Add(1)
			go func() {
				defer wg.Done()

				logger.Trace("server", "%s: DeleteLock(%s)", clientUuid, request.Payload.(network.ReqDeleteLock).IndexID)
				err := lrepository.DeleteLock(request.Payload.(network.ReqDeleteLock).IndexID)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeleteLock",
					Payload: network.ResDeleteLock{
						Err: retErr,
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
				checksums, err := lrepository.GetBlobs()
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetBlobs",
					Payload: network.ResGetBlobs{
						Checksums: checksums,
						Err:       retErr,
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
				logger.Trace("server", "%s: PutBlob(%016x)", clientUuid, request.Payload.(network.ReqPutBlob).Checksum)
				err := lrepository.PutBlob(request.Payload.(network.ReqPutBlob).Checksum, request.Payload.(network.ReqPutBlob).Data)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutBlob",
					Payload: network.ResPutBlob{
						Err: retErr,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqCheckBlob":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: CheckBlob(%016x)", clientUuid, request.Payload.(network.ReqCheckBlob).Checksum)
				exists, err := lrepository.CheckBlob(request.Payload.(network.ReqCheckBlob).Checksum)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResCheckBlob",
					Payload: network.ResCheckBlob{
						Exists: exists,
						Err:    retErr,
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
				logger.Trace("server", "%s: GetBlob(%016x)", clientUuid, request.Payload.(network.ReqGetBlob).Checksum)
				data, err := lrepository.GetBlob(request.Payload.(network.ReqGetBlob).Checksum)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetBlob",
					Payload: network.ResGetBlob{
						Data: data,
						Err:  retErr,
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

				logger.Trace("server", "%s: DeleteBlob(%s)", clientUuid, request.Payload.(network.ReqDeleteBlob).Checksum)

				var err error
				if options.NoDelete {
					err = fmt.Errorf("not allowed to delete")
				} else {
					err = lrepository.DeleteBlob(request.Payload.(network.ReqDeleteBlob).Checksum)
				}
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeleteBlob",
					Payload: network.ResDeleteBlob{
						Err: retErr,
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
				checksums, err := lrepository.GetIndexes()
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetIndexes",
					Payload: network.ResGetIndexes{
						Checksums: checksums,
						Err:       retErr,
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
				logger.Trace("server", "%s: PutIndex(%016x)", clientUuid, request.Payload.(network.ReqPutIndex).Checksum)
				err := lrepository.PutIndex(request.Payload.(network.ReqPutIndex).Checksum, request.Payload.(network.ReqPutIndex).Data)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutIndex",
					Payload: network.ResPutIndex{
						Err: retErr,
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
				logger.Trace("server", "%s: GetIndex(%016x)", clientUuid, request.Payload.(network.ReqGetIndex).Checksum)
				data, err := lrepository.GetIndex(request.Payload.(network.ReqGetIndex).Checksum)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetIndex",
					Payload: network.ResGetIndex{
						Data: data,
						Err:  retErr,
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

				logger.Trace("server", "%s: DeleteIndex(%s)", clientUuid, request.Payload.(network.ReqDeleteIndex).Checksum)

				var err error
				if options.NoDelete {
					err = fmt.Errorf("not allowed to delete")
				} else {
					err = lrepository.DeleteIndex(request.Payload.(network.ReqDeleteIndex).Checksum)
				}
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeleteIndex",
					Payload: network.ResDeleteIndex{
						Err: retErr,
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
				checksums, err := lrepository.GetPackfiles()
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetPackfiles",
					Payload: network.ResGetPackfiles{
						Checksums: checksums,
						Err:       retErr,
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
				logger.Trace("server", "%s: PutPackfile(%016x)", clientUuid, request.Payload.(network.ReqPutPackfile).Checksum)
				err := lrepository.PutPackfile(request.Payload.(network.ReqPutPackfile).Checksum, request.Payload.(network.ReqPutPackfile).Data)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutPackfile",
					Payload: network.ResPutPackfile{
						Err: retErr,
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
				logger.Trace("server", "%s: GetPackfile(%016x)", clientUuid, request.Payload.(network.ReqGetPackfile).Checksum)
				data, err := lrepository.GetPackfile(request.Payload.(network.ReqGetPackfile).Checksum)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetPackfile",
					Payload: network.ResGetPackfile{
						Data: data,
						Err:  retErr,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
				}
			}()

		case "ReqGetPackfileSubpart":
			wg.Add(1)
			go func() {
				defer wg.Done()
				logger.Trace("server", "%s: GetPackfileSubpart(%016x, %d, %d)", clientUuid,
					request.Payload.(network.ReqGetPackfileSubpart).Checksum,
					request.Payload.(network.ReqGetPackfileSubpart).Offset,
					request.Payload.(network.ReqGetPackfileSubpart).Length)
				data, err := lrepository.GetPackfileSubpart(request.Payload.(network.ReqGetPackfileSubpart).Checksum,
					request.Payload.(network.ReqGetPackfileSubpart).Offset,
					request.Payload.(network.ReqGetPackfileSubpart).Length)
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetPackfileSubpart",
					Payload: network.ResGetPackfileSubpart{
						Data: data,
						Err:  retErr,
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

				logger.Trace("server", "%s: DeletePackfile(%s)", clientUuid, request.Payload.(network.ReqDeletePackfile).Checksum)

				var err error
				if options.NoDelete {
					err = fmt.Errorf("not allowed to delete")
				} else {
					err = lrepository.DeletePackfile(request.Payload.(network.ReqDeletePackfile).Checksum)
				}
				retErr := ""
				if err != nil {
					retErr = err.Error()
				}
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeletePackfile",
					Payload: network.ResDeletePackfile{
						Err: retErr,
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
}
