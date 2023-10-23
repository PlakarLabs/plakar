package httpd

import (
	"net/http"

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/gorilla/mux"
)

func Server(repository *storage.Repository, addr string) error {

	network.ProtocolRegister()

	r := mux.NewRouter()
	//	r.HandleFunc("/", viewRepository)
	//	r.HandleFunc("/snapshot/{snapshot}:/", browse)
	//	r.HandleFunc("/snapshot/{snapshot}:{path:.+}/", browse)
	//	r.HandleFunc("/raw/{snapshot}:{path:.+}", raw)
	//	r.HandleFunc("/snapshot/{snapshot}:{path:.+}", object)
	//
	//	r.HandleFunc("/search", search_snapshots)

	return http.ListenAndServe(addr, r)
}

/*
func handleConnection(rd io.Reader, wr io.Writer) {
	decoder := gob.NewDecoder(rd)
	encoder := gob.NewEncoder(wr)

	var repository *storage.Repository
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
				repository, err = storage.Create(dirPath, request.Payload.(network.ReqCreate).RepositoryConfig)
				result := network.Request{
					Uuid:    request.Uuid,
					Type:    "ResCreate",
					Payload: network.ResCreate{Err: err},
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
				dirPath := request.Payload.(network.ReqOpen).Repository
				if dirPath == "" {
					dirPath = filepath.Join(homeDir, ".plakar")
				}

				logger.Trace("server", "%s: Open(%s)", clientUuid, dirPath)
				repository, err = storage.Open(dirPath)
				var payload network.ResOpen
				if err != nil {
					payload = network.ResOpen{RepositoryConfig: nil, Err: err}
				} else {
					config := repository.Configuration()
					payload = network.ResOpen{RepositoryConfig: &config, Err: nil}
				}
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
				txUuid := request.Payload.(network.ReqCommit).Transaction
				data := request.Payload.(network.ReqCommit).Data
				err := repository.Commit(txUuid, data)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResCommit",
					Payload: network.ResCommit{
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

				logger.Trace("server", "%s: Close()", clientUuid)
				err := repository.Close()
				repository = nil
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResClose",
					Payload: network.ResClose{
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
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetSnapshots",
					Payload: network.ResGetSnapshots{
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
				logger.Trace("server", "%s: PutSnapshot()", clientUuid, request.Payload.(network.ReqPutSnapshot).IndexID)
				err := repository.PutSnapshot(request.Payload.(network.ReqPutSnapshot).IndexID, request.Payload.(network.ReqPutSnapshot).Data)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutSnapshot",
					Payload: network.ResPutSnapshot{
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
				logger.Trace("server", "%s: GetMetadata(%s)", clientUuid, request.Payload.(network.ReqGetSnapshot).Uuid)
				data, err := repository.GetSnapshot(request.Payload.(network.ReqGetSnapshot).Uuid)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetSnapshot",
					Payload: network.ResGetSnapshot{
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

				logger.Trace("server", "%s: DeleteSnapshot(%s)", clientUuid, request.Payload.(network.ReqDeleteSnapshot).Uuid)
				err := repository.DeleteSnapshot(request.Payload.(network.ReqDeleteSnapshot).Uuid)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeleteSnapshot",
					Payload: network.ResDeleteSnapshot{
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
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetBlobs",
					Payload: network.ResGetBlobs{
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
				logger.Trace("server", "%s: PutBlob(%016x)", clientUuid, request.Payload.(network.ReqPutBlob).Checksum)
				err := repository.PutBlob(request.Payload.(network.ReqPutBlob).Checksum, request.Payload.(network.ReqPutBlob).Data)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutBlob",
					Payload: network.ResPutBlob{
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
				logger.Trace("server", "%s: GetBlob(%016x)", clientUuid, request.Payload.(network.ReqGetBlob).Checksum)
				data, err := repository.GetBlob(request.Payload.(network.ReqGetBlob).Checksum)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetBlob",
					Payload: network.ResGetBlob{
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

				logger.Trace("server", "%s: DeleteBlob(%s)", clientUuid, request.Payload.(network.ReqDeleteBlob).Checksum)
				err := repository.DeleteBlob(request.Payload.(network.ReqDeleteBlob).Checksum)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeleteBlob",
					Payload: network.ResDeleteBlob{
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
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetIndexes",
					Payload: network.ResGetIndexes{
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
				logger.Trace("server", "%s: PutIndex(%016x)", clientUuid, request.Payload.(network.ReqPutIndex).Checksum)
				err := repository.PutIndex(request.Payload.(network.ReqPutIndex).Checksum, request.Payload.(network.ReqPutIndex).Data)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutIndex",
					Payload: network.ResPutIndex{
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
				logger.Trace("server", "%s: GetIndex(%016x)", clientUuid, request.Payload.(network.ReqGetIndex).Checksum)
				data, err := repository.GetIndex(request.Payload.(network.ReqGetIndex).Checksum)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetIndex",
					Payload: network.ResGetIndex{
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

				logger.Trace("server", "%s: DeleteIndex(%s)", clientUuid, request.Payload.(network.ReqDeleteIndex).Checksum)
				err := repository.DeleteIndex(request.Payload.(network.ReqDeleteIndex).Checksum)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeleteIndex",
					Payload: network.ResDeleteIndex{
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
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetPackfiles",
					Payload: network.ResGetPackfiles{
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
				logger.Trace("server", "%s: PutPackfile(%016x)", clientUuid, request.Payload.(network.ReqPutPackfile).Checksum)
				err := repository.PutPackfile(request.Payload.(network.ReqPutPackfile).Checksum, request.Payload.(network.ReqPutPackfile).Data)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResPutPackfile",
					Payload: network.ResPutPackfile{
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
				logger.Trace("server", "%s: GetPackfile(%016x)", clientUuid, request.Payload.(network.ReqGetPackfile).Checksum)
				data, err := repository.GetPackfile(request.Payload.(network.ReqGetPackfile).Checksum)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResGetPackfile",
					Payload: network.ResGetPackfile{
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

				logger.Trace("server", "%s: DeletePackfile(%s)", clientUuid, request.Payload.(network.ReqDeletePackfile).Checksum)
				err := repository.DeletePackfile(request.Payload.(network.ReqDeletePackfile).Checksum)
				result := network.Request{
					Uuid: request.Uuid,
					Type: "ResDeletePackfile",
					Payload: network.ResDeletePackfile{
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
*/
