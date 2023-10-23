package httpd

import (
	"encoding/json"
	"net/http"

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/gorilla/mux"
)

var lrepository *storage.Repository

func openRepository(w http.ResponseWriter, r *http.Request) {
	var reqOpen network.ReqOpen
	if err := json.NewDecoder(r.Body).Decode(&reqOpen); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	config := lrepository.Configuration()

	var resOpen network.ResOpen
	resOpen.RepositoryConfig = &config
	resOpen.Err = nil
	if err := json.NewEncoder(w).Encode(resOpen); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func closeRepository(w http.ResponseWriter, r *http.Request) {
	var reqClose network.ReqClose
	if err := json.NewDecoder(r.Body).Decode(&reqClose); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if reqClose.Uuid != lrepository.Configuration().RepositoryID.String() {
		http.Error(w, "UUID mismatch", http.StatusBadRequest)
		return
	}

	var resClose network.ResClose
	resClose.Err = nil
	if err := json.NewEncoder(w).Encode(resClose); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// snapshots
func getSnapshots(w http.ResponseWriter, r *http.Request) {
	var reqGetSnapshots network.ReqGetSnapshots
	if err := json.NewDecoder(r.Body).Decode(&reqGetSnapshots); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetSnapshots network.ResGetSnapshots
	snapshots, err := lrepository.GetSnapshots()
	if err != nil {
		resGetSnapshots.Err = err
	} else {
		resGetSnapshots.Snapshots = snapshots
	}
	if err := json.NewEncoder(w).Encode(resGetSnapshots); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putSnapshot(w http.ResponseWriter, r *http.Request) {
	var reqPutSnapshot network.ReqPutSnapshot
	if err := json.NewDecoder(r.Body).Decode(&reqPutSnapshot); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resPutSnapshot network.ResPutSnapshot
	resPutSnapshot.Err = lrepository.PutSnapshot(reqPutSnapshot.IndexID, reqPutSnapshot.Data)
	if err := json.NewEncoder(w).Encode(resPutSnapshot); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getSnapshot(w http.ResponseWriter, r *http.Request) {
	var reqGetSnapshot network.ReqGetSnapshot
	if err := json.NewDecoder(r.Body).Decode(&reqGetSnapshot); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetSnapshot network.ResGetSnapshot
	data, err := lrepository.GetSnapshot(reqGetSnapshot.IndexID)
	if err != nil {
		resGetSnapshot.Err = err
	} else {
		resGetSnapshot.Data = data
	}
	if err := json.NewEncoder(w).Encode(resGetSnapshot); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deleteSnapshot(w http.ResponseWriter, r *http.Request) {
	var reqDeleteSnapshot network.ReqDeleteSnapshot
	if err := json.NewDecoder(r.Body).Decode(&reqDeleteSnapshot); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resDeleteSnapshot network.ResDeleteSnapshot
	resDeleteSnapshot.Err = lrepository.DeleteSnapshot(reqDeleteSnapshot.IndexID)
	if err := json.NewEncoder(w).Encode(resDeleteSnapshot); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func commitSnapshot(w http.ResponseWriter, r *http.Request) {
	var ReqCommit network.ReqCommit
	if err := json.NewDecoder(r.Body).Decode(&ReqCommit); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var ResCommit network.ResCommit
	ResCommit.Err = lrepository.Commit(ReqCommit.IndexID, ReqCommit.Data)
	if err := json.NewEncoder(w).Encode(ResCommit); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// snapshots
func getBlobs(w http.ResponseWriter, r *http.Request) {
	var reqGetBlobs network.ReqGetBlobs
	if err := json.NewDecoder(r.Body).Decode(&reqGetBlobs); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetBlobs network.ResGetBlobs
	checksums, err := lrepository.GetBlobs()
	if err != nil {
		resGetBlobs.Err = err
	} else {
		resGetBlobs.Checksums = checksums
	}
	if err := json.NewEncoder(w).Encode(resGetBlobs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putBlob(w http.ResponseWriter, r *http.Request) {
	var reqPutBlob network.ReqPutBlob
	if err := json.NewDecoder(r.Body).Decode(&reqPutBlob); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resPutBlob network.ResPutBlob
	resPutBlob.Err = lrepository.PutBlob(reqPutBlob.Checksum, reqPutBlob.Data)
	if err := json.NewEncoder(w).Encode(resPutBlob); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getBlob(w http.ResponseWriter, r *http.Request) {
	var reqGetBlob network.ReqGetBlob
	if err := json.NewDecoder(r.Body).Decode(&reqGetBlob); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetBlob network.ResGetBlob
	data, err := lrepository.GetBlob(reqGetBlob.Checksum)
	if err != nil {
		resGetBlob.Err = err
	} else {
		resGetBlob.Data = data
	}
	if err := json.NewEncoder(w).Encode(resGetBlob); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deleteBlob(w http.ResponseWriter, r *http.Request) {
	var reqDeleteBlob network.ReqDeleteBlob
	if err := json.NewDecoder(r.Body).Decode(&reqDeleteBlob); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resDeleteBlob network.ResDeleteBlob
	resDeleteBlob.Err = lrepository.DeleteBlob(reqDeleteBlob.Checksum)
	if err := json.NewEncoder(w).Encode(resDeleteBlob); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// indexes
func getIndexes(w http.ResponseWriter, r *http.Request) {
	var reqGetIndexes network.ReqGetIndexes
	if err := json.NewDecoder(r.Body).Decode(&reqGetIndexes); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetIndexes network.ResGetIndexes
	indexes, err := lrepository.GetIndexes()
	if err != nil {
		resGetIndexes.Err = err
	} else {
		resGetIndexes.Checksums = indexes
	}
	if err := json.NewEncoder(w).Encode(resGetIndexes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putIndex(w http.ResponseWriter, r *http.Request) {
	var reqPutIndex network.ReqPutIndex
	if err := json.NewDecoder(r.Body).Decode(&reqPutIndex); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resPutIndex network.ResPutIndex
	resPutIndex.Err = lrepository.PutIndex(reqPutIndex.Checksum, reqPutIndex.Data)
	if err := json.NewEncoder(w).Encode(resPutIndex); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getIndex(w http.ResponseWriter, r *http.Request) {
	var reqGetIndex network.ReqGetIndex
	if err := json.NewDecoder(r.Body).Decode(&reqGetIndex); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetIndex network.ResGetIndex
	data, err := lrepository.GetIndex(reqGetIndex.Checksum)
	if err != nil {
		resGetIndex.Err = err
	} else {
		resGetIndex.Data = data
	}
	if err := json.NewEncoder(w).Encode(resGetIndex); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deleteIndex(w http.ResponseWriter, r *http.Request) {
	var reqDeleteIndex network.ReqDeleteIndex
	if err := json.NewDecoder(r.Body).Decode(&reqDeleteIndex); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resDeleteIndex network.ResDeleteIndex
	resDeleteIndex.Err = lrepository.DeleteIndex(reqDeleteIndex.Checksum)
	if err := json.NewEncoder(w).Encode(resDeleteIndex); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// packfiles
func getPackfiles(w http.ResponseWriter, r *http.Request) {
	var reqGetPackfiles network.ReqGetPackfiles
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfiles); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetPackfiles network.ResGetPackfiles
	packfiles, err := lrepository.GetPackfiles()
	if err != nil {
		resGetPackfiles.Err = err
	} else {
		resGetPackfiles.Checksums = packfiles
	}
	if err := json.NewEncoder(w).Encode(resGetPackfiles); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putPackfile(w http.ResponseWriter, r *http.Request) {
	var reqPutPackfile network.ReqPutPackfile
	if err := json.NewDecoder(r.Body).Decode(&reqPutPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resPutPackfile network.ResPutPackfile
	resPutPackfile.Err = lrepository.PutPackfile(reqPutPackfile.Checksum, reqPutPackfile.Data)
	if err := json.NewEncoder(w).Encode(resPutPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getPackfile(w http.ResponseWriter, r *http.Request) {
	var reqGetPackfile network.ReqGetPackfile
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetPackfile network.ResGetPackfile
	data, err := lrepository.GetPackfile(reqGetPackfile.Checksum)
	if err != nil {
		resGetPackfile.Err = err
	} else {
		resGetPackfile.Data = data
	}
	if err := json.NewEncoder(w).Encode(resGetPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deletePackfile(w http.ResponseWriter, r *http.Request) {
	var reqDeletePackfile network.ReqDeletePackfile
	if err := json.NewDecoder(r.Body).Decode(&reqDeletePackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resDeletePackfile network.ResDeletePackfile
	resDeletePackfile.Err = lrepository.DeletePackfile(reqDeletePackfile.Checksum)
	if err := json.NewEncoder(w).Encode(resDeletePackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func Server(repository *storage.Repository, addr string) error {

	lrepository = repository

	network.ProtocolRegister()

	r := mux.NewRouter()
	r.HandleFunc("/", openRepository).Methods("GET")
	r.HandleFunc("/", closeRepository).Methods("POST")

	r.HandleFunc("/snapshots", getSnapshots).Methods("GET")
	r.HandleFunc("/snapshot", putSnapshot).Methods("PUT")
	r.HandleFunc("/snapshot", getSnapshot).Methods("GET")
	r.HandleFunc("/snapshot", deleteSnapshot).Methods("DELETE")
	r.HandleFunc("/snapshot", commitSnapshot).Methods("POST")

	r.HandleFunc("/blobs", getBlobs).Methods("GET")
	r.HandleFunc("/blob", putBlob).Methods("PUT")
	r.HandleFunc("/blob", getBlob).Methods("GET")
	r.HandleFunc("/blob", deleteBlob).Methods("DELETE")

	r.HandleFunc("/indexes", getIndexes).Methods("GET")
	r.HandleFunc("/index", putIndex).Methods("PUT")
	r.HandleFunc("/index", getIndex).Methods("GET")

	r.HandleFunc("/packfiles", getPackfiles).Methods("GET")
	r.HandleFunc("/packfile", putPackfile).Methods("PUT")
	r.HandleFunc("/packfile", getPackfile).Methods("GET")

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
