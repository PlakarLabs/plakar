package network

import (
	"encoding/gob"
	"log"
	"net"
	"sync"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"
)

func Server(store *storage.Store, addr string) {

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
		go handleConnection(store, c)
	}
}

func handleConnection(store *storage.Store, conn net.Conn) {
	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)

	transactions := make(map[string]*storage.Transaction)

	var wg sync.WaitGroup
	Uuid, _ := uuid.NewRandom()
	clientUuid := Uuid.String()

	for {
		request := Request{}
		err := decoder.Decode(&request)
		if err != nil {
			break
		}

		wg.Add(1)
		go func() {
			switch request.Type {
			case "ReqOpen":
				logger.Trace("%s: Open", clientUuid)
				result := Request{
					Uuid:    request.Uuid,
					Type:    "ResOpen",
					Payload: ResOpen{StoreConfig: store.Configuration()},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqGetIndexes":
				logger.Trace("%s: GetIndexes", clientUuid)
				indexes, err := store.GetIndexes()
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetIndexes",
					Payload: ResGetIndexes{
						Indexes: indexes,
						Err:     err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqGetIndex":
				logger.Trace("%s: GetIndex(%s)", clientUuid, request.Payload.(ReqGetIndex).Uuid)
				data, err := store.GetIndex(request.Payload.(ReqGetIndex).Uuid)
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
					break
				}

			case "ReqGetObject":
				logger.Trace("%s: GetObject(%s)", clientUuid, request.Payload.(ReqGetObject).Checksum)
				data, err := store.GetObject(request.Payload.(ReqGetObject).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetObject",
					Payload: ResGetObject{
						Data: data,
						Err:  err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqGetChunk":
				logger.Trace("%s: GetChunk(%s)", clientUuid, request.Payload.(ReqGetChunk).Checksum)
				data, err := store.GetChunk(request.Payload.(ReqGetChunk).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResGetChunk",
					Payload: ResGetChunk{
						Data: data,
						Err:  err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqCheckObject":
				logger.Trace("%s: CheckObject(%s)", clientUuid, request.Payload.(ReqCheckObject).Checksum)
				exists, err := store.CheckObject(request.Payload.(ReqCheckObject).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResCheckObject",
					Payload: ResCheckObject{
						Exists: exists,
						Err:    err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqCheckChunk":
				logger.Trace("%s: CheckChunk(%s)", clientUuid, request.Payload.(ReqCheckChunk).Checksum)
				exists, err := store.CheckChunk(request.Payload.(ReqCheckChunk).Checksum)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResCheckChunk",
					Payload: ResCheckChunk{
						Exists: exists,
						Err:    err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqPurge":
				logger.Trace("%s: Purge(%s)", clientUuid, request.Payload.(ReqPurge).Uuid)
				err := store.Purge(request.Payload.(ReqPurge).Uuid)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResPurge",
					Payload: ResPurge{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqTransaction":
				logger.Trace("%s: Transaction", clientUuid)
				tx, err := store.Transaction()
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
					break
				}
				transactions[tx.GetUuid()] = tx

			case "ReqReferenceChunks":
				logger.Trace("%s: ReferenceChunks()", clientUuid)
				txUuid := request.Payload.(ReqReferenceChunks).Transaction
				tx := transactions[txUuid]
				exists, err := tx.ReferenceChunks(request.Payload.(ReqReferenceChunks).Keys)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResReferenceChunks",
					Payload: ResReferenceChunks{
						Exists: exists,
						Err:    err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqReferenceObjects":
				logger.Trace("%s: ReferenceObjects()", clientUuid)
				txUuid := request.Payload.(ReqReferenceObjects).Transaction
				tx := transactions[txUuid]
				exists, err := tx.ReferenceObjects(request.Payload.(ReqReferenceObjects).Keys)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResReferenceObjects",
					Payload: ResReferenceObjects{
						Exists: exists,
						Err:    err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqPutChunk":
				logger.Trace("%s: PutChunk(%s)", clientUuid, request.Payload.(ReqPutChunk).Checksum)
				txUuid := request.Payload.(ReqPutChunk).Transaction
				tx := transactions[txUuid]
				err := tx.PutChunk(request.Payload.(ReqPutChunk).Checksum, request.Payload.(ReqPutChunk).Data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResPutChunk",
					Payload: ResPutChunk{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqPutObject":
				logger.Trace("%s: PutObject(%s)", clientUuid, request.Payload.(ReqPutObject).Checksum)
				txUuid := request.Payload.(ReqPutObject).Transaction
				tx := transactions[txUuid]
				err := tx.PutObject(request.Payload.(ReqPutObject).Checksum, request.Payload.(ReqPutObject).Data)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResPutObject",
					Payload: ResPutObject{
						Err: err,
					},
				}
				err = encoder.Encode(&result)
				if err != nil {
					logger.Warn("%s", err)
					break
				}

			case "ReqPutIndex":
				logger.Trace("%s: PutIndex()", clientUuid)
				txUuid := request.Payload.(ReqPutIndex).Transaction
				tx := transactions[txUuid]
				err := tx.PutIndex(request.Payload.(ReqPutIndex).Data)
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
					break
				}

			case "ReqCommit":
				logger.Trace("%s: Commit()", clientUuid)
				txUuid := request.Payload.(ReqCommit).Transaction
				tx := transactions[txUuid]
				err := tx.Commit()
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
					break
				}

			case "ReqClose":
				logger.Trace("%s: Close()", clientUuid)
				_ = request.Payload.(ReqClose).Uuid
				err := store.Close()
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
					break
				}

			}
			wg.Done()
		}()
	}
	wg.Wait()
}
