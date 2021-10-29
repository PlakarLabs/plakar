package network

import (
	"encoding/gob"
	"log"
	"net"
	"sync"

	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"
)

func Server(store storage.Store, addr string) {

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

func handleConnection(store storage.Store, conn net.Conn) {

	transactions := make(map[string]storage.Transaction)
	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)

	var wg sync.WaitGroup

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

			case "ReqPurge":
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
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
