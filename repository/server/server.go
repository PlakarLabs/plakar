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

package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/repository"
	"github.com/poolpOrg/plakar/repository/compression"
)

type ServerStore struct {
	Namespace  string
	Repository string

	SkipDirs []string

	BackingStore repository.Store

	repository.Store
}

type ServerTransaction struct {
	Uuid     string
	store    *ServerStore
	prepared bool

	SkipDirs []string

	BackingTransaction repository.Transaction

	repository.Transaction
}

func (store *ServerStore) Init() {
	store.SkipDirs = append(store.SkipDirs, path.Clean(store.Repository))
}

func (store *ServerStore) Transaction() repository.Transaction {
	tx := &ServerTransaction{}
	tx.BackingTransaction = store.BackingStore.Transaction()
	tx.Uuid = tx.BackingTransaction.Snapshot().Uuid
	tx.store = store
	tx.prepared = false
	tx.SkipDirs = store.SkipDirs
	return tx
}

func (store *ServerStore) Snapshot(id string) (*repository.Snapshot, error) {
	index, err := store.IndexGet(id)
	if err != nil {
		return nil, err
	}

	index, _ = compression.Inflate(index)

	var snapshotStorage repository.SnapshotStorage

	if err = json.Unmarshal(index, &snapshotStorage); err != nil {
		return nil, err
	}

	snapshot := repository.Snapshot{}
	snapshot.Uuid = snapshotStorage.Uuid
	snapshot.CreationTime = snapshotStorage.CreationTime
	snapshot.Version = snapshotStorage.Version
	snapshot.Directories = snapshotStorage.Directories
	snapshot.Files = snapshotStorage.Files
	snapshot.NonRegular = snapshotStorage.NonRegular
	snapshot.Sums = snapshotStorage.Sums
	snapshot.Objects = snapshotStorage.Objects
	snapshot.Chunks = snapshotStorage.Chunks
	snapshot.Size = snapshotStorage.Size
	snapshot.RealSize = snapshotStorage.RealSize
	snapshot.BackingStore = store

	return &snapshot, nil
}

func (store *ServerStore) Snapshots() ([]string, error) {
	return store.BackingStore.Snapshots()
}

func (store *ServerStore) IndexGet(Uuid string) ([]byte, error) {
	return store.BackingStore.IndexGet(Uuid)
}

func (store *ServerStore) ObjectGet(checksum string) ([]byte, error) {
	return store.BackingStore.ObjectGet(checksum)
}

func (store *ServerStore) ChunkGet(checksum string) ([]byte, error) {
	return store.BackingStore.ChunkGet(checksum)
}

func (store *ServerStore) Purge(id string) error {
	return store.BackingStore.Purge(id)
}

func (transaction *ServerTransaction) Snapshot() *repository.Snapshot {
	return &repository.Snapshot{
		Uuid:         transaction.Uuid,
		CreationTime: time.Now(),
		Version:      "0.1.0",
		Directories:  make(map[string]*repository.FileInfo),
		Files:        make(map[string]*repository.FileInfo),
		NonRegular:   make(map[string]*repository.FileInfo),
		Sums:         make(map[string]string),
		Objects:      make(map[string]*repository.Object),
		Chunks:       make(map[string]*repository.Chunk),

		BackingTransaction: transaction,
		SkipDirs:           transaction.SkipDirs,
	}
}

func (transaction *ServerTransaction) ObjectMark(checksum string) bool {
	return transaction.BackingTransaction.ObjectMark(checksum)
}

func (transaction *ServerTransaction) ObjectPut(checksum string, buf string) error {
	return transaction.BackingTransaction.ObjectPut(checksum, buf)
}

func (transaction *ServerTransaction) ChunksMark(keys []string) map[string]bool {
	return transaction.BackingTransaction.ChunksMark(keys)
}

func (transaction *ServerTransaction) ChunkPut(checksum string, buf string) error {
	return transaction.BackingTransaction.ChunkPut(checksum, buf)
}

func (transaction *ServerTransaction) IndexPut(buf string) error {
	return transaction.BackingTransaction.IndexPut(buf)
}

func (transaction *ServerTransaction) Commit(snapshot *repository.Snapshot) (*repository.Snapshot, error) {
	return transaction.BackingTransaction.Commit(snapshot)
}

func Server(host string, store repository.Store) {
	lstore := &ServerStore{}
	lstore.BackingStore = store

	listener, err := net.Listen("tcp", host)
	if err != nil {
		log.Fatalln(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		// If you want, you can increment a counter here and inject to handleClientRequest below as client identifier
		go func() {
			defer conn.Close()

			clientReader := bufio.NewReader(conn)
			var currentTransaction repository.Transaction
			var currentSnapshot *repository.Snapshot

			for {
				// Waiting for the client request
				clientRequest, err := clientReader.ReadString('\n')

				switch err {
				case nil:
					clientRequest := strings.TrimSpace(clientRequest)
					if clientRequest == "Snapshots" {
						snapshots, err := lstore.Snapshots()
						ret := make([]string, 0)
						if err == nil {
							for _, snapshot := range snapshots {
								ret = append(ret, snapshot)
							}
						}
						print(snapshots)
						data, _ := json.Marshal(&struct {
							Snapshots []string
							Error     error
						}{ret, err})
						if _, err = conn.Write(data); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if strings.HasPrefix(clientRequest, "IndexGet:") {
						Uuid := clientRequest[9:]
						data, _ := lstore.IndexGet(Uuid)
						data, _ = json.Marshal(&struct{ Index []byte }{data})
						if _, err = conn.Write(data); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if strings.HasPrefix(clientRequest, "ObjectGet:") {
						checksum := clientRequest[10:]
						data, _ := lstore.ObjectGet(checksum)
						data, _ = json.Marshal(&struct{ Object []byte }{data})
						if _, err = conn.Write(data); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if strings.HasPrefix(clientRequest, "ChunkGet:") {
						checksum := clientRequest[9:]
						data, _ := lstore.ChunkGet(checksum)
						data, _ = json.Marshal(&struct{ Chunk []byte }{data})
						if _, err = conn.Write(data); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if strings.HasPrefix(clientRequest, "Purge:") {
						Uuid := clientRequest[6:]
						err := lstore.Purge(Uuid)
						data, _ := json.Marshal(&struct{ Error error }{err})
						if _, err = conn.Write(data); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if clientRequest == "Transaction" {
						currentTransaction = lstore.Transaction()
						currentSnapshot = currentTransaction.Snapshot()

						data, _ := json.Marshal(&struct{ Uuid uuid.UUID }{uuid.Must(uuid.Parse(currentSnapshot.Uuid))})
						if _, err = conn.Write(data); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if strings.HasPrefix(clientRequest, "ObjectMark:") {
						checksum := clientRequest[11:]
						res := currentTransaction.ObjectMark(checksum)
						currentSnapshot.Objects[checksum] = nil
						data, _ := json.Marshal(&struct{ Res bool }{res})
						if _, err = conn.Write(data); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if clientRequest == "ChunksMark" {
						data, err := clientReader.ReadString('\n')

						var checksums struct{ Checksums []string }
						err = json.Unmarshal([]byte(data), &checksums)
						if err != nil {
						}

						for _, checksum := range checksums.Checksums {
							currentSnapshot.Chunks[checksum] = nil
						}

						res := currentTransaction.ChunksMark(checksums.Checksums)
						data2, _ := json.Marshal(&struct{ Res map[string]bool }{res})

						if _, err = conn.Write(data2); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if strings.HasPrefix(clientRequest, "ObjectPut:") {
						checksum := clientRequest[10:]
						data, err := clientReader.ReadString('\n')

						var Object struct{ Data []byte }
						err = json.Unmarshal([]byte(data), &Object)
						if err != nil {
						}

						res := currentTransaction.ObjectPut(checksum, string(Object.Data))
						data2, _ := json.Marshal(&struct{ Error error }{res})
						if _, err = conn.Write(data2); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if strings.HasPrefix(clientRequest, "ChunkPut:") {
						checksum := clientRequest[9:]
						data, err := clientReader.ReadString('\n')

						var Chunk struct{ Data []byte }
						err = json.Unmarshal([]byte(data), &Chunk)
						if err != nil {
						}

						res := currentTransaction.ChunkPut(checksum, string(Chunk.Data))
						data2, _ := json.Marshal(&struct{ Error error }{res})
						if _, err = conn.Write(data2); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if clientRequest == "IndexPut" {
						data, err := clientReader.ReadString('\n')

						var Index struct{ Index []byte }
						err = json.Unmarshal([]byte(data), &Index)
						if err != nil {
						}

						res := currentTransaction.IndexPut(string(Index.Index))
						data2, _ := json.Marshal(&struct{ Error error }{res})
						if _, err = conn.Write(data2); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if clientRequest == "Commit" {
						fmt.Println(currentSnapshot.Uuid)
						_, err = currentTransaction.Commit(currentSnapshot)
						data, _ := json.Marshal(&struct{ Error error }{err})
						if _, err = conn.Write(data); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
						if _, err = conn.Write([]byte("\n")); err != nil {
							log.Printf("failed to respond to client: %v\n", err)
						}
					}

					if clientRequest == ":QUIT" {
						log.Println("client requested server to close the connection so closing")
						return
					} else {
						log.Println(clientRequest)
					}
				case io.EOF:
					log.Println("client closed the connection by terminating the process")
					return
				default:
					log.Printf("error: %v\n", err)
					return
				}

			}
		}()
	}
}
