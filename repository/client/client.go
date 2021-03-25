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

package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"path"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/repository"
	"github.com/poolpOrg/plakar/repository/compression"
)

type ClientStore struct {
	Namespace  string
	Repository string

	SkipDirs []string

	conn         net.Conn
	serverReader *bufio.Reader

	repository.Store
}

type ClientTransaction struct {
	Uuid     string
	store    *ClientStore
	prepared bool

	SkipDirs []string

	repository.Transaction
}

func (store *ClientStore) Init() {
	store.SkipDirs = append(store.SkipDirs, path.Clean(store.Repository))

	conn, err := net.Dial("tcp", store.Repository[9:])
	if err != nil {
		log.Fatalln(err)
	}

	store.conn = conn
	store.serverReader = bufio.NewReader(conn)

	//conn.Write([]byte("test\n"))
	//fmt.Println(serverReader.ReadBytes('\n'))

	/*
			for {
			// Waiting for the client request
			clientRequest, err := clientReader.ReadString('\n')

			switch err {
			case nil:
				clientRequest := strings.TrimSpace(clientRequest)
				if _, err = con.Write([]byte(clientRequest + "\n")); err != nil {
					log.Printf("failed to send the client request: %v\n", err)
				}
			case io.EOF:
				log.Println("client closed the connection")
				return
			default:
				log.Printf("client error: %v\n", err)
				return
			}

			// Waiting for the server response
			serverResponse, err := serverReader.ReadString('\n')

			switch err {
			case nil:
				log.Println(strings.TrimSpace(serverResponse))
			case io.EOF:
				log.Println("server closed the connection")
				return
			default:
				log.Printf("server error: %v\n", err)
				return
			}
		}
	*/

}

func (store *ClientStore) Transaction() repository.Transaction {

	store.conn.Write([]byte("Transaction\n"))
	data, _ := store.serverReader.ReadBytes('\n')

	var Uuid struct{ Uuid uuid.UUID }
	err := json.Unmarshal(data, &Uuid)
	if err != nil {
		return nil
	}

	tx := &ClientTransaction{}
	tx.Uuid = Uuid.Uuid.String()
	tx.store = store
	tx.prepared = false
	tx.SkipDirs = store.SkipDirs
	return tx
}

func (store *ClientStore) Snapshot(id string) (*repository.Snapshot, error) {
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

func (store *ClientStore) ObjectExists(checksum string) bool {
	return false
}

func (store *ClientStore) ChunkExists(checksum string) bool {
	return false
}

func (store *ClientStore) Snapshots() []string {
	ret := make([]string, 0)

	store.conn.Write([]byte("Snapshots\n"))
	data, _ := store.serverReader.ReadBytes('\n')

	var snapshots struct{ Snapshots []string }
	err := json.Unmarshal(data, &snapshots)
	if err != nil {
		return ret
	}

	return snapshots.Snapshots
}

func (store *ClientStore) IndexGet(Uuid string) ([]byte, error) {
	store.conn.Write([]byte(fmt.Sprintf("IndexGet:%s\n", Uuid)))
	data, _ := store.serverReader.ReadBytes('\n')

	var index struct{ Index []byte }
	err := json.Unmarshal(data, &index)
	if err != nil {
		return nil, err
	}

	return index.Index, nil
}

func (store *ClientStore) ObjectGet(checksum string) ([]byte, error) {
	store.conn.Write([]byte(fmt.Sprintf("ObjectGet:%s\n", checksum)))
	data, _ := store.serverReader.ReadBytes('\n')

	var object struct{ Object []byte }
	err := json.Unmarshal(data, &object)
	if err != nil {
		return nil, err
	}

	return object.Object, nil
}

func (store *ClientStore) ChunkGet(checksum string) ([]byte, error) {
	store.conn.Write([]byte(fmt.Sprintf("ChunkGet:%s\n", checksum)))
	data, _ := store.serverReader.ReadBytes('\n')

	var chunk struct{ Chunk []byte }
	err := json.Unmarshal(data, &chunk)
	if err != nil {
		return nil, err
	}

	return chunk.Chunk, nil
}

func (store *ClientStore) Purge(id string) error {
	store.conn.Write([]byte(fmt.Sprintf("Purge:%s\n", id)))
	data, _ := store.serverReader.ReadBytes('\n')

	var result struct{ Error error }
	err := json.Unmarshal(data, &result)
	if err != nil {
		return err
	}
	return result.Error
}

func (transaction *ClientTransaction) Snapshot() *repository.Snapshot {
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

func (transaction *ClientTransaction) ObjectsCheck(keys []string) map[string]bool {
	fmt.Println("ObjectsCheck")
	ret := make(map[string]bool)

	for _, key := range keys {
		ret[key] = transaction.store.ObjectExists(key)
	}

	return ret
}

func (transaction *ClientTransaction) ChunksMark(keys []string) map[string]bool {
	fmt.Println("ChunksMark")
	store := transaction.store

	data, _ := json.Marshal(&struct{ Checksums []string }{keys})

	store.conn.Write([]byte(fmt.Sprintf("ChunksMark\n")))
	store.conn.Write(data)
	store.conn.Write([]byte("\n"))

	data, _ = store.serverReader.ReadBytes('\n')
	var res struct{ Res map[string]bool }
	err := json.Unmarshal(data, &res)
	if err != nil {
		return nil
	}
	return res.Res
}

func (transaction *ClientTransaction) ChunksCheck(keys []string) map[string]bool {
	fmt.Println("ChunksCheck")
	ret := make(map[string]bool)

	for _, key := range keys {
		ret[key] = transaction.store.ChunkExists(key)
	}

	return ret
}

func (transaction *ClientTransaction) ObjectMark(checksum string) bool {
	fmt.Println("ObjectMark")
	store := transaction.store

	store.conn.Write([]byte(fmt.Sprintf("ObjectMark:%s\n", checksum)))
	data, _ := store.serverReader.ReadBytes('\n')

	var res struct{ Res bool }
	err := json.Unmarshal(data, &res)
	if err != nil {
		return false
	}

	return res.Res
}

func (transaction *ClientTransaction) ObjectRecord(checksum string, buf string) (bool, error) {
	fmt.Println("ObjectRecord")
	return false, nil
}

func (transaction *ClientTransaction) ObjectPut(checksum string, buf string) error {
	fmt.Println("ObjectPut")
	store := transaction.store

	data, _ := json.Marshal(&struct{ Data []byte }{[]byte(buf)})

	store.conn.Write([]byte(fmt.Sprintf("ObjectPut:%s\n", checksum)))
	store.conn.Write(data)
	store.conn.Write([]byte("\n"))

	data, _ = store.serverReader.ReadBytes('\n')
	var result struct{ Error error }
	err := json.Unmarshal(data, &result)
	if err != nil {
		return err
	}
	return result.Error
}

func (transaction *ClientTransaction) ChunkPut(checksum string, buf string) error {
	fmt.Println("ChunkPut")
	store := transaction.store
	data, _ := json.Marshal(&struct{ Data []byte }{[]byte(buf)})

	store.conn.Write([]byte(fmt.Sprintf("ChunkPut:%s\n", checksum)))
	store.conn.Write([]byte(data))
	store.conn.Write([]byte("\n"))

	data, _ = store.serverReader.ReadBytes('\n')
	var result struct{ Error error }
	err := json.Unmarshal(data, &result)
	if err != nil {
		return err
	}
	return result.Error
}

func (transaction *ClientTransaction) ChunkExists(checksum string) bool {
	fmt.Println("ChunkExists")
	return transaction.store.ChunkExists(checksum)
}

func (transaction *ClientTransaction) IndexPut(buf string) error {
	fmt.Println("IndexPut")
	store := transaction.store

	data, _ := json.Marshal(&struct{ Index []byte }{[]byte(buf)})

	store.conn.Write([]byte(fmt.Sprintf("IndexPut\n")))
	store.conn.Write(data)
	store.conn.Write([]byte("\n"))

	data, _ = store.serverReader.ReadBytes('\n')
	var result struct{ Error error }
	err := json.Unmarshal(data, &result)
	if err != nil {
		return err
	}
	return result.Error
}

func (transaction *ClientTransaction) Commit(snapshot *repository.Snapshot) (*repository.Snapshot, error) {
	fmt.Println("Commit")
	store := transaction.store

	store.conn.Write([]byte("Commit\n"))
	data, _ := store.serverReader.ReadBytes('\n')
	var result struct{ Error error }
	err := json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return snapshot, result.Error
}
