package server

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gliderlabs/ssh"
	"github.com/poolpOrg/plakar/storage"
	gossh "golang.org/x/crypto/ssh"
)

var lstore storage.Store

func Run(store storage.Store) {
	lstore = store
	server := ssh.Server{
		Addr: ":9876",
		ChannelHandlers: map[string]ssh.ChannelHandler{
			"plakar": handleChannel,
		},
	}

	log.Fatal(server.ListenAndServe())
}

func handleChannel(srv *ssh.Server, conn *gossh.ServerConn, newChan gossh.NewChannel, ctx ssh.Context) {
	fmt.Println("accepted connection")
	sshChannel, sshRequest, err := newChan.Accept()
	if err != nil {
		conn.Close()
		return
	}
	_ = sshChannel

	clientData := newChan.ExtraData()
	_ = clientData // will be used to infer plakar

	var tx storage.Transaction

	for msg := range sshRequest {
		fmt.Println(msg.Type)
		switch msg.Type {
		case "Open":
			storeConfig := lstore.Configuration()
			marshalled, m_err := json.Marshal(&storeConfig)
			if m_err != nil {
				conn.Close()
				return
			}
			sshChannel.SendRequest("Open", false, marshalled)

		case "GetIndexes":
			indexes, err := lstore.GetIndexes()
			marshalled, m_err := json.Marshal(struct {
				Indexes []string
				Err     error
			}{indexes, err})
			if m_err != nil {
				conn.Close()
				return
			}
			sshChannel.SendRequest("GetIndexes", false, marshalled)

		case "GetIndex":
			Uuid := string(msg.Payload)
			data, err := lstore.GetIndex(Uuid)
			if err != nil {
				fmt.Println(err)
			}
			marshalled, m_err := json.Marshal(struct {
				Data []byte
				Err  error
			}{data, err})
			if m_err != nil {
				conn.Close()
				return
			}
			sshChannel.SendRequest("GetIndex", false, marshalled)

		case "GetObject":
			Checksum := string(msg.Payload)
			data, err := lstore.GetObject(Checksum)
			marshalled, m_err := json.Marshal(struct {
				Data []byte
				Err  error
			}{data, err})
			if m_err != nil {
				conn.Close()
				return
			}
			sshChannel.SendRequest("GetObject", false, marshalled)

		case "GetChunk":
			Checksum := string(msg.Payload)
			data, err := lstore.GetChunk(Checksum)
			marshalled, m_err := json.Marshal(struct {
				Data []byte
				Err  error
			}{data, err})
			if m_err != nil {
				conn.Close()
				return
			}
			sshChannel.SendRequest("GetChunk", false, marshalled)

		case "Purge":
			Uuid := string(msg.Payload)
			err := lstore.Purge(Uuid)
			marshalled, m_err := json.Marshal(struct {
				Err error
			}{err})
			if m_err != nil {
				conn.Close()
				return
			}
			sshChannel.SendRequest("Purge", false, marshalled)

		case "Transaction":
			tx = lstore.Transaction()
			marshalled, m_err := json.Marshal(tx.GetUuid())
			if m_err != nil {
				conn.Close()
				return
			}
			sshChannel.SendRequest("Transaction", false, marshalled)

		case "ReferenceChunks":
			var keys []string
			um_err := json.Unmarshal(msg.Payload, &keys)
			if um_err != nil {
				fmt.Println("failed")
				conn.Close()
				return
			}

			exists, err := tx.ReferenceChunks(keys)
			marshalled, m_err := json.Marshal(struct {
				Exists []bool
				Err    error
			}{exists, err})
			if m_err != nil {
				conn.Close()
				return
			}
			_, err = sshChannel.SendRequest("ReferenceChunks", false, marshalled)
			if err != nil {
				conn.Close()
				return
			}

		case "ReferenceObjects":
			var keys []string
			um_err := json.Unmarshal(msg.Payload, &keys)
			if um_err != nil {
				conn.Close()
				return
			}

			exists, err := tx.ReferenceObjects(keys)
			marshalled, m_err := json.Marshal(struct {
				Exists []bool
				Err    error
			}{exists, err})
			if m_err != nil {
				conn.Close()
				return
			}
			_, err = sshChannel.SendRequest("ReferenceObjects", false, marshalled)
			if err != nil {
				conn.Close()
				return
			}

		case "PutObject":
			var object struct {
				Checksum string
				Data     []byte
			}
			um_err := json.Unmarshal(msg.Payload, &object)
			if um_err != nil {
				conn.Close()
				return
			}

			err := tx.PutObject(object.Checksum, object.Data)
			marshalled, m_err := json.Marshal(struct {
				Err error
			}{err})
			if m_err != nil {
				conn.Close()
				return
			}
			_, err = sshChannel.SendRequest("PutObject", false, marshalled)
			if err != nil {
				conn.Close()
				return
			}

		case "PutChunk":
			var chunk struct {
				Checksum string
				Data     []byte
			}
			um_err := json.Unmarshal(msg.Payload, &chunk)
			if um_err != nil {
				fmt.Println("###1")
				conn.Close()
				return
			}

			err := tx.PutChunk(chunk.Checksum, chunk.Data)
			marshalled, m_err := json.Marshal(struct {
				Err error
			}{err})
			if m_err != nil {
				fmt.Println("###2")
				conn.Close()
				return
			}
			_, err = sshChannel.SendRequest("PutChunk", false, marshalled)
			if err != nil {
				fmt.Println("###3")
				conn.Close()
				return
			}
			fmt.Println("###4")

		case "PutIndex":
			err := tx.PutIndex(msg.Payload)
			marshalled, m_err := json.Marshal(struct {
				Err error
			}{err})
			if m_err != nil {
				conn.Close()
				return
			}
			_, err = sshChannel.SendRequest("PutIndex", false, marshalled)
			if err != nil {
				conn.Close()
				return
			}

		case "Commit":
			err := tx.Commit()
			marshalled, m_err := json.Marshal(struct {
				Err error
			}{err})
			if m_err != nil {
				conn.Close()
				return
			}
			_, err = sshChannel.SendRequest("Commit", false, marshalled)
			if err != nil {
				conn.Close()
				return
			}
		}
	}

	fmt.Println("closed connection")
}
