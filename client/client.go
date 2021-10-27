package client

import (
	"fmt"
	"log"

	"golang.org/x/crypto/ssh"
)

type ClientStoreConfig struct {
	Uuid       string
	Encrypted  string
	Compressed string
}

type ClientStore interface {
	Create(repository string, configuration ClientStoreConfig) error
	Open(repository string) error
	Configuration() ClientStoreConfig

	Transaction() ClientTransaction

	GetIndexes() ([]string, error)
	GetIndex(id string) ([]byte, error)
	GetObject(checksum string) ([]byte, error)
	GetChunk(checksum string) ([]byte, error)

	Purge(id string) error
}

type ClientTransaction interface {
	GetUuid() string

	ReferenceObjects(keys []string) ([]bool, error)
	PutObject(checksum string, data []byte) error

	ReferenceChunks(keys []string) ([]bool, error)
	PutChunk(checksum string, data []byte) error

	PutIndex(data []byte) error
	Commit() error
}

func Run() {
	config := &ssh.ClientConfig{
		User: "username",
		Auth: []ssh.AuthMethod{
			// ...
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", "127.0.0.1:2222", config)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// pass public key as part of extra data
	goChan, req, err := conn.OpenChannel("plakar", nil)
	if err != nil {
		log.Fatal(err)
	}

	res, _ := goChan.SendRequest("begin", true, []byte("kikoo"))
	fmt.Println(res)

	for msg := range req {
		fmt.Println(msg)
	}

	fmt.Println(goChan)
	fmt.Println(req)
}
