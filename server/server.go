package server

import (
	"fmt"
	"log"

	"github.com/gliderlabs/ssh"
	gossh "golang.org/x/crypto/ssh"
)

func Run() {
	server := ssh.Server{
		Addr: ":2222",
		ChannelHandlers: map[string]ssh.ChannelHandler{
			"plakar": handleChannel,
		},
	}

	log.Fatal(server.ListenAndServe())
}

func handleChannel(srv *ssh.Server, conn *gossh.ServerConn, newChan gossh.NewChannel, ctx ssh.Context) {
	fmt.Println("accepted connection")
	goChan, req, err := newChan.Accept()
	if err != nil {
		conn.Close()
		return
	}
	_ = goChan

	clientData := newChan.ExtraData()
	_ = clientData // will be used to infer plakar

	for msg := range req {
		fmt.Println(msg)
		switch msg.Type {
		case "begin":
			msg.Reply(true, nil)
		}

	}

	fmt.Println("closed connection")
}
