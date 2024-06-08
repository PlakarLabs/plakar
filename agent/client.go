package agent

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/poolpOrg/go-agentbuilder/client"
	"github.com/poolpOrg/go-agentbuilder/protocol"
)

type Client struct {
	client     *client.Client
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey
}

func NewClient(address string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) *Client {
	return &Client{
		client:     client.NewClient(address),
		publicKey:  publicKey,
		privateKey: privateKey,
	}
}

func (c *Client) Run() {
	for {
		if err := c.client.Run(c.clientHandler); err != nil {
			fmt.Println("Error: ", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func (c *Client) clientHandler(session *client.Session, incoming <-chan protocol.Packet) error {
	for packet := range incoming {
		switch req := packet.Payload.(type) {
		case ReqIdentify:
			fmt.Println("server identified as", base64.RawStdEncoding.EncodeToString(req.PublicKey))
			packet.Response(NewResIdentify(c.publicKey))
		case ReqPing:
			fmt.Println("server sent ping request")
			packet.Response(NewResPing(req))
		case ReqPushConfiguration:
			fmt.Println("server sent agent configuration")
			packet.Response(NewResOK())
			//packet.Response(NewResKO(fmt.Errorf("not implemented")))
		default:
			return fmt.Errorf("unexpected request type: %T", req)
		}
	}
	return nil
}
