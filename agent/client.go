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

	scheduler *Scheduler
}

func NewClient(address string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) *Client {
	return &Client{
		client:     client.NewClient(address),
		publicKey:  publicKey,
		privateKey: privateKey,

		scheduler: NewScheduler(),
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

func (c *Client) ConfigureTasks(tasks []Task, notify chan<- SchedulerEvent) error {
	// replace all existing tasks
	tasksMap := make(map[string]struct{})
	for _, task := range tasks {
		tasksMap[task.Name] = struct{}{}
		c.scheduler.Schedule(task, notify)
	}

	for taskID := range c.scheduler.tasks {
		if _, ok := tasksMap[taskID]; !ok {
			c.scheduler.Cancel(taskID)
		}
	}

	return nil
}

func (c *Client) clientHandler(session *client.Session, incoming <-chan protocol.Packet) error {
	var serverPublicKey ed25519.PublicKey

	notify := make(chan SchedulerEvent)
	go func() {
		for event := range notify {
			switch event.(type) {
			case TaskCompleted:
				session.Request(NewReqTaskEvent(event.(TaskCompleted).Name, "success"))
			}
		}
	}()

	for packet := range incoming {
		switch req := packet.Payload.(type) {
		case ReqIdentify:
			serverPublicKey = req.PublicKey
			fmt.Println("server identified as", base64.RawStdEncoding.EncodeToString(req.PublicKey))
			packet.Response(NewResIdentify(c.publicKey, ed25519.Sign(c.privateKey, req.Challenge)))
			_ = serverPublicKey

		case ReqPing:
			fmt.Println("server sent ping request")
			packet.Response(NewResPing(req))

		case ReqPushConfiguration:
			if err := c.ConfigureTasks(req.Tasks, notify); err != nil {
				packet.Response(NewResKO(err))
			} else {
				packet.Response(NewResOK())
			}
		default:
			return fmt.Errorf("unexpected request type: %T", req)
		}
	}
	return nil
}
