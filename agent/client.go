package agent

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/go-agentbuilder/client"
	"github.com/poolpOrg/go-agentbuilder/protocol"
)

type Client struct {
	client     *client.Client
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey

	scheduler map[uuid.UUID]chan bool
}

func NewClient(address string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) *Client {
	return &Client{
		client:     client.NewClient(address),
		publicKey:  publicKey,
		privateKey: privateKey,

		scheduler: make(map[uuid.UUID]chan bool),
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

func (c *Client) ConfigureTasks(tasks []Task) error {
	// replace all existing tasks
	tasksMap := make(map[uuid.UUID]struct{})

	for _, task := range tasks {
		tasksMap[task.ID] = struct{}{}
		if _, ok := c.scheduler[task.ID]; ok {
			close(c.scheduler[task.ID])
		}
		c.scheduler[task.ID] = make(chan bool)
		go func(_task Task) {
			fmt.Println("scheduling task", _task.Name, "every", _task.Interval.String(), "keep for", _task.Keep.String(), "=>", _task.Origin)
			<-time.After(time.Until(_task.StartAT))
			for {
				select {
				case <-c.scheduler[_task.ID]:
					return
				case <-time.After(_task.Interval):
					fmt.Printf("[%s] %s: %s\n", time.Now().UTC(), _task.Name, _task.Origin)
					exec.Command(os.Args[0], "push", "-tag", _task.Name, _task.Origin).Run()

					if _task.Keep > 0 {
						now := time.Now()
						olderParam := now.Add(-_task.Keep).Format(time.RFC3339)
						exec.Command(os.Args[0], "rm", "-older", olderParam, "-tag", _task.Name).Run()
					}

				}
			}
		}(task)
	}

	for taskID := range c.scheduler {
		if _, ok := tasksMap[taskID]; !ok {
			close(c.scheduler[taskID])
			delete(c.scheduler, taskID)
		}
	}

	return nil
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
			if err := c.ConfigureTasks(req.Tasks); err != nil {
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
