package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"

	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
)

type client struct {
	socketPath string
	conn       net.Conn
	encoder    *json.Encoder
	decoder    *json.Decoder
	result     chan error
}

func newClient(network string, address string) (*client, error) {
	if network != "unix" {
		return nil, fmt.Errorf("unsupported network: %s", network)
	}

	conn, err := net.Dial("unix", address)
	if err != nil {
		return nil, err
	}

	return &client{
		socketPath: address,
		conn:       conn,
		encoder:    json.NewEncoder(conn),
		decoder:    json.NewDecoder(conn),
		result:     make(chan error),
	}, nil
}

func (c *client) close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *client) send(ctx *context.Context, repo *repository.Repository, argv []string) error {
	packet := newCommand(ctx, repo, argv)
	go func() {
		err := c.encoder.Encode(packet)
		if err != nil {
			c.result <- err
		}
	}()
	return <-c.result
}

func NewRPC(ctx *context.Context, repo *repository.Repository, argv []string) error {
	logger := ctx.Logger

	cl, err := newClient("unix", filepath.Join(ctx.GetCacheDir(), "agent.sock"))
	if err != nil {
		return err
	}
	defer cl.close()

	go func() {
		for {
			var response Response
			err := cl.decoder.Decode(&response)
			if err != nil {
				logger.Warn("error decoding message: %v", err)
				break
			}

			switch response.Type {
			case "error":
				if len(response.Data) == 0 {
					close(cl.result)
					return
				}
				cl.result <- errors.New(string(response.Data))
				close(cl.result)
				return

			case "stdout":
				logger.Printf("%s", string(response.Data))

			case "stderr":
				logger.Warn("%s", string(response.Data))

			default:
				logger.Warn("unknown response type: %s", response.Type)
			}
		}
	}()

	return cl.send(ctx, repo, argv)
}
