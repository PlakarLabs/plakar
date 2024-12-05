package agent

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	"github.com/PlakarKorp/plakar/context"
)

type Agent struct {
	socketPath string
	listener   net.Listener
	ctx        *context.Context
}

func NewAgent(ctx *context.Context, network string, address string) (*Agent, error) {
	if network != "unix" {
		return nil, fmt.Errorf("unsupported network: %s", network)
	}

	a := &Agent{
		socketPath: address,
		ctx:        ctx,
	}

	if _, err := os.Stat(a.socketPath); err == nil {
		if !a.checkSocket() {
			a.Close()
		} else {
			return nil, fmt.Errorf("already running")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	listener, err := net.Listen("unix", a.socketPath)
	if err != nil {
		return nil, err
	}
	a.listener = listener

	if err := os.Chmod(a.socketPath, 0600); err != nil {
		a.Close()
		return nil, err
	}

	return a, nil
}

func (a *Agent) checkSocket() bool {
	conn, err := net.Dial("unix", a.socketPath)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (a *Agent) Close() {
	if a.listener != nil {
		a.listener.Close()
	}
	os.Remove(a.socketPath)
}

func (a *Agent) Run(hdl func(ctx *context.Context, session *Session)) {
	logger := a.ctx.Logger

	logger.Info("agent listening on %s", a.socketPath)
	for {
		conn, err := a.listener.Accept()
		if err != nil {
			logger.Warn("failed to accept connection: %v", err)
			continue
		}
		go func() {
			defer conn.Close()
			hdl(a.ctx, NewSession(a, conn))
		}()
	}
}

type Session struct {
	agent   *Agent
	encoder *json.Encoder
	decoder *json.Decoder
}

func NewSession(agent *Agent, conn net.Conn) *Session {
	return &Session{
		agent:   agent,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
	}
}

func (s *Session) Read() (*Command, error) {
	var packet Command
	if err := s.decoder.Decode(&packet); err != nil {
		return nil, err
	}
	return &packet, nil
}

func (s *Session) Write(resp Response) error {
	return s.encoder.Encode(resp)
}

func (s *Session) Stdout(msg string) error {
	return s.encoder.Encode(newStdout(msg))
}

func (s *Session) Stderr(msg string) error {
	return s.encoder.Encode(newStderr(msg))
}

func (s *Session) Result(err error) error {
	return s.encoder.Encode(newError(err))
}
