package server

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/PlakarLabs/plakar/agent"
	"github.com/google/uuid"
)

func init() {
	agent.ProtocolRegister()
}

var sessions = make(map[string]*Agent)
var sessionsMutex sync.Mutex

func Sessions() []Agent {
	sessionsMutex.Lock()
	defer sessionsMutex.Unlock()
	var result []Agent
	for k := range sessions {
		result = append(result, *sessions[k])
	}
	return result
}

func addAgent(a *Agent) {
	sessionsMutex.Lock()
	defer sessionsMutex.Unlock()
	sessions[a.AgentID] = a
}

type Agent struct {
	conn    net.Conn
	encoder *gob.Encoder
	decoder *gob.Decoder
	mu      sync.Mutex

	inflightRequests map[uuid.UUID]chan agent.Request
	notifications    chan agent.Request
	disconnect       chan struct{}

	AgentID         string    `json:"agent_id"`
	Uptime          time.Time `json:"uptime"`
	Address         string    `json:"address"`
	OperatingSystem string    `json:"operating_system"`
	Architecture    string    `json:"architecture"`
}

func Server(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		a := NewAgent(c, c, c)
		go a.Handle()
	}
}

func NewAgent(conn net.Conn, rd io.Reader, wr io.Writer) *Agent {
	return &Agent{
		AgentID:          uuid.NewString(),
		Uptime:           time.Now(),
		Address:          conn.RemoteAddr().String(),
		conn:             conn,
		decoder:          gob.NewDecoder(rd),
		encoder:          gob.NewEncoder(wr),
		inflightRequests: make(map[uuid.UUID]chan agent.Request),
		notifications:    make(chan agent.Request),
		disconnect:       make(chan struct{}),
	}
}

func (a *Agent) Handle() {
	fmt.Printf("[%s] connected\n", a.AgentID)
	go func() {
		for m := range a.notifications {
			a.mu.Lock()
			notify := a.inflightRequests[m.Uuid]
			a.mu.Unlock()
			notify <- m
		}
	}()

	go func() {
		for {
			result := agent.Request{}
			err := a.decoder.Decode(&result)
			if err != nil {
				a.conn.Close()
				a.disconnect <- struct{}{}
				return
			}
			a.notifications <- result
		}
	}()

	disonnected := false
	err := a.Info()
	if err != nil {
		fmt.Println(err)
		disonnected = true
	}
	if !disonnected {
		addAgent(a)
		for {
			select {
			case <-time.After(10 * time.Second):
				a.Ping()
			case <-a.disconnect:
				disonnected = true
			}
			if disonnected {
				break
			}
		}
	}
	fmt.Printf("[%s] disconnected\n", a.AgentID)
}

func (a *Agent) sendRequest(Type string, Payload interface{}) (*agent.Request, error) {
	Uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	request := agent.Request{
		Uuid:    Uuid,
		Type:    Type,
		Payload: Payload,
	}

	notify := make(chan agent.Request)
	a.mu.Lock()
	a.inflightRequests[request.Uuid] = notify
	a.mu.Unlock()

	err = a.encoder.Encode(&request)
	if err != nil {
		return nil, err
	}

	result := <-notify
	a.mu.Lock()
	delete(a.inflightRequests, request.Uuid)
	a.mu.Unlock()
	close(notify)
	return &result, nil
}

func (a *Agent) Ping() error {
	fmt.Printf("[%s] <- ping request\n", a.AgentID)
	_, err := a.sendRequest("ReqPing", agent.ReqPing{})
	if err != nil {
		return err
	}
	fmt.Printf("[%s] -> ping response\n", a.AgentID)
	return nil
}

func (a *Agent) Info() error {
	fmt.Printf("[%s] <- info request\n", a.AgentID)
	info, err := a.sendRequest("ReqInfo", agent.ReqInfo{})
	if err != nil {
		return err
	}

	a.OperatingSystem = info.Payload.(agent.ResInfo).OperatingSystem
	a.Architecture = info.Payload.(agent.ResInfo).Architecture

	fmt.Printf("[%s] -> info response\n", a.AgentID)
	return nil
}
