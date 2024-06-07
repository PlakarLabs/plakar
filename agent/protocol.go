package agent

import (
	"crypto/ed25519"
	"encoding/gob"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

const VERSION = "1.0.0"

type Protocol struct {
	conn    net.Conn
	encoder *gob.Encoder
	decoder *gob.Decoder
	mu      sync.Mutex

	inflightRequests map[uuid.UUID]chan Packet
	notifications    chan Packet
}

func NewProtocol(conn net.Conn) *Protocol {
	return &Protocol{
		conn:             conn,
		encoder:          gob.NewEncoder(conn),
		decoder:          gob.NewDecoder(conn),
		inflightRequests: make(map[uuid.UUID]chan Packet),
		notifications:    make(chan Packet),
	}
}

func (p *Protocol) Request(Payload interface{}) error {
	request := p.NewRequest(Payload)
	return p.encoder.Encode(&request)
}

func (p *Protocol) Query(Payload interface{}, cb func(interface{}) error) error {
	request := p.NewRequest(Payload)

	notify := make(chan Packet)
	p.mu.Lock()
	p.inflightRequests[request.Uuid] = notify
	p.mu.Unlock()

	err := p.encoder.Encode(&request)
	if err != nil {
		return err
	}

	result := <-notify
	p.mu.Lock()
	delete(p.inflightRequests, request.Uuid)
	p.mu.Unlock()
	close(notify)

	return cb(result.Payload)
}

func (p *Protocol) Incoming() <-chan Packet {
	pchan := make(chan Packet)
	go func() {
		for m := range p.notifications {
			p.mu.Lock()
			notify := p.inflightRequests[m.Uuid]
			p.mu.Unlock()
			notify <- m
		}
	}()

	go func() {
		defer p.conn.Close()
		defer close(pchan)
		for {
			result := Packet{}
			err := p.decoder.Decode(&result)
			if err != nil {
				return
			}
			result.protocol = p
			p.mu.Lock()
			_, ok := p.inflightRequests[result.Uuid]
			p.mu.Unlock()
			if ok {
				p.notifications <- result
			} else {
				pchan <- result
			}
		}
	}()

	return pchan
}

func (p *Protocol) NewRequest(payload interface{}) Packet {
	Uuid, err := uuid.NewRandom()
	if err != nil {
		return Packet{}
	}
	return Packet{
		protocol: p,
		Uuid:     Uuid,
		Payload:  payload,
	}
}

type Packet struct {
	protocol *Protocol
	Uuid     uuid.UUID
	Payload  interface{}
}

func (p Packet) Response(payload interface{}) error {
	return p.protocol.encoder.Encode(&Packet{
		Uuid:    p.Uuid,
		Payload: payload,
	})
}

type ReqPing struct {
	Timestamp time.Time
}

type ResPing struct {
	Timestamp time.Time
}

type ReqIdentify struct {
	PublicKey ed25519.PublicKey
}

type ResIdentify struct {
	PublicKey       ed25519.PublicKey
	OperatingSystem string
	Architecture    string
	ProtocolVersion string
	Hostname        string
	NumCPU          int
}

func ProtocolRegister() {
	gob.Register(Packet{})

	gob.Register(ReqPing{})
	gob.Register(ResPing{})

	gob.Register(ReqIdentify{})
	gob.Register(ResIdentify{})
}
