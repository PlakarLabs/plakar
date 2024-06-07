package server

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
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

type Agent struct {
	PublicKey       []byte        `json:"public_key"`
	Uptime          time.Time     `json:"uptime"`
	Ping            time.Time     `json:"ping"`
	Latency         time.Duration `json:"latency"`
	TimeDelta       time.Duration `json:"time_delta"`
	Address         string        `json:"address"`
	OperatingSystem string        `json:"operating_system"`
	Architecture    string        `json:"architecture"`
	ProtocolVersion string        `json:"protocol_version"`
	Hostname        string        `json:"hostname"`
	NumCPU          int           `json:"num_cpu"`
}

func NewAgent(conn net.Conn) *Agent {
	return &Agent{
		Uptime:  time.Now(),
		Address: conn.RemoteAddr().String(),
	}
}

type Server struct {
	localAddr  string
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey

	sessions      map[string]*Session
	sessionsMutex sync.Mutex
}

func NewServer(addr string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) *Server {
	return &Server{
		localAddr:  addr,
		publicKey:  publicKey,
		privateKey: privateKey,

		sessions: make(map[string]*Session),
	}
}

func (s *Server) Agents() []Agent {
	s.sessionsMutex.Lock()
	defer s.sessionsMutex.Unlock()

	var result []Agent = make([]Agent, 0, len(s.sessions))
	for k := range s.sessions {
		result = append(result, *s.sessions[k].Agent)
	}
	return result
}

func (s *Server) Sessions() []Session {
	s.sessionsMutex.Lock()
	defer s.sessionsMutex.Unlock()

	var result []Session = make([]Session, 0, len(s.sessions))
	for k := range s.sessions {
		result = append(result, *s.sessions[k])
	}
	return result
}

func (s *Server) registerSession(session *Session) error {
	s.sessionsMutex.Lock()
	defer s.sessionsMutex.Unlock()
	if _, ok := s.sessions[string(session.Agent.PublicKey)]; ok {
		return fmt.Errorf("agent already connected: %s", session.Agent.PublicKey)
	}
	s.sessions[string(session.Agent.PublicKey)] = session
	return nil
}

func (s *Server) unregisterSession(session *Session) {
	s.sessionsMutex.Lock()
	defer s.sessionsMutex.Unlock()
	delete(s.sessions, string(session.Agent.PublicKey))
}

func (s *Server) Run() {
	l, err := net.Listen("tcp", s.localAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		session := s.newSession(c, s.publicKey, s.privateKey)
		go session.Handle()
	}
}

func (s *Server) newSession(conn net.Conn, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey) *Session {
	return &Session{
		server:     s,
		sessionID:  uuid.NewString(),
		conn:       conn,
		protocol:   agent.NewProtocol(conn),
		publicKey:  publicKey,
		privateKey: privateKey,
		remoteAddr: conn.RemoteAddr().String(),
	}
}

type Session struct {
	server     *Server
	sessionID  string
	conn       net.Conn
	protocol   *agent.Protocol
	remoteAddr string

	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey

	Agent *Agent
}

func (s *Session) Request(payload interface{}) error {
	return s.protocol.Request(payload)
}

func (s *Session) Query(payload interface{}, cb func(interface{}) error) error {
	return s.protocol.Query(payload, cb)
}

func (s *Session) Handle() {
	fmt.Printf("[%s] connected\n", s.conn.RemoteAddr().String())

	s.Agent = NewAgent(s.conn)
	incoming := s.protocol.Incoming()

	exit := false

	if err := s.ping(); err != nil {
		fmt.Printf("[%s] ping error: %s\n", s.sessionID, err)
		exit = true
	} else if err := s.identify(); err != nil {
		fmt.Printf("[%s] identify error: %s\n", s.sessionID, err)
		exit = true
	}
	s.server.registerSession(s)

	for {
		if exit {
			break
		}
		select {
		case packet, ok := <-incoming:
			if !ok {
				exit = true
				break
			}
			//switch payload := packet.Payload.(type) {
			//case agent.ResPing:
			//	fmt.Printf("[%s] -> ping reply at %s\n", s.conn.RemoteAddr(), payload.Timestamp)
			//	s.protocol.Response(packet.Uuid, agent.ResPing{})
			//}
			_ = packet
		case <-time.After(300 * time.Second):
			if err := s.ping(); err != nil {
				fmt.Printf("[%s] ping error: %s\n", s.sessionID, err)
				exit = true
			}
		}
	}
	s.server.unregisterSession(s)
	fmt.Printf("[%s] disconnected\n", s.conn.RemoteAddr().String())
}

func (s *Session) ping() error {
	sentTime := time.Now()
	fmt.Printf("[%s] <- ping request at %s\n", s.remoteAddr, sentTime)
	return s.protocol.Query(agent.ReqPing{Timestamp: time.Now()}, func(res interface{}) error {
		if res, ok := res.(agent.ResPing); !ok {
			return fmt.Errorf("invalid response")
		} else {
			recvTime := time.Now()
			latency := recvTime.Sub(sentTime)
			timeDelta := res.Timestamp.Sub(sentTime) - time.Duration(latency/2)
			s.Agent.Ping = recvTime
			s.Agent.Latency = latency
			s.Agent.TimeDelta = timeDelta
			fmt.Printf("[%s] -> ping response (latency: %s, timedelta: %s)\n",
				s.remoteAddr,
				latency,
				timeDelta)
			return nil
		}
	})
}

func (s *Session) identify() error {
	sentTime := time.Now()
	fmt.Printf("[%s] <- identify request at %s\n", s.remoteAddr, sentTime)
	return s.protocol.Query(agent.ReqIdentify{PublicKey: s.publicKey}, func(res interface{}) error {
		if res, ok := res.(agent.ResIdentify); !ok {
			return fmt.Errorf("invalid response")
		} else {
			recvTime := time.Now()
			latency := recvTime.Sub(sentTime)
			s.Agent.PublicKey = res.PublicKey
			s.Agent.OperatingSystem = res.OperatingSystem
			s.Agent.Architecture = res.Architecture
			s.Agent.ProtocolVersion = res.ProtocolVersion
			s.Agent.Hostname = res.Hostname
			s.Agent.NumCPU = res.NumCPU
			s.Agent.Ping = recvTime
			s.Agent.Latency = latency

			fmt.Printf("[%s] -> identify response from %s (latency: %s)\n",
				s.remoteAddr,
				base64.RawStdEncoding.EncodeToString(res.PublicKey),
				latency)
			return nil
		}
	})
}
