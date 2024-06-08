package agent

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/go-agentbuilder/protocol"
	"github.com/poolpOrg/go-agentbuilder/server"
)

type ServerConfigTask struct {
	Name     string        `yaml:"name"`
	Source   string        `yaml:"source"`
	StartAt  string        `yaml:"tag"`
	Interval time.Duration `yaml:"interval"`
	Keep     time.Duration `yaml:"keep"`
}

type ServerConfigAgent struct {
	PublicKey string `yaml:"public_key"`
	Tasks     []Task `yaml:"tasks"`
}

type ServerConfig struct {
	Agents []ServerConfigAgent `yaml:"agents"`
}

type Session struct {
	PublicKey       ed25519.PublicKey
	OperatingSystem string
	Architecture    string
	Hostname        string
	NumCPU          int
}

func NewSession() *Session {
	return &Session{}
}

type Server struct {
	server     *server.Server
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey

	config *ServerConfig

	sessions      map[string]*Session
	sessionsMutex sync.Mutex
}

func (s *Server) GetAgentConfiguration(publicKey ed25519.PublicKey) *ServerConfigAgent {
	for _, agent := range s.config.Agents {
		if agent.PublicKey == base64.RawStdEncoding.EncodeToString(publicKey) {
			return &agent
		}
	}
	return nil
}

func NewServer(address string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey, config *ServerConfig) *Server {
	return &Server{
		server:     server.NewServer(address),
		publicKey:  publicKey,
		privateKey: privateKey,

		config: config,

		sessions: make(map[string]*Session),
	}
}

func (s *Server) ListenAndServe() {
	s.server.ListenAndServe(s.serverHandler)
}

func (s *Server) serverHandler(session *server.Session, incoming <-chan protocol.Packet) error {
	challenge := make([]byte, 32)
	_, err := rand.Read(challenge)
	if err != nil {
		return err
	}

	clientSession := NewSession()
	defer func() {
		s.sessionsMutex.Lock()
		if clientSession.PublicKey != nil {
			delete(s.sessions, base64.RawStdEncoding.EncodeToString(clientSession.PublicKey))
		}
		s.sessionsMutex.Unlock()
	}()

	var agentConfiguration *ServerConfigAgent
	err = session.Query(NewReqIdentify(s.publicKey, challenge), func(response interface{}) error {
		if res, ok := response.(ResIdentify); ok {
			if !ed25519.Verify(res.PublicKey, challenge, res.ChallengeResponse) {
				return fmt.Errorf("invalid challenge response for client %s", base64.RawStdEncoding.EncodeToString(res.PublicKey))
			}
			fmt.Println("client identified as", base64.RawStdEncoding.EncodeToString(res.PublicKey))

			clientSession.PublicKey = res.PublicKey
			clientSession.OperatingSystem = res.OperatingSystem
			clientSession.Architecture = res.Architecture
			clientSession.Hostname = res.Hostname
			clientSession.NumCPU = res.NumCPU

			s.sessionsMutex.Lock()
			s.sessions[base64.RawStdEncoding.EncodeToString(clientSession.PublicKey)] = clientSession
			s.sessionsMutex.Unlock()

			agentConfiguration = s.GetAgentConfiguration(res.PublicKey)
			if agentConfiguration == nil {
				return fmt.Errorf("unknown agent")
			}
			return nil
		} else {
			return fmt.Errorf("unexpected response type: %T", response)
		}
	})
	if err != nil {
		return err
	}

	tasks := []Task{}
	for _, configuredTask := range agentConfiguration.Tasks {
		tasks = append(tasks, NewTask(uuid.Must(uuid.NewRandom()), configuredTask.Name, configuredTask.Source, "", time.Now(), configuredTask.Interval, configuredTask.Keep))
	}

	// push configuration
	err = session.Query(NewReqPushConfiguration(tasks), func(response interface{}) error {
		switch res := response.(type) {
		case ResOK:
			return nil
		case ResKO:
			return fmt.Errorf("client configuration push failed: %s", res.Err)
		default:
			return fmt.Errorf("unexpected response type: %T", response)
		}
	})
	if err != nil {
		return err
	}

	for {
		select {
		case packet, ok := <-incoming:
			if !ok {
				return nil
			}
			switch req := packet.Payload.(type) {
			case ReqTaskEvent:
				fmt.Printf("[%s] %s: %s\n", time.Now().UTC(), req.Name, req.Event)
			default:
				return fmt.Errorf("unexpected request type: %T", req)
			}

		case <-time.After(60 * time.Second):
			fmt.Println("no packets received in 60 seconds")
		}
	}
	return nil
}
