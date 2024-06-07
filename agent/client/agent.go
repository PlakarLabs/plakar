package client

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"runtime"
	"time"

	"github.com/PlakarLabs/plakar/agent"
	"github.com/PlakarLabs/plakar/agent/server"
)

type Agent struct {
	remoteAddr string

	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey

	protocol *agent.Protocol

	proxify *server.Server
}

func init() {
	agent.ProtocolRegister()
}

func NewAgent(remoteAddr string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey, proxify *server.Server) *Agent {
	return &Agent{
		publicKey:  publicKey,
		privateKey: privateKey,
		remoteAddr: remoteAddr,
		proxify:    proxify,
	}
}

func (a *Agent) Run(server string) {
	for {
		a.session(server)
		time.Sleep(5 * time.Second)
	}
}

func (a *Agent) session(server string) {
	location, err := url.Parse("plakman://" + server)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := a.connectTCP(location)
	if err != nil {
		log.Fatal(err)
	}

	a.protocol = agent.NewProtocol(conn)
	a.handleConnection()
}

func (a *Agent) connectTCP(location *url.URL) (*net.TCPConn, error) {
	port := location.Port()
	if port == "" {
		port = "8081"
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", location.Hostname()+":"+port)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Fatal(err)
	}
	return conn, nil
}

func (a *Agent) handleConnection() {
	fmt.Printf("[%s] connected\n", a.remoteAddr)

	incoming := a.protocol.Incoming()
	exit := false
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
			switch payload := packet.Payload.(type) {
			case agent.ReqPing:
				now := time.Now()
				fmt.Printf("[%s] -> ping request at %s\n", a.remoteAddr, payload.Timestamp)
				packet.Response(agent.ResPing{Timestamp: now})
				fmt.Printf("[%s] <- ping response at %s\n", a.remoteAddr, now)

			case agent.ReqIdentify:
				fmt.Printf("[%s] -> identify request from %s\n",
					a.remoteAddr, base64.RawStdEncoding.EncodeToString(payload.PublicKey))

				hostname, err := os.Hostname()
				if err != nil {
					hostname = "localhost"
				}

				packet.Response(agent.ResIdentify{
					PublicKey:       a.publicKey,
					OperatingSystem: runtime.GOOS,
					Architecture:    runtime.GOARCH,
					ProtocolVersion: agent.VERSION,
					Hostname:        hostname,
					NumCPU:          runtime.NumCPU(),
				})
				fmt.Printf("[%s] <- identify response\n", a.remoteAddr)

			default:
				fmt.Printf("[%s] unknown packet\n", a.remoteAddr)
				exit = true
			}

		case <-time.After(10 * time.Second):
			fmt.Println("ticker timeout")
		}
	}
	fmt.Printf("[%s] disconnected\n", a.remoteAddr)
}
