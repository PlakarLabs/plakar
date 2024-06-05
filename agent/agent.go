package agent

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"runtime"
	"sync"
	"time"
)

type Agent struct {
	server string

	encoder *gob.Encoder
	decoder *gob.Decoder
}

func init() {
	ProtocolRegister()
}

func NewAgent(server string) *Agent {
	return &Agent{
		server: server,
	}
}

func (a *Agent) Run(server string) {
	for {
		a.session(server)
		fmt.Printf("[%s] disconnected (retry in 30 seconds)\n", a.server)
		time.Sleep(10 * time.Second)
	}
}

func (a *Agent) session(server string) {
	location, err := url.Parse(server)
	if err != nil {
		log.Fatal(err)
	}

	a.encoder = nil
	a.decoder = nil

	conn, err := a.connectTCP(location)
	if err != nil {
		log.Fatal(err)
	}
	a.handleConnection(conn, conn)
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

func (a *Agent) handleConnection(rd io.Reader, wr io.Writer) {

	decoder := gob.NewDecoder(rd)
	encoder := gob.NewEncoder(wr)

	var wg sync.WaitGroup

	fmt.Printf("[%s] connected\n", a.server)
	for {
		request := Request{}
		err := decoder.Decode(&request)
		if err != nil {
			break
		}

		switch request.Type {
		case "ReqPing":
			wg.Add(1)
			go func() {
				defer wg.Done()
				fmt.Printf("[%s] -> ping request\n", a.server)
				result := Request{
					Uuid:    request.Uuid,
					Type:    "ResPing",
					Payload: ResPing{},
				}
				fmt.Printf("[%s] <- ping response\n", a.server)
				err = encoder.Encode(&result)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s", err)
				}
			}()

		case "ReqInfo":
			wg.Add(1)
			go func() {
				defer wg.Done()
				fmt.Printf("[%s] -> info request\n", a.server)
				result := Request{
					Uuid: request.Uuid,
					Type: "ResInfo",
					Payload: ResInfo{
						OperatingSystem: runtime.GOOS,
						Architecture:    runtime.GOARCH,
					},
				}
				fmt.Printf("[%s] <- info response\n", a.server)
				err = encoder.Encode(&result)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s", err)
				}
			}()

		default:
			fmt.Println("Unknown request type", request.Type)
		}
	}
	wg.Wait()
}
