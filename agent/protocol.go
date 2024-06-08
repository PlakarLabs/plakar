package agent

import (
	"crypto/ed25519"
	"os"
	"runtime"
	"time"

	"github.com/poolpOrg/go-agentbuilder/protocol"
)

const VERSION = "1.0.0"

type ReqIdentify struct {
	Timestamp time.Time
	PublicKey ed25519.PublicKey
	Version   string
}

func NewReqIdentify(publicKey ed25519.PublicKey) ReqIdentify {
	return ReqIdentify{
		Timestamp: time.Now(),
		PublicKey: publicKey,
		Version:   VERSION,
	}
}

type ResIdentify struct {
	Timestamp       time.Time
	PublicKey       ed25519.PublicKey
	Version         string
	OperatingSystem string
	Architecture    string
	Hostname        string
	NumCPU          int
}

func NewResIdentify(publicKey ed25519.PublicKey) ResIdentify {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return ResIdentify{
		Timestamp:       time.Now(),
		PublicKey:       publicKey,
		Version:         VERSION,
		OperatingSystem: runtime.GOOS,
		Architecture:    runtime.GOARCH,
		Hostname:        hostname,
		NumCPU:          runtime.NumCPU(),
	}
}

type ReqPing struct {
	Timestamp time.Time
}

func NewReqPing() ReqPing {
	return ReqPing{
		Timestamp: time.Now(),
	}
}

type ResPing struct {
	Timestamp time.Time
	Latency   time.Duration
}

func NewResPing(ping ReqPing) ResPing {
	return ResPing{
		Timestamp: time.Now(),
		Latency:   time.Since(ping.Timestamp),
	}
}

type ReqPushConfiguration struct {
}

func NewReqPushConfiguration() ReqPushConfiguration {
	return ReqPushConfiguration{}
}

type ResOK struct {
}

func NewResOK() ResOK {
	return ResOK{}
}

type ResKO struct {
	Err string
}

func NewResKO(err error) ResKO {
	return ResKO{
		Err: err.Error(),
	}
}

func init() {
	protocol.Register(ReqIdentify{})
	protocol.Register(ResIdentify{})

	protocol.Register(ReqPing{})
	protocol.Register(ResPing{})

	protocol.Register(ReqPushConfiguration{})

	protocol.Register(ResOK{})
	protocol.Register(ResKO{})
}
