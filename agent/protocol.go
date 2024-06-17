package agent

import (
	"crypto/ed25519"
	"os"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/go-agentbuilder/protocol"
)

const VERSION = "1.0.0"

type ReqIdentify struct {
	Timestamp time.Time
	PublicKey ed25519.PublicKey
	Version   string
	Challenge []byte
}

func NewReqIdentify(publicKey ed25519.PublicKey, challenge []byte) ReqIdentify {
	return ReqIdentify{
		Timestamp: time.Now(),
		PublicKey: publicKey,
		Version:   VERSION,
		Challenge: challenge,
	}
}

type ResIdentify struct {
	Timestamp         time.Time
	PublicKey         ed25519.PublicKey
	Version           string
	ChallengeResponse []byte

	OperatingSystem string
	Architecture    string
	Hostname        string
	NumCPU          int
}

func NewResIdentify(publicKey ed25519.PublicKey, challengeResponse []byte) ResIdentify {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return ResIdentify{
		Timestamp:         time.Now(),
		PublicKey:         publicKey,
		Version:           VERSION,
		ChallengeResponse: challengeResponse,

		OperatingSystem: runtime.GOOS,
		Architecture:    runtime.GOARCH,
		Hostname:        hostname,
		NumCPU:          runtime.NumCPU(),
	}
}

type ResIdentifyChallenge struct {
	ChallengeResponse []byte
}

func NewResIdentifyChallenge(challengeResponse []byte) ResIdentifyChallenge {
	return ResIdentifyChallenge{
		ChallengeResponse: challengeResponse,
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

type Task struct {
	ID          uuid.UUID
	Name        string
	Source      string
	Destination string

	StartAT  time.Time
	Interval time.Duration
	Keep     time.Duration
}

func NewTask(id uuid.UUID, name string, source string, destination string, startAt time.Time, interval time.Duration, keep time.Duration) Task {
	return Task{
		ID:          uuid.New(),
		Name:        name,
		Source:      source,
		Destination: destination,

		StartAT:  startAt,
		Interval: interval,
		Keep:     keep,
	}
}

type ReqPushConfiguration struct {
	Tasks []Task
}

func NewReqPushConfiguration(tasks []Task) ReqPushConfiguration {
	return ReqPushConfiguration{
		Tasks: tasks,
	}
}

type ReqTaskEvent struct {
	Name  string
	Event string
}

func NewReqTaskEvent(name string, event string) ReqTaskEvent {
	return ReqTaskEvent{
		Name:  name,
		Event: event,
	}
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

	protocol.Register(ResIdentifyChallenge{})
	protocol.Register(ReqTaskEvent{})

	protocol.Register(ReqPing{})
	protocol.Register(ResPing{})

	protocol.Register(ReqPushConfiguration{})

	protocol.Register(ResOK{})
	protocol.Register(ResKO{})
}
