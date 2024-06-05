package agent

import (
	"encoding/gob"

	"github.com/google/uuid"
)

type Request struct {
	Uuid    uuid.UUID
	Type    string
	Payload interface{}
}

type ReqPing struct {
}

type ResPing struct {
}

type ReqInfo struct {
}

type ResInfo struct {
	OperatingSystem string
	Architecture    string
}

func ProtocolRegister() {
	gob.Register(Request{})

	gob.Register(ReqPing{})
	gob.Register(ResPing{})

	gob.Register(ReqInfo{})
	gob.Register(ResInfo{})
}
