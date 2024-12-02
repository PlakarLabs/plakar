package objects

import (
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

type Checksum [32]byte

func (m Checksum) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%0x", m[:]))
}

type CustomMetadata struct {
	Key   string `msgpack:"key" json:"key"`
	Value []byte `msgpack:"value" json:"value"`
}

type Object struct {
	Checksum       Checksum         `msgpack:"checksum" json:"checksum"`
	Chunks         []Chunk          `msgpack:"chunks" json:"chunks"`
	ContentType    string           `msgpack:"content_type,omitempty" json:"content_type"`
	CustomMetadata []CustomMetadata `msgpack:"custom_metadata,omitempty" json:"custom_metadata"`
	Tags           []string         `msgpack:"tags,omitempty" json:"tags"`
	Entropy        float64          `msgpack:"entropy,omitempty" json:"entropy"`
}

func NewObject() *Object {
	return &Object{
		CustomMetadata: make([]CustomMetadata, 0),
	}
}

func NewObjectFromBytes(serialized []byte) (*Object, error) {
	var o Object
	if err := msgpack.Unmarshal(serialized, &o); err != nil {
		return nil, err
	}
	if o.CustomMetadata == nil {
		o.CustomMetadata = make([]CustomMetadata, 0)
	}
	if o.Tags == nil {
		o.Tags = make([]string, 0)
	}
	return &o, nil
}

func (o *Object) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(o)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

type Chunk struct {
	Checksum Checksum `msgpack:"checksum" json:"checksum"`
	Length   uint32   `msgpack:"length" json:"length"`
	Entropy  float64  `msgpack:"entropy" json:"entropy"`
}
