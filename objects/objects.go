package objects

import (
	"github.com/vmihailenco/msgpack/v5"
)

type CustomMetadata struct {
	Key   string `msgpack:"key"`
	Value []byte `msgpack:"value"`
}

type Object struct {
	Checksum       [32]byte         `msgpack:"checksum"`
	Chunks         []Chunk          `msgpack:"chunks"`
	ContentType    string           `msgpack:"contentType,omitempty"`
	CustomMetadata []CustomMetadata `msgpack:"customMetadata,omitempty"`
	Tags           []string         `msgpack:"tags,omitempty"`
	Entropy        float64          `msgpack:"entropy,omitempty"`
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
	Checksum [32]byte `msgpack:"checksum"`
	Length   uint32   `msgpack:"length"`
	Entropy  float64  `msgpack:"entropy"`
}
