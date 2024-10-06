package identity

import (
	"net/mail"

	"github.com/PlakarLabs/plakar/encryption/keypair"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type Identity struct {
	Identifier uuid.UUID
	Address    string
	Keypair    keypair.KeyPair
}

func New(address string, keypair keypair.KeyPair) (*Identity, error) {
	if _, err := mail.ParseAddress(address); err != nil {
		return nil, err
	}

	if identifier, err := uuid.NewRandom(); err != nil {
		return nil, err
	} else {
		return &Identity{
			Identifier: identifier,
			Address:    address,
			Keypair:    keypair,
		}, nil
	}
}

func FromBytes(data []byte) (*Identity, error) {
	var i Identity
	if err := msgpack.Unmarshal(data, &i); err != nil {
		return nil, err
	}
	return &i, nil
}

func (i *Identity) ToBytes() ([]byte, error) {
	if data, err := msgpack.Marshal(i); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

func (i *Identity) Sign(data []byte) []byte {
	return i.Keypair.Sign(data)
}

func (i *Identity) Verify(data []byte, signature []byte) bool {
	return i.Keypair.Verify(data, signature)
}

func (i *Identity) GetIdentifier() uuid.UUID {
	return i.Identifier
}

func (i *Identity) GetAddress() string {
	return i.Address
}

func (i *Identity) GetPublicKey() []byte {
	return i.Keypair.PublicKey
}

func (i *Identity) GetPrivateKey() []byte {
	return i.Keypair.PrivateKey
}

func (i *Identity) GetKeypair() keypair.KeyPair {
	return i.Keypair
}
