package identity

import (
	"bytes"
	"crypto/rand"
	"io"
	"net/mail"
	"os"
	"path/filepath"
	"time"

	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/encryption/keypair"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/crypto/scrypt"
)

type Identity struct {
	Identifier uuid.UUID
	Timestamp  time.Time
	Address    string
	Keypair    keypair.KeyPair
}

func UnsealIdentity(keyringDir string, identifier uuid.UUID) (*Identity, error) {
	var err error

	data, err := os.ReadFile(filepath.Join(keyringDir, identifier.String()))
	if err != nil {
		return nil, err
	}

	attempt := 0
	maxAttempts := 3
	for attempt < maxAttempts {
		passphrase, err := utils.GetPassphrase("identity")
		if err != nil {
			return nil, err
		}
		if identity, err := Unseal(data, passphrase); err == nil {
			return identity, nil
		} else if attempt == maxAttempts {
			return nil, err
		}
		attempt++
	}
	return nil, err
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
			Timestamp:  time.Now(),
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

func Unseal(data []byte, passphrase []byte) (*Identity, error) {
	salt := data[:32]
	dk, err := scrypt.Key(passphrase, salt, 1<<15, 8, 1, 32)
	if err != nil {
		return nil, err
	}

	rd, err := encryption.DecryptStream(dk, bytes.NewReader(data[32:]))
	if err != nil {
		return nil, err
	}

	buf, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return FromBytes(buf)
}

func (i *Identity) Seal(passphrase []byte) ([]byte, error) {
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	dk, err := scrypt.Key(passphrase, salt, 1<<15, 8, 1, 32)
	if err != nil {
		return nil, err
	}
	serialized, err := i.ToBytes()
	if err != nil {
		return nil, err
	}

	rd, err := encryption.EncryptStream(dk, bytes.NewReader(serialized))
	if err != nil {
		return nil, err
	}

	buf, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return append(salt, buf...), nil
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

func (i *Identity) GetTimestamp() time.Time {
	return i.Timestamp
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
