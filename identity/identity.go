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

type SealedIdentity struct {
	Identifier uuid.UUID
	Timestamp  time.Time
	Address    string
	PublicKey  []byte
	PrivateKey []byte
}

type Identity struct {
	Identifier uuid.UUID
	Timestamp  time.Time
	Address    string
	KeyPair    keypair.KeyPair
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
			KeyPair:    keypair,
		}, nil
	}
}

func Load(keyringDir string, identifier uuid.UUID) (*SealedIdentity, error) {
	data, err := os.ReadFile(filepath.Join(keyringDir, identifier.String()))
	if err != nil {
		return nil, err
	}

	var si SealedIdentity
	if err := msgpack.Unmarshal(data, &si); err != nil {
		return nil, err
	}

	return &si, nil
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

func Unseal(data []byte, passphrase []byte) (*Identity, error) {
	var si SealedIdentity
	if err := msgpack.Unmarshal(data, &si); err != nil {
		return nil, err
	}

	data = si.PrivateKey
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

	i := &Identity{
		Identifier: si.Identifier,
		Timestamp:  si.Timestamp,
		Address:    si.Address,
		KeyPair: keypair.KeyPair{
			PublicKey:  si.PublicKey,
			PrivateKey: buf,
		},
	}
	return i, nil
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

	si := SealedIdentity{
		Identifier: i.Identifier,
		Timestamp:  i.Timestamp,
		Address:    i.Address,
		PublicKey:  i.KeyPair.PublicKey,
	}
	rd, err := encryption.EncryptStream(dk, bytes.NewReader(i.KeyPair.PrivateKey))
	if err != nil {
		return nil, err
	}
	buf, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	si.PrivateKey = append(salt, buf...)

	if data, err := msgpack.Marshal(si); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

func (i *Identity) Sign(data []byte) []byte {
	return i.KeyPair.Sign(data)
}

func (i *Identity) Verify(data []byte, signature []byte) bool {
	return i.KeyPair.Verify(data, signature)
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
	return i.KeyPair.PublicKey
}

func (i *Identity) GetPrivateKey() []byte {
	return i.KeyPair.PrivateKey
}

func (i *Identity) GetKeypair() keypair.KeyPair {
	return i.KeyPair
}
