package encryption

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

func BuildSecretFromPassphrase(passphrase []byte) string {
	salt := make([]byte, 16)
	rand.Read(salt)
	dk := pbkdf2.Key(passphrase, salt, 4096, 32, sha256.New)
	secret := sha256.Sum256(dk)
	return base64.StdEncoding.EncodeToString(append(salt, secret[:]...))
}

func DeriveSecret(passphrase []byte, secret string) ([]byte, error) {
	decoded_secret, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return nil, err
	}

	salt, sum := decoded_secret[0:16], decoded_secret[16:]
	dk := pbkdf2.Key(passphrase, salt, 4096, 32, sha256.New)
	dksum := sha256.Sum256(dk)
	if !bytes.Equal(dksum[:], sum) {
		return nil, fmt.Errorf("passphrase does not match")
	}
	return dk, nil
}
