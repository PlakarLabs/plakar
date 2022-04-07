/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package encryption

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"

	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/compression"
	"golang.org/x/crypto/pbkdf2"
)

func KeypairGenerate() (*Keypair, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, err
	}

	keypair := &Keypair{}
	keypair.CreationTime = time.Now()
	keypair.Uuid = uuid.NewString()
	keypair.PrivateKey = privateKey
	keypair.PublicKey = publicKey
	keypair.Key = make([]byte, 32)
	rand.Read(keypair.Key)

	return keypair, nil
}

func KeypairLoad(passphrase []byte, data []byte) (*Keypair, error) {
	keypair := &Keypair{}
	data, err := keypair.Decrypt(passphrase, data)
	if err != nil {
		return nil, err
	}
	return keypair.Deserialize(data)
}

func (keypair *Keypair) Serialize() (*SerializedKeypair, error) {
	skeypair := &SerializedKeypair{}
	skeypair.CreationTime = keypair.CreationTime
	skeypair.Uuid = keypair.Uuid
	skeypair.PrivateKey = base64.StdEncoding.EncodeToString(keypair.PrivateKey)
	skeypair.PublicKey = base64.StdEncoding.EncodeToString(keypair.PublicKey)
	skeypair.Key = base64.StdEncoding.EncodeToString(keypair.Key)

	return skeypair, nil
}

func (keypair *Keypair) Deserialize(data []byte) (*Keypair, error) {
	skeypair := &SerializedKeypair{}
	err := json.Unmarshal(data, &skeypair)
	if err != nil {
		return nil, err
	}

	privateKey, err := base64.StdEncoding.DecodeString(skeypair.PrivateKey)
	if err != nil {
		return nil, err
	}
	publicKey, err := base64.StdEncoding.DecodeString(skeypair.PublicKey)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(skeypair.Key)
	if err != nil {
		return nil, err
	}

	nkeypair := &Keypair{}
	nkeypair.CreationTime = skeypair.CreationTime
	nkeypair.Uuid = skeypair.Uuid
	nkeypair.PrivateKey = privateKey
	nkeypair.PublicKey = publicKey
	nkeypair.Key = key

	return nkeypair, nil
}

func (keypair *Keypair) Public() (*PublicKey, error) {
	publicKey := &PublicKey{}
	publicKey.CreationTime = keypair.CreationTime
	publicKey.Uuid = keypair.Uuid
	publicKey.PublicKey = keypair.PublicKey
	return publicKey, nil
}

func (publicKey *PublicKey) Serialize() (*SerializedPublicKey, error) {

	x509pub, err := x509.MarshalPKIXPublicKey(publicKey.PublicKey)
	if err != nil {
		return nil, err
	}

	spublicKey := &SerializedPublicKey{}
	spublicKey.CreationTime = publicKey.CreationTime
	spublicKey.Uuid = publicKey.Uuid
	spublicKey.PublicKey = base64.StdEncoding.EncodeToString(x509pub)
	return spublicKey, nil
}

func (pubKey *PublicKey) Deserialize(data []byte) (*PublicKey, error) {
	spublicKey := &SerializedPublicKey{}
	err := json.Unmarshal(data, &spublicKey)
	if err != nil {
		return nil, err
	}

	publicKey, err := base64.StdEncoding.DecodeString(spublicKey.PublicKey)
	if err != nil {
		return nil, err
	}

	npubKey := &PublicKey{}
	npubKey.CreationTime = pubKey.CreationTime
	npubKey.Uuid = pubKey.Uuid
	npubKey.PublicKey = publicKey

	return npubKey, nil
}

func (keypair *Keypair) Encrypt(passphrase []byte) ([]byte, error) {
	serialized, err := keypair.Serialize()
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(serialized)
	if err != nil {
		return nil, err
	}
	data = compression.Deflate(data)

	salt := make([]byte, 16)
	rand.Read(salt)
	dk := pbkdf2.Key(passphrase, salt, 4096, 32, sha256.New)

	block, _ := aes.NewCipher(dk)
	aesGCM, err := cipher.NewGCM(block)
	nonce := make([]byte, aesGCM.NonceSize())
	rand.Read(nonce)
	return append(salt[:], aesGCM.Seal(nonce, nonce, data, nil)[:]...), nil
}

func (keypair *Keypair) Decrypt(passphrase []byte, data []byte) ([]byte, error) {
	salt, ciphertext := data[:16], data[16:]
	dk := pbkdf2.Key(passphrase, salt, 4096, 32, sha256.New)

	block, err := aes.NewCipher(dk)
	aesGCM, err := cipher.NewGCM(block)
	nonce, ciphertext := ciphertext[:aesGCM.NonceSize()], ciphertext[aesGCM.NonceSize():]

	cleartext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return compression.Inflate(cleartext)
}

func (keypair *Keypair) Sign(data []byte) ([]byte, error) {
	return keypair.PrivateKey.Sign(nil, data, crypto.Hash(0))
}

func (keypair *Keypair) Verify(data []byte, sig []byte) bool {
	return ed25519.Verify(keypair.PublicKey, data, sig)
}
