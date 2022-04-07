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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/compression"
	"golang.org/x/crypto/pbkdf2"
)

func SecretGenerate() (*Secret, error) {
	secret := &Secret{}
	secret.CreationTime = time.Now()
	secret.Uuid = uuid.NewString()
	secret.Key = make([]byte, 32)
	rand.Read(secret.Key)
	return secret, nil
}

func SecretLoad(key []byte, data []byte) (*Secret, error) {
	secret := &Secret{}
	data, err := secret.Decrypt(key, data)
	if err != nil {
		return nil, err
	}
	return secret.Deserialize(data)
}

func (secret *Secret) Serialize() (*SerializedSecret, error) {
	ssecret := &SerializedSecret{}
	ssecret.CreationTime = secret.CreationTime
	ssecret.Uuid = secret.Uuid
	ssecret.Key = base64.StdEncoding.EncodeToString(secret.Key)
	return ssecret, nil
}

func (secret *Secret) Deserialize(data []byte) (*Secret, error) {
	ssecret := &SerializedSecret{}
	err := json.Unmarshal(data, &ssecret)
	if err != nil {
		return nil, err
	}

	key, err := base64.StdEncoding.DecodeString(ssecret.Key)
	if err != nil {
		return nil, err
	}

	nsecret := &Secret{}
	nsecret.CreationTime = ssecret.CreationTime
	nsecret.Uuid = ssecret.Uuid
	nsecret.Key = key

	return nsecret, nil
}

func (secret *Secret) Encrypt(key []byte) ([]byte, error) {
	serialized, err := secret.Serialize()
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
	dk := pbkdf2.Key(key, salt, 4096, 32, sha256.New)

	block, _ := aes.NewCipher(dk)
	aesGCM, err := cipher.NewGCM(block)
	nonce := make([]byte, aesGCM.NonceSize())
	rand.Read(nonce)
	return append(salt[:], aesGCM.Seal(nonce, nonce, data, nil)[:]...), nil
}

func (secret *Secret) Decrypt(key []byte, data []byte) ([]byte, error) {
	salt, ciphertext := data[:16], data[16:]
	dk := pbkdf2.Key(key, salt, 4096, 32, sha256.New)

	block, err := aes.NewCipher(dk)
	aesGCM, err := cipher.NewGCM(block)
	nonce, ciphertext := ciphertext[:aesGCM.NonceSize()], ciphertext[aesGCM.NonceSize():]

	cleartext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return compression.Inflate(cleartext)
}

func Encrypt(key []byte, buf []byte) ([]byte, error) {
	subkey := make([]byte, 32)
	rand.Read(subkey)

	ecb, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	encsubkey := make([]byte, ecb.BlockSize()*2)
	ecb.Encrypt(encsubkey[:ecb.BlockSize()], subkey[:ecb.BlockSize()])
	ecb.Encrypt(encsubkey[ecb.BlockSize():], subkey[ecb.BlockSize():])

	block, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aesGCM.NonceSize())
	rand.Read(nonce)

	return append(encsubkey[:], aesGCM.Seal(nonce, nonce, buf, nil)[:]...), nil
}

func Decrypt(key []byte, buf []byte) ([]byte, error) {
	ecb, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	subkey := make([]byte, ecb.BlockSize()*2)

	encsubkey, ciphertext := buf[:ecb.BlockSize()*2], buf[ecb.BlockSize()*2:]
	ecb.Decrypt(subkey[ecb.BlockSize():], encsubkey[ecb.BlockSize():])
	ecb.Decrypt(subkey[:ecb.BlockSize()], encsubkey[:ecb.BlockSize()])

	block, err := aes.NewCipher(subkey)
	if err != nil {
		return nil, err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce, ciphertext := ciphertext[:aesGCM.NonceSize()], ciphertext[aesGCM.NonceSize():]
	cleartext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return cleartext, nil
}
