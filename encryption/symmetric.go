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
)

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
