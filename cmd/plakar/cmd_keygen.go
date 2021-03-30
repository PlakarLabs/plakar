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

package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"syscall"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/repository"
	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/crypto/ssh/terminal"
)

func cmd_keygen(store repository.Store, args []string) {
	pubkeyCurve := elliptic.P384() //see http://golang.org/pkg/crypto/elliptic/#P256

	//privatekey := new(ecdsa.PrivateKey)
	privateKey, err := ecdsa.GenerateKey(pubkeyCurve, rand.Reader) // this generates a public & private key pair

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	x509Encoded, _ := x509.MarshalECPrivateKey(privateKey)
	base64priv := base64.StdEncoding.EncodeToString(x509Encoded)

	x509EncodedPub, _ := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	base64pub := base64.StdEncoding.EncodeToString(x509EncodedPub)

	masterKey := make([]byte, 32)
	rand.Read(masterKey)
	base64master := base64.StdEncoding.EncodeToString(masterKey)

	type SerializedKeypair struct {
		Name    string
		Private string
		Public  string
		Master  string
	}
	var keypair SerializedKeypair
	keypair.Name = uuid.NewString()
	keypair.Private = base64priv
	keypair.Public = base64pub
	keypair.Master = base64master

	data, _ := json.Marshal(keypair)

	password := []byte("")
	for {
		fmt.Fprintf(os.Stderr, "password: ")
		bytePassword1, _ := terminal.ReadPassword(syscall.Stdin)
		fmt.Fprintf(os.Stderr, "\nconfirm: ")
		bytePassword2, _ := terminal.ReadPassword(syscall.Stdin)
		if string(bytePassword1) != string(bytePassword2) {
			fmt.Fprintf(os.Stderr, "\npasswords mismatch, try again.\n")
			continue
		}
		fmt.Fprintf(os.Stderr, "\n")
		password = bytePassword1
		break
	}

	salt := make([]byte, 16)
	rand.Read(salt)
	dk := pbkdf2.Key(password, salt, 4096, 32, sha256.New)

	block, _ := aes.NewCipher(dk)
	aesGCM, err := cipher.NewGCM(block)
	nonce := make([]byte, aesGCM.NonceSize())
	rand.Read(nonce)
	ciphertext := append(salt[:], aesGCM.Seal(nonce, nonce, data, nil)[:]...)

	fmt.Printf("%s\n", base64.StdEncoding.EncodeToString(ciphertext))
	fmt.Fprintf(os.Stderr, "keypair %s generated\n", keypair.Name)
}
