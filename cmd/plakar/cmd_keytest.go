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
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"syscall"

	"github.com/poolpOrg/plakar/repository"
	"github.com/poolpOrg/plakar/repository/compression"
	"github.com/poolpOrg/plakar/repository/encryption"
	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/crypto/ssh/terminal"
)

func cmd_keytest(store repository.Store, args []string) {
	file, _ := os.Open(args[0])
	reader := bufio.NewReader(file)
	buf, _ := ioutil.ReadAll(reader)
	file.Close()

	fmt.Fprintf(os.Stderr, "password: ")
	passphrase, _ := terminal.ReadPassword(syscall.Stdin)

	keypair, err := encryption.Keyload(passphrase, buf)
	if err != nil {
	}

	pem, _ := keypair.Encrypt(passphrase)
	fmt.Printf("%s", pem)
}

func cmd_keytest2(store repository.Store, args []string) {
	file, _ := os.Open(args[0])
	reader := bufio.NewReader(file)
	b, _ := ioutil.ReadAll(reader)
	pembuf, _ := pem.Decode(b)
	ciphertext := pembuf.Bytes

	fmt.Fprintf(os.Stderr, "password: ")
	password, _ := terminal.ReadPassword(syscall.Stdin)

	salt, ciphertext := ciphertext[:16], ciphertext[16:]
	dk := pbkdf2.Key(password, salt, 4096, 32, sha256.New)

	block, err := aes.NewCipher(dk)
	aesGCM, err := cipher.NewGCM(block)
	nonce, ciphertext := ciphertext[:aesGCM.NonceSize()], ciphertext[aesGCM.NonceSize():]

	cleartext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\ninvalid passphrase, can't decrypt keypair\n")
		return
	}

	cleartext, _ = compression.Inflate(cleartext)

	type SerializedKeypair struct {
		Uuid    string
		Private string
		Public  string
		Master  string
	}
	var keypair SerializedKeypair
	json.Unmarshal(cleartext, &keypair)
	fmt.Println()
	fmt.Println("Uuid:", keypair.Uuid)
	fmt.Println("Private:", keypair.Private)
	fmt.Println("Public:", keypair.Public)
	fmt.Println("Master:", keypair.Master)
}
