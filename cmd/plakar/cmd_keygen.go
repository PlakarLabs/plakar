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
	"fmt"
	"os"
	"syscall"

	"github.com/poolpOrg/plakar/repository"
	"github.com/poolpOrg/plakar/repository/encryption"
	"golang.org/x/crypto/ssh/terminal"
)

func cmd_keygen(store repository.Store, args []string) {
	keypair, err := encryption.Keygen()
	if err != nil {
	}

	passphrase := []byte("")
	for {
		fmt.Fprintf(os.Stderr, "passphrase: ")
		passphrase1, _ := terminal.ReadPassword(syscall.Stdin)
		fmt.Fprintf(os.Stderr, "\npassphrase (confirm): ")
		passphrase2, _ := terminal.ReadPassword(syscall.Stdin)
		if string(passphrase1) != string(passphrase2) {
			fmt.Fprintf(os.Stderr, "\npassphrases mismatch, try again.\n")
			continue
		}
		fmt.Fprintf(os.Stderr, "\n")
		passphrase = passphrase1
		break
	}

	pem, err := keypair.Encrypt(passphrase)
	if err != nil {

	}

	fmt.Printf("%s", pem)
	fmt.Fprintf(os.Stderr, "keypair %s generated\n", keypair.Uuid)
}
