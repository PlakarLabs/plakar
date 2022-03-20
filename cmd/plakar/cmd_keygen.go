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

	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/local"
)

func init() {
	registerCommand("keygen", cmd_keygen)
}

func keypairGenerate() ([]byte, error) {
	keypair, err := encryption.Keygen()
	if err != nil {
		return nil, err
	}

	var passphrase []byte
	for {
		passphrase, err = helpers.GetPassphraseConfirm()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			continue
		}
		break
	}

	pem, err := keypair.Encrypt(passphrase)
	if err != nil {
		return nil, err
	}

	return pem, err
}

func cmd_keygen(ctx Plakar, args []string) int {
	_, err := local.GetEncryptedKeypair(ctx.Workdir)
	if err == nil {
		fmt.Fprintf(os.Stderr, "key already exists in local store\n")
		return 1
	}

	encryptedKeypair, err := keypairGenerate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not generate keypair: %s\n", err)
		return 1
	}
	err = local.SetEncryptedKeypair(ctx.Workdir, encryptedKeypair)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not save keypair in local store: %s\n", err)
		return 1
	}

	return 0
}
