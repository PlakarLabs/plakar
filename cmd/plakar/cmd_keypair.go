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
	"flag"
	"fmt"
	"os"

	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("keypair", cmd_keypair)
}

func cmd_keypair(ctx Plakar, store *storage.Store, args []string) int {
	flags := flag.NewFlagSet("keypair", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "%s: need at list one parameter\n", flag.CommandLine.Name())
		return 1
	}

	cmd, _ := flags.Arg(0), flags.Args()[1:]
	switch cmd {
	case "info":
		encryptedKeypair, err := ctx.Workdir.GetEncryptedKeypair()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get keypair\n", flag.CommandLine.Name())
			return 1
		}

		var keypair *encryption.Keypair
		for {
			passphrase, err := helpers.GetPassphrase("keypair")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			keypair, err = encryption.KeypairLoad(passphrase, encryptedKeypair)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			break
		}
		skeypair, err := keypair.Serialize()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not serialize keypair\n", flag.CommandLine.Name())
			return 1
		}

		fmt.Println("Uuid:", skeypair.Uuid)
		fmt.Println("CreationTime:", skeypair.CreationTime)
		fmt.Println("Key:", skeypair.Key)
		fmt.Println("Private:", skeypair.PrivateKey)
		fmt.Println("Public:", skeypair.PublicKey)

	case "passphrase":
		encryptedKeypair, err := ctx.Workdir.GetEncryptedKeypair()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get keypair\n", flag.CommandLine.Name())
			return 1
		}

		var keypair *encryption.Keypair
		for {
			passphrase, err := helpers.GetPassphrase("current keypair")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			keypair, err = encryption.KeypairLoad(passphrase, encryptedKeypair)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			break
		}

		var passphrase []byte
		for {
			passphrase, err = helpers.GetPassphraseConfirm("new keypair")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			break
		}

		pem, err := keypair.Encrypt(passphrase)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return 1
		}

		err = ctx.Workdir.SaveEncryptedKeypair(pem)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not save keypair in local store: %s\n", err)
			return 1
		}

	default:
		fmt.Fprintf(os.Stderr, "%s: unknown subcommand: %s\n", flag.CommandLine.Name(), cmd)
		return 1
	}

	return 0
}
