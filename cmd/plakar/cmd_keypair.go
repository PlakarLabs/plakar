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
	"github.com/poolpOrg/plakar/local"
)

func init() {
	registerCommand("keypair", cmd_keypair)
}

func keypairGenerate() (string, []byte, error) {
	keypair, err := encryption.KeypairGenerate()
	if err != nil {
		return "", nil, err
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
		return "", nil, err
	}

	return keypair.Uuid, pem, err
}

func cmd_keypair(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("keypair", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "%s: need at list one parameter\n", flag.CommandLine.Name())
		return 1
	}

	cmd, subargs := flags.Arg(0), flags.Args()[1:]
	switch cmd {
	case "gen":
		defaultKey, err := local.GetDefaultKeypairID(ctx.Workdir)
		if defaultKey != "" {
			fmt.Fprintf(os.Stderr, "you already have a keypair: %s\n", defaultKey)
			return 1
		}

		uuid, encryptedKeypair, err := keypairGenerate()
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not generate keypair: %s\n", err)
			return 1
		}
		err = local.SetEncryptedKeypair(ctx.Workdir, uuid, encryptedKeypair)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not save keypair in local store: %s\n", err)
			return 1
		}

		if defaultKey == "" {
			local.SetDefaultKeypairID(ctx.Workdir, uuid)
		}

	case "info":
		keyUuid := ""
		if len(subargs) == 0 {
			tmp, err := local.GetDefaultKeypairID(ctx.Workdir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get default keypair\n", flag.CommandLine.Name())
				return 1
			}
			keyUuid = tmp
		} else {
			keyUuid = subargs[0]
		}

		encryptedKeypair, err := local.GetEncryptedKeypair(ctx.Workdir, keyUuid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get keypair\n", flag.CommandLine.Name())
			return 1
		}

		var keypair *encryption.Keypair
		for {
			passphrase, err := helpers.GetPassphrase()
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
		fmt.Println("Master:", skeypair.MasterKey)
		fmt.Println("Private:", skeypair.PrivateKey)
		fmt.Println("Public:", skeypair.PublicKey)

	default:
		fmt.Fprintf(os.Stderr, "%s: unknown subcommand: %s\n", flag.CommandLine.Name(), cmd)
		return 1
	}

	return 0
}
