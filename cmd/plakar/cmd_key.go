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
	"encoding/base64"
	"flag"
	"fmt"
	"os"

	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/local"
)

func init() {
	registerCommand("key", cmd_key)
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

func keypairDerive(keypair *encryption.Keypair) (string, []byte, error) {
	nkeypair, err := encryption.KeypairDerive(keypair)
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

	pem, err := nkeypair.Encrypt(passphrase)
	if err != nil {
		return "", nil, err
	}

	return nkeypair.Uuid, pem, err
}

func cmd_key(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("key", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "%s: need at list one parameter\n", flag.CommandLine.Name())
		return 1
	}

	cmd, subargs := flags.Arg(0), flags.Args()[1:]
	switch cmd {
	case "ls":
		keys, err := local.GetKeys(ctx.Workdir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get keys\n", flag.CommandLine.Name())
			return 1
		}
		for _, key := range keys {
			fmt.Println(key)
		}

	case "gen":
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

		defaultKey, err := local.GetDefaultKeypairID(ctx.Workdir)
		if defaultKey == "" {
			local.SetDefaultKeypairID(ctx.Workdir, uuid)
		}

	case "derive":
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
		uuid, encryptedKeypair, err := keypairDerive(keypair)

		err = local.SetEncryptedKeypair(ctx.Workdir, uuid, encryptedKeypair)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not save keypair in local store: %s\n", err)
			return 1
		}

		defaultKey, err := local.GetDefaultKeypairID(ctx.Workdir)
		if defaultKey == "" {
			local.SetDefaultKeypairID(ctx.Workdir, uuid)
		}

	case "export":
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

		keypair, err := local.GetEncryptedKeypair(ctx.Workdir, keyUuid)
		if err != nil {
			// not supposed to happen at this point
			fmt.Fprintf(os.Stderr, "%s: could not get keypair\n", flag.CommandLine.Name())
			return 1
		}
		fmt.Println(base64.StdEncoding.EncodeToString([]byte(keypair)))

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

	case "default":
		if len(subargs) == 0 {
			keyUuid, _ := local.GetDefaultKeypairID(ctx.Workdir)
			if keyUuid != "" {
				fmt.Println(keyUuid)
			}
		} else if len(subargs) == 1 {
			local.SetDefaultKeypairID(ctx.Workdir, subargs[0])
		}

	default:
		fmt.Fprintf(os.Stderr, "%s: unknown subcommand: %s\n", flag.CommandLine.Name(), cmd)
		return 1
	}

	return 0
}
