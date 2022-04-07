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
	"syscall"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/storage"
	"golang.org/x/term"
)

func createStore(repository string, storeConfig storage.StoreConfig) error {
	store, err := FindStoreBackend(repository)
	if err != nil {
		return err
	}
	err = store.Create(repository, storeConfig)
	if err != nil {
		return err
	}
	return nil
}

func cmd_create(ctx Plakar, args []string) int {
	var opt_noencryption bool
	var opt_nocompression bool

	flags := flag.NewFlagSet("init", flag.ExitOnError)
	flags.BoolVar(&opt_noencryption, "no-encryption", false, "disable transparent encryption")
	flags.BoolVar(&opt_nocompression, "no-compression", false, "disable transparent compression")
	flags.Parse(args)

	storeConfig := storage.StoreConfig{}
	storeConfig.Version = storage.VERSION
	storeConfig.Uuid = uuid.NewString()
	if opt_nocompression {
		storeConfig.Compression = ""
	} else {
		storeConfig.Compression = "gzip"
	}

	/* load keypair from plakar */
	var keypair *encryption.Keypair
	var secret *encryption.Secret
	if !opt_noencryption {
		encryptedKeypair, err := ctx.Workdir.GetEncryptedKeypair()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: could not load keypair: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}

		for {
			fmt.Fprintf(os.Stderr, "keypair passphrase: ")
			passphrase, _ := term.ReadPassword(syscall.Stdin)
			keypair, err = encryption.KeypairLoad(passphrase, encryptedKeypair)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\n")
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			fmt.Fprintf(os.Stderr, "\n")
			break
		}

		secret, err = encryption.SecretGenerate()
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not generate key for repository\n")
			os.Exit(1)
		}
		storeConfig.Encryption = secret.Uuid
	}

	switch flags.NArg() {
	case 0:
		err := createStore(ctx.Repository, storeConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}
	case 1:
		err := createStore(flags.Arg(0), storeConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}
	default:
		fmt.Fprintf(os.Stderr, "%s: too many paramters\n", ctx.Repository)
		return 1
	}

	if !opt_noencryption {
		encrypted, err := secret.Encrypt(keypair.Key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not encrypt key for repository\n")
			os.Exit(1)
		}

		err = ctx.Workdir.SaveEncryptedSecret(secret.Uuid, encrypted)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not save master key for repository: %s\n", err)
			os.Exit(1)
		}
	}

	return 0
}
