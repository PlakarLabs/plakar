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
	"github.com/poolpOrg/plakar/local"
	"github.com/poolpOrg/plakar/storage"
	"golang.org/x/term"
)

func cmd_create(ctx Plakar, args []string) int {
	var no_encryption bool
	var no_compression bool

	flags := flag.NewFlagSet("create", flag.ExitOnError)
	flags.BoolVar(&no_encryption, "no-encryption", false, "disable transparent encryption")
	flags.BoolVar(&no_compression, "no-compression", false, "disable transparent compression")
	flags.Parse(args)

	/* load keypair from plakar */
	if !no_encryption {
		encryptedKeypair, err := local.GetEncryptedKeypair(ctx.Workdir)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "key not found, run `plakar keygen`\n")
				os.Exit(1)
			} else {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				os.Exit(1)
			}
		}
		ctx.EncryptedKeypair = encryptedKeypair
	}

	storeConfig := storage.StoreConfig{}
	storeConfig.Version = storage.VERSION
	storeConfig.Uuid = uuid.NewString()
	if no_compression {
		storeConfig.Compression = ""
	} else {
		storeConfig.Compression = "gzip"
	}
	if !no_encryption {
		for {
			var keypair *encryption.Keypair
			fmt.Fprintf(os.Stderr, "passphrase: ")
			passphrase, _ := term.ReadPassword(syscall.Stdin)
			keypair, err := encryption.Keyload(passphrase, ctx.EncryptedKeypair)
			if err != nil {
				fmt.Fprintf(os.Stderr, "\n")
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			fmt.Fprintf(os.Stderr, "\n")
			ctx.keypair = keypair
			break
		}
		storeConfig.Encryption = ctx.keypair.Uuid
	}
	if len(flags.Args()) == 0 {
		err := ctx.store.Create(ctx.Repository, storeConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not create store: %s\n", ctx.Repository, err)
			return 1
		}
	} else {
		for _, storeLocation := range flags.Args() {
			err := ctx.store.Create(storeLocation, storeConfig)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not create store: %s\n", ctx.Repository, err)
				continue
			}
		}

	}
	return 0
}
