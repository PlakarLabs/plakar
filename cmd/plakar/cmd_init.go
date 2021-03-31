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
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/storage"
	"github.com/poolpOrg/plakar/storage/client"
	"github.com/poolpOrg/plakar/storage/fs"
	"golang.org/x/term"
)

func cmd_init(ctx plakar.Plakar, args []string) {
	var cleartext bool

	flags := flag.NewFlagSet("plakar init", flag.ExitOnError)
	flags.BoolVar(&cleartext, "cleartext", false, "disable transparent encryption")
	flags.Parse(args)

	storeConfig := storage.StoreConfig{}
	storeConfig.Uuid = uuid.NewString()
	if !cleartext {
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
			ctx.Keypair = keypair
			break
		}
		storeConfig.Encrypted = ctx.Keypair.Uuid
	}
	if len(flags.Args()) == 0 {
		err := initializeStore(ctx, storeloc, storeConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not initialize store: %s\n", storeloc, err)
			return
		}
		fmt.Printf("%s: store initialized\n", storeloc)
	} else {
		for _, storeLocation := range flags.Args() {
			err := initializeStore(ctx, storeLocation, storeConfig)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not initialize store: %s\n", storeloc, err)
				continue
			}
			fmt.Printf("%s: store initialized\n", storeLocation)
		}

	}
}

func initializeStore(ctx plakar.Plakar, storeLocation string, storeConfig storage.StoreConfig) error {
	var nstore storage.Store
	if strings.HasPrefix(storeLocation, "plakar://") {
		pstore := &client.ClientStore{}
		pstore.Ctx = &ctx
		pstore.Repository = storeLocation
		nstore = pstore

	} else {
		pstore := &fs.FSStore{}
		pstore.Ctx = &ctx
		pstore.Repository = storeLocation
		nstore = pstore
	}
	return nstore.Initialize(storeConfig)
}