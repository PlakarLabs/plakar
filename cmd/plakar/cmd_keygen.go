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

	"github.com/poolpOrg/plakar/local"
)

func init() {
	registerCommand("keygen", cmd_keygen)
}

func cmd_keygen(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("keygen", flag.ExitOnError)
	flags.Parse(args)

	defaultKey, _ := local.GetDefaultKeypairID(ctx.Workdir)
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

	return 0
}
