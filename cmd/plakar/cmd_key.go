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

	"github.com/poolpOrg/plakar/local"
)

func cmd_key(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("key", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "%s: need at list one parameter\n", flag.CommandLine.Name())
		return 1
	}

	cmd, _ := flags.Arg(0), flags.Args()[1:]
	switch cmd {
	case "export":
		if ctx.Store().Configuration().Encrypted == "" {
			fmt.Fprintf(os.Stderr, "%s: plakar repository is not encrypted\n", flag.CommandLine.Name())
			return 1
		}

		keypair, err := local.GetEncryptedKeypair(ctx.Workdir)
		if err != nil {
			// not supposed to happen at this point
			fmt.Fprintf(os.Stderr, "%s: could not get keypair\n", flag.CommandLine.Name())
			return 1
		}
		fmt.Println(base64.StdEncoding.EncodeToString([]byte(keypair)))

	case "info":
		if ctx.Store().Configuration().Encrypted == "" {
			fmt.Fprintf(os.Stderr, "%s: plakar repository is not encrypted\n", flag.CommandLine.Name())
			return 1
		}

		skeypair, err := ctx.keypair.Serialize()
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
