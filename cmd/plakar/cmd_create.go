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

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/storage"
)

func cmd_create(ctx Plakar, args []string) int {
	var opt_noencryption bool
	var opt_nocompression bool

	flags := flag.NewFlagSet("init", flag.ExitOnError)
	flags.BoolVar(&opt_noencryption, "no-encryption", false, "disable transparent encryption")
	flags.BoolVar(&opt_nocompression, "no-compression", false, "disable transparent compression")
	flags.Parse(args)

	repositoryConfig := storage.RepositoryConfig{}
	repositoryConfig.Version = storage.VERSION
	repositoryConfig.RepositoryID = uuid.Must(uuid.NewRandom())
	if opt_nocompression {
		repositoryConfig.Compression = ""
	} else {
		repositoryConfig.Compression = "gzip"
	}

	if !opt_noencryption {
		var passphrase []byte
		if ctx.KeyFromFile == "" {
			for {
				tmp, err := helpers.GetPassphraseConfirm("repository")
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s\n", err)
					continue
				}
				passphrase = tmp
				break
			}
		} else {
			passphrase = []byte(ctx.KeyFromFile)
		}
		repositoryConfig.Encryption = encryption.BuildSecretFromPassphrase(passphrase)
	}

	switch flags.NArg() {
	case 0:
		repository, err := storage.Create(ctx.Repository, repositoryConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}
		repository.Close()
	case 1:
		repository, err := storage.Create(flags.Arg(0), repositoryConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}
		repository.Close()
	default:
		fmt.Fprintf(os.Stderr, "%s: too many parameters\n", ctx.Repository)
		return 1
	}

	return 0
}
