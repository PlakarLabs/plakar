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

package create

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/PlakarLabs/plakar/cmd/plakar/subcommands"
	"github.com/PlakarLabs/plakar/cmd/plakar/utils"
	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	subcommands.Register("create", cmd_create)
}

func cmd_create(ctx *context.Context, _ *repository.Repository, args []string) int {
	var opt_noencryption bool
	var opt_nocompression bool
	var opt_hashing string
	var opt_compression string

	flags := flag.NewFlagSet("create", flag.ExitOnError)
	flags.BoolVar(&opt_noencryption, "no-encryption", false, "disable transparent encryption")
	flags.BoolVar(&opt_nocompression, "no-compression", false, "disable transparent compression")
	flags.StringVar(&opt_hashing, "hashing", "sha256", "swap the hashing function")
	flags.StringVar(&opt_compression, "compression", "lz4", "swap the compression function")
	flags.Parse(args)

	storageConfiguration := storage.NewConfiguration()
	if opt_nocompression {
		storageConfiguration.Compression = ""
	} else {
		storageConfiguration.Compression = opt_compression
	}
	storageConfiguration.Hashing = opt_hashing

	if !opt_noencryption {
		var passphrase []byte

		envPassphrase := os.Getenv("PLAKAR_PASSPHRASE")
		if ctx.GetKeyFromFile() == "" {
			if envPassphrase != "" {
				passphrase = []byte(envPassphrase)
			} else {
				for {
					tmp, err := utils.GetPassphraseConfirm("repository")
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s\n", err)
						continue
					}
					passphrase = tmp
					break
				}
			}
		} else {
			passphrase = []byte(ctx.GetKeyFromFile())
		}

		encryptionKey, err := encryption.BuildSecretFromPassphrase(passphrase)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}

		storageConfiguration.Encryption = encryption.DefaultAlgorithm()
		storageConfiguration.EncryptionKey = encryptionKey
	}

	switch flags.NArg() {
	case 0:
		repo, err := storage.Create(ctx, filepath.Join(ctx.GetHomeDir(), ".plakar"), *storageConfiguration)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}
		repo.Close()
	case 1:
		repo, err := storage.Create(ctx, flags.Arg(0), *storageConfiguration)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}
		repo.Close()
	default:
		fmt.Fprintf(os.Stderr, "%s: too many parameters\n", flag.CommandLine.Name())
		return 1
	}

	return 0
}
