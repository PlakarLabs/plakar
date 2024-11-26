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

package identity

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"net/mail"
	"os"
	"path/filepath"
	"time"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/encryption/keypair"
	"github.com/PlakarKorp/plakar/identity"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/google/uuid"
)

func init() {
	subcommands.Register("identity", cmd_identity)
}

func cmd_identity(ctx *context.Context, _ *repository.Repository, args []string) int {
	flags := flag.NewFlagSet("identity", flag.ExitOnError)
	flags.Usage = func() {
		fmt.Println("usage: plakar identity")
		flags.PrintDefaults()
	}
	flags.Parse(args)

	if flags.NArg() == 0 {
		flags.Usage()
		return 1
	}

	os.MkdirAll(ctx.GetKeyringDir(), 0700)

	subcommand := flags.Arg(0)

	switch subcommand {
	case "create":
		return identity_create(ctx, flags.Args()[1:])

	case "info":
		return identity_info(ctx, flags.Args()[1:])

	case "list", "ls":
		return identity_list(ctx)

	default:
		flags.Usage()
	}

	fmt.Println(ctx.GetKeyringDir())

	return 0
}

func identity_create(ctx *context.Context, args []string) int {
	reader := bufio.NewReader(os.Stdin)

	var email string
	attempts := 0
	maxAttempts := 3

	for attempts < maxAttempts {
		fmt.Print("Enter email address: ")
		input, _ := reader.ReadString('\n')
		email = input[:len(input)-1] // Trim the newline character

		// Validate email address
		_, err := mail.ParseAddress(email)
		if err != nil {
			fmt.Println("Invalid email address. Please try again.")
			attempts++
			if attempts == maxAttempts {
				fmt.Println("Too many failed attempts. Exiting.")
				os.Exit(1)
			}
		} else {
			break
		}
	}

	var passphrase []byte
	attempts = 0
	for attempts < maxAttempts {
		tmp, err := utils.GetPassphraseConfirm("identity")
		if err != nil {
			fmt.Println("Error reading passphrase. Please try again.")
			attempts++
			if attempts == maxAttempts {
				fmt.Println("Too many failed attempts. Exiting.")
				os.Exit(1)
			}
		} else {
			passphrase = tmp
			break
		}
	}

	kp, err := keypair.Generate()
	if err != nil {
		fmt.Println("Error generating keypair:", err)
		return 1
	}

	id, err := identity.New(email, *kp)
	if err != nil {
		fmt.Println("Error creating identity:", err)
		return 1
	}

	fp, err := os.Create(filepath.Join(ctx.GetKeyringDir(), id.Identifier.String()))
	if err != nil {
		fmt.Println("Error creating identity file:", err)
		return 1
	}
	defer fp.Close()

	data, err := id.Seal(passphrase)
	if err != nil {
		fmt.Println("Error sealing identity:", err)
		return 1
	}
	fp.Write(data)
	return 0
}

func identity_info(ctx *context.Context, args []string) int {
	var passphrase []byte
	attempts := 0
	maxAttempts := 3
	for attempts < maxAttempts {
		tmp, err := utils.GetPassphraseConfirm("identity")
		if err != nil {
			fmt.Println("Error reading passphrase. Please try again.")
			attempts++
			if attempts == maxAttempts {
				fmt.Println("Too many failed attempts. Exiting.")
				os.Exit(1)
			}
		} else {
			passphrase = tmp
			break
		}
	}

	data, err := os.ReadFile(filepath.Join(ctx.GetKeyringDir(), args[0]))
	if err != nil {
		fmt.Println("Error reading identity file:", err)
		return 1
	}

	id, err := identity.Unseal(data, passphrase)
	if err != nil {
		fmt.Println("Error unsealing identity:", err)
		return 1
	}

	fmt.Println("Identifier:", id.Identifier)
	return 0
}

func identity_list(ctx *context.Context) int {
	files, err := os.ReadDir(ctx.GetKeyringDir())
	if err != nil {
		fmt.Println("Error reading keyring directory:", err)
		return 1
	}

	for _, file := range files {
		si, err := identity.Load(ctx.GetKeyringDir(), uuid.MustParse(file.Name()))
		if err != nil {
			fmt.Println("Error loading identity:", err)
			return 1
		}
		fmt.Printf("%s keyId=%s public=%s address=%s\n",
			si.Timestamp.UTC().Format(time.RFC3339),
			si.Identifier.String(),
			base64.RawStdEncoding.EncodeToString(si.PublicKey),
			si.Address)
	}
	return 0
}
