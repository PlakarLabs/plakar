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
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/PlakarLabs/plakar/storage"
	"github.com/anmitsu/go-shlex"
)

func init() {
	registerCommand("shell", cmd_shell)
}

func cmd_shell(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("shell", flag.ExitOnError)
	flags.Parse(args)

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("%s> ", ctx.Repository)
		text, err := reader.ReadString('\n')
		if err != nil {
			return 1
		}
		text = strings.TrimSuffix(text, "\n")

		argv, err := shlex.Split(text, true)
		if err != nil {
			continue
		}

		if argv[0] == "exit" || argv[0] == "quit" || argv[0] == "q" {
			break
		}

		exitCode, _ := executeCommand(ctx, repository, argv[0], argv[1:])
		if exitCode == -1 {
			fmt.Fprintf(os.Stderr, "%s: unsupported command: %s", flag.CommandLine.Name(), argv[0])
		}
	}
	return 0
}
