//go:build go1.16
// +build go1.16

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

	"github.com/PlakarLabs/plakar/storage"
	v1 "github.com/PlakarLabs/plakar/ui/v1"
	v2 "github.com/PlakarLabs/plakar/ui/v2"
)

func init() {
	registerCommand("ui", cmd_ui)
}

func cmd_ui(ctx Plakar, repository *storage.Repository, args []string) int {
	var opt_nospawn bool
	var opt_addr string
	var opt_v2 bool

	flags := flag.NewFlagSet("ui", flag.ExitOnError)
	flags.BoolVar(&opt_nospawn, "no-spawn", false, "don't spawn browser")
	flags.StringVar(&opt_addr, "addr", "", "address to listen on")
	flags.BoolVar(&opt_v2, "v2", false, "use v2 UI")
	flags.Parse(args)

	if opt_v2 {
		err := v2.Ui(repository, opt_addr, !opt_nospawn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
			return 1
		}
	}

	err := v1.Ui(repository, opt_addr, !opt_nospawn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
		return 1
	}

	return 0
}
