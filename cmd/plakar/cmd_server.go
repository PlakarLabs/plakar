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

	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("server", cmd_server)
}

func cmd_server(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("server", flag.ExitOnError)
	flags.Parse(args)

	addr := ":9876"
	if flags.NArg() == 1 {
		addr = flags.Arg(0)
	}

	network.Server(repository, addr)
	return 0
}
