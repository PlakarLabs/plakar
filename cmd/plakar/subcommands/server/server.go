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

package server

import (
	"flag"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/server/httpd"
	"github.com/PlakarKorp/plakar/server/plakard"
)

func init() {
	subcommands.Register("server", cmd_server)
}

func cmd_server(ctx *context.Context, repo *repository.Repository, args []string) int {
	var opt_protocol string
	var opt_allowdelete bool

	flags := flag.NewFlagSet("server", flag.ExitOnError)
	flags.StringVar(&opt_protocol, "protocol", "plakar", "protocol to use (http or plakar)")
	flags.BoolVar(&opt_allowdelete, "allow-delete", false, "disable delete operations")
	flags.Parse(args)

	addr := ":9876"
	if flags.NArg() == 1 {
		addr = flags.Arg(0)
	}

	noDelete := true
	if opt_allowdelete {
		noDelete = false
	}

	switch opt_protocol {
	case "http":
		httpd.Server(repo, addr, noDelete)
	case "plakar":
		options := &plakard.ServerOptions{
			NoOpen:   true,
			NoCreate: true,
			NoDelete: noDelete,
		}
		plakard.Server(ctx, repo, addr, options)
	default:
		logging.Error("unsupported protocol: %s", opt_protocol)
	}
	return 0
}
