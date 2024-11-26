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

package ui

import (
	"flag"
	"fmt"
	"os"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
	v2 "github.com/PlakarKorp/plakar/ui/v2"
	"github.com/google/uuid"
)

func init() {
	subcommands.Register("ui", cmd_ui)
}

func cmd_ui(ctx *context.Context, repo *repository.Repository, args []string) int {
	var opt_addr string
	var opt_cors bool
	var opt_noauth bool
	var opt_nospawn bool

	flags := flag.NewFlagSet("ui", flag.ExitOnError)
	flags.StringVar(&opt_addr, "addr", "", "address to listen on")
	flags.BoolVar(&opt_cors, "cors", false, "enable CORS")
	flags.BoolVar(&opt_noauth, "no-auth", false, "don't use authentication")
	flags.BoolVar(&opt_nospawn, "no-spawn", false, "don't spawn browser")
	flags.Parse(args)

	ui_opts := v2.UiOptions{
		NoSpawn: opt_nospawn,
		Cors:    opt_cors,
		Token:   "",
	}

	if !opt_noauth {
		ui_opts.Token = uuid.NewString()
	}

	err := v2.Ui(repo, opt_addr, &ui_opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), flags.Name(), err)
		return 1
	}

	return 0
}
