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
	"log"
	"os"

	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/snapshot"
)

func cmd_push(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("push", flag.ExitOnError)
	flags.Parse(args)

	dir, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	snap, err := snapshot.New(ctx.Store())
	if err != nil {
		log.Fatal(err)
	}

	snap.CommandLine = ctx.CommandLine

	if flags.NArg() == 0 {
		snap.Push(dir)
	} else {
		for i := 0; i < flags.NArg(); i++ {
			snap.Push(flags.Args()[i])
		}
	}

	if len(snap.Directories) == 0 && len(snap.Files) == 0 {
		logger.Warn("empty snapshot, not committing")
		return 1
	}

	err = snap.Commit()
	if err != nil {
		os.Exit(1)
	}
	logger.Info("%s: OK", snap.Uuid)
	return 0
}
