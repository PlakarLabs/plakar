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
	"log"

	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("fork", cmd_fork)
}

func cmd_fork(ctx Plakar, repository *storage.Repository, args []string) int {

	flags := flag.NewFlagSet("fork", flag.ExitOnError)
	flags.Parse(args)

	if len(args) != 1 {
		log.Fatal("need a snapshot ID to fork")
		return info_plakar(repository)
	}

	snapshots, err := getSnapshots(repository, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for _, snap := range snapshots {
		nsnap, err := snapshot.Fork(repository, snap.Metadata.IndexID)
		if err != nil {
			log.Fatal(err)
		}
		if err := nsnap.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	return 0
}
