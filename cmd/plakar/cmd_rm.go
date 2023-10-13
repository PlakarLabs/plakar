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
	"sync"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("rm", cmd_rm)
}

func cmd_rm(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("rm", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		log.Fatalf("%s: need at least one snapshot ID to rm", flag.CommandLine.Name())
	}

	snapshots, err := getSnapshots(repository, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	errors := 0
	wg := sync.WaitGroup{}
	for _, snap := range snapshots {
		wg.Add(1)
		go func(snap *snapshot.Snapshot) {
			err := repository.Purge(snap.Metadata.GetIndexID())
			if err != nil {
				logger.Error("%s", err)
				errors++
			}
			wg.Done()
		}(snap)
	}
	wg.Wait()

	if errors != 0 {
		return 1
	}
	return 0
}
