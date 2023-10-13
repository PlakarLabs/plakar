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
	"strconv"
	"sync"

	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("keep", cmd_keep)
}

func cmd_keep(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("keep", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		log.Fatalf("%s: need a number of snapshots to keep", flag.CommandLine.Name())
	}

	count, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatalf("%s: %s: need a number of snapshots to keep", flag.CommandLine.Name(), args[0])
	}

	snapshotsList, err := getSnapshotsList(repository)
	if err != nil {
		log.Fatal(err)
	}
	if len(snapshotsList) < count {
		return 0
	}

	snapshots, err := getSnapshots(repository, nil)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}
	snapshots = sortSnapshotsByDate(snapshots)[:len(snapshots)-count]
	for _, snap := range snapshots {
		wg.Add(1)
		go func(snap *snapshot.Snapshot) {
			repository.Purge(snap.Metadata.GetIndexID())
			wg.Done()
		}(snap)
	}
	wg.Wait()

	return 0
}
