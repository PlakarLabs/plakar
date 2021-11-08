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

	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/snapshot"
)

func cmd_keep(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("keep", flag.ExitOnError)
	flags.Parse(args)

	if len(args) == 0 {
		log.Fatalf("%s: need a number of snapshots to keep", flag.CommandLine.Name())
	}

	count, err := strconv.Atoi(args[0])
	if err != nil {
		log.Fatalf("%s: %s: need a number of snapshots to keep", flag.CommandLine.Name(), args[0])
	}

	snapshotsList := getSnapshotsList(ctx)
	if len(snapshotsList) < count {
		return 0
	}

	snapshots := make([]*snapshot.Snapshot, 0)
	for _, snapshotUuid := range snapshotsList {
		snap, err := snapshot.Load(ctx.Store(), snapshotUuid)
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), snapshotUuid)
		}
		snapshots = append(snapshots, snap)
	}

	snapshots = helpers.SnapshotsSortedByDate(snapshots)[:len(snapshots)-count]
	for _, snap := range snapshots {
		ctx.Store().Purge(snap.Uuid)
	}

	return 0
}