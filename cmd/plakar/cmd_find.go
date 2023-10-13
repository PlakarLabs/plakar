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
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("find", cmd_find)
}

func cmd_find(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("find", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() < 1 {
		log.Fatalf("%s: need at least a chunk prefix to search", flag.CommandLine.Name())
	}

	result := make(map[*snapshot.Snapshot]map[string]bool)
	snapshotsList, err := getSnapshotsList(repository)
	if err != nil {
		log.Fatal(err)
	}
	for _, snapshotUuid := range snapshotsList {
		snap, err := snapshot.Load(repository, snapshotUuid)
		if err != nil {
			log.Fatal(err)
			return 1
		}

		result[snap] = make(map[string]bool)

		for _, arg := range flags.Args() {
			// try finding a pathname to a directory of file
			if strings.Contains(arg, "/") {
				for _, pathname := range snap.Filesystem.ListStat() {
					if pathname == arg {
						if exists := result[snap][pathname]; !exists {
							result[snap][pathname] = true
						}
					}
				}
			}

			// try finding a directory or file
			for _, name := range snap.Filesystem.ListStat() {
				if filepath.Base(name) == arg {
					if exists := result[snap][arg]; !exists {
						result[snap][name] = true
					}
				}
			}

		}
	}

	snapshots := make([]*snapshot.Snapshot, 0)
	for snap := range result {
		snapshots = append(snapshots, snap)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Metadata.CreationTime.Before(snapshots[j].Metadata.CreationTime)
	})

	for _, snap := range snapshots {
		files := make([]string, 0)
		for file := range result[snap] {
			files = append(files, file)
		}

		sort.Slice(files, func(i, j int) bool {
			return files[i] < files[j]
		})

		for _, pathname := range files {
			fmt.Printf("%s  %s %s\n", snap.Metadata.CreationTime.UTC().Format(time.RFC3339), snap.Metadata.GetIndexShortID(), pathname)
		}
	}

	return 0
}
