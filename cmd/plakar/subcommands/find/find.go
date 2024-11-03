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

package find

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/PlakarLabs/plakar/cmd/plakar/subcommands"
	"github.com/PlakarLabs/plakar/cmd/plakar/utils"
	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot"
)

func init() {
	subcommands.Register("find", cmd_find)
}

func cmd_find(ctx *context.Context, repo *repository.Repository, args []string) int {
	flags := flag.NewFlagSet("find", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() < 1 {
		log.Fatalf("%s: need at least a chunk prefix to search", flag.CommandLine.Name())
	}

	result := make(map[*snapshot.Snapshot]map[string]bool)
	snapshotsList, err := utils.GetSnapshotsList(repo)
	if err != nil {
		log.Fatal(err)
	}
	for _, snapshotUuid := range snapshotsList {
		snap, err := snapshot.Load(repo, snapshotUuid)
		if err != nil {
			log.Fatal(err)
			return 1
		}

		fs, err := snap.Filesystem()
		if err != nil {
			log.Fatal(err)
			return 1
		}

		result[snap] = make(map[string]bool)

		for _, arg := range flags.Args() {
			// try finding a pathname to a directory of file
			if strings.Contains(arg, "/") {
				for pathname := range fs.Pathnames() {
					if pathname == arg {
						if exists := result[snap][pathname]; !exists {
							result[snap][pathname] = true
						}
					}
				}
			}

			// try finding a directory or file
			for name := range fs.Pathnames() {
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
		return snapshots[i].Header.CreationTime.Before(snapshots[j].Header.CreationTime)
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
			fmt.Printf("%s  %x %s\n", snap.Header.CreationTime.UTC().Format(time.RFC3339), snap.Header.GetIndexShortID(), pathname)
		}
	}

	return 0
}
