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
	"strings"

	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("pull", cmd_pull)
}

func cmd_pull(ctx Plakar, repository *storage.Repository, args []string) int {
	var pullPath string
	var pullRebase bool
	var opt_progress bool

	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	flags := flag.NewFlagSet("pull", flag.ExitOnError)
	flags.StringVar(&pullPath, "path", dir, "base directory where pull will restore")
	flags.BoolVar(&pullRebase, "rebase", false, "strip pathname when pulling")
	flags.BoolVar(&opt_progress, "progress", false, "display progress bar")
	flags.Parse(args)

	if flags.NArg() == 0 {
		metadatas, err := getMetadatas(repository, nil)
		if err != nil {
			log.Fatal(err)
		}

		for i := len(metadatas); i != 0; i-- {
			metadata := metadatas[i-1]
			for _, scannedDir := range metadata.ScannedDirectories {
				if dir == scannedDir || strings.HasPrefix(dir, fmt.Sprintf("%s/", scannedDir)) {
					snap, err := snapshot.Load(repository, metadata.GetIndexID())
					if err != nil {
						return 1
					}
					snap.Pull(pullPath, true, dir, opt_progress)
					return 0
				}
			}
		}
		log.Fatalf("%s: could not find a snapshot to restore this path from", flag.CommandLine.Name())
		return 1
	}

	snapshots, err := getSnapshots(repository, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for offset, snap := range snapshots {
		_, pattern := parseSnapshotID(flags.Args()[offset])
		snap.Pull(pullPath, pullRebase, pattern, opt_progress)
	}

	return 0
}
