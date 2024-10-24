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

	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/snapshot/exporter"
)

func init() {
	registerCommand("pull", cmd_restore)
	registerCommand("restore", cmd_restore)
}

func cmd_restore(ctx *context.Context, repo *repository.Repository, args []string) int {
	var pullPath string
	var pullRebase bool
	var exporterInstance *exporter.Exporter

	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	flags := flag.NewFlagSet("restore`", flag.ExitOnError)
	flags.StringVar(&pullPath, "to", "", "base directory where pull will restore")
	flags.BoolVar(&pullRebase, "rebase", false, "strip pathname when pulling")
	flags.Parse(args)

	if pullPath == "" {
		exporterInstance, err = exporter.NewExporter(dir)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		exporterInstance, err = exporter.NewExporter(pullPath)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer exporterInstance.Close()

	if flags.NArg() == 0 {
		metadatas, err := getHeaders(repo, nil)
		if err != nil {
			log.Fatal(err)
		}

		for i := len(metadatas); i != 0; i-- {
			metadata := metadatas[i-1]
			for _, scannedDir := range metadata.ScannedDirectories {
				if dir == scannedDir || strings.HasPrefix(dir, fmt.Sprintf("%s/", scannedDir)) {
					snap, err := snapshot.Load(repo, metadata.GetIndexID())
					if err != nil {
						return 1
					}
					snap.Restore(exporterInstance, true, dir)
					return 0
				}
			}
		}
		log.Fatalf("%s: could not find a snapshot to restore this path from", flag.CommandLine.Name())
		return 1
	}

	snapshots, err := getSnapshots(repo, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for offset, snap := range snapshots {
		_, pattern := parseSnapshotID(flags.Args()[offset])
		snap.Restore(exporterInstance, pullRebase, pattern)
	}

	return 0
}
