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

	"github.com/dustin/go-humanize"
)

func cmd_info(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("info", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		log.Fatalf("%s: need at least one snapshot ID to pull", flag.CommandLine.Name())
	}

	snapshots, err := getSnapshots(ctx.Store(), flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for _, snapshot := range snapshots {
		fmt.Printf("Uuid: %s\n", snapshot.Uuid)
		fmt.Printf("CreationTime: %s\n", snapshot.CreationTime)
		fmt.Printf("Version: %s\n", snapshot.Version)
		fmt.Printf("Hostname: %s\n", snapshot.Hostname)
		fmt.Printf("Username: %s\n", snapshot.Username)
		fmt.Printf("CommandLine: %s\n", snapshot.CommandLine)
		fmt.Printf("Directories: %d\n", len(snapshot.Filesystem.Directories))
		fmt.Printf("Files: %d\n", len(snapshot.Filesystem.Files))
		fmt.Printf("NonRegular: %d\n", len(snapshot.Filesystem.NonRegular))
		fmt.Printf("Sums: %d\n", len(snapshot.Pathnames))
		fmt.Printf("Objects: %d\n", len(snapshot.Objects))
		fmt.Printf("Chunks: %d\n", len(snapshot.Chunks))
		fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(snapshot.Size), snapshot.Size)
	}

	return 0
}
