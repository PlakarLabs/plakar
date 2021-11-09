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
)

func cmd_cat(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameter is required", flags.Name())
		return 1
	}

	snapshots, err := getSnapshots(ctx.Store(), flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	errors := 0
	for offset, snapshot := range snapshots {
		_, pathname := parseSnapshotID(flags.Args()[offset])

		if pathname == "" {
			logger.Error("%s: missing filename for snapshot %s", flags.Name(), snapshot.Uuid)
			errors++
			continue
		}

		checksum, ok := snapshot.Pathnames[pathname]
		if !ok {
			fmt.Fprintf(os.Stderr, "%s: %s:%s: %s\n", flag.CommandLine.Name(), snapshot.Uuid, pathname, os.ErrNotExist)
			errors++
			continue
		}

		object, err := snapshot.GetObject(checksum)
		if err != nil {
			logger.Error("%s: could not obtain object '%s'", flags.Name(), checksum)
			errors++
			continue
		}

		for _, chunk := range object.Chunks {
			data, err := snapshot.GetChunk(chunk.Checksum)
			if err != nil {
				logger.Error("%s: could not obtain chunk '%s'", flags.Name(), chunk.Checksum)
				errors++
				continue
			}
			os.Stdout.Write(data)
		}
	}

	if errors != 0 {
		return 1
	}
	return 0
}
