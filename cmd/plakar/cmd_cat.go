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
	"os"

	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("cat", cmd_cat)
}

func cmd_cat(ctx Plakar, store *storage.Store, args []string) int {
	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameter is required", flags.Name())
		return 1
	}

	snapshots, err := getSnapshots(store, flags.Args())
	if err != nil {
		logger.Error("%s: could not obtain snapshots list: %s", flags.Name(), err)
		return 1
	}

	errors := 0
	for offset, snapshot := range snapshots {
		_, pathname := parseSnapshotID(flags.Args()[offset])

		if pathname == "" {
			logger.Error("%s: missing filename for snapshot %s", flags.Name(), snapshot.Metadata.Uuid)
			errors++
			continue
		}

		object := snapshot.LookupObjectForPathname(pathname)
		if object == nil {
			logger.Error("%s: could not open file '%s'", flags.Name(), pathname)
			errors++
			continue
		}

		for _, chunkChecksum := range object.Chunks {
			data, err := snapshot.GetChunk(chunkChecksum)
			if err != nil {
				logger.Error("%s: %s: could not obtain chunk '%s': %s", flags.Name(), pathname, chunkChecksum, err)
				errors++
				continue
			}
			_, err = os.Stdout.Write(data)
			if err != nil {
				logger.Error("%s: %s: could not write chunk '%s' to stdout: %s", flags.Name(), pathname, chunkChecksum, err)
				break
			}
		}
	}

	if errors != 0 {
		return 1
	}
	return 0
}
