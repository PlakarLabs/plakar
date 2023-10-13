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
	"io"

	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("checksum", cmd_checksum)
}

func cmd_checksum(ctx Plakar, repository *storage.Repository, args []string) int {
	var enableFastChecksum bool

	flags := flag.NewFlagSet("checksum", flag.ExitOnError)
	flags.BoolVar(&enableFastChecksum, "fast", false, "enable fast checksum (return recorded checksum)")

	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameter is required", flags.Name())
		return 1
	}

	snapshots, err := getSnapshots(repository, flags.Args())
	if err != nil {
		logger.Error("%s: could not obtain snapshots list: %s", flags.Name(), err)
		return 1
	}

	errors := 0
	for offset, snapshot := range snapshots {
		_, pathname := parseSnapshotID(flags.Args()[offset])

		if pathname == "" {
			logger.Error("%s: missing filename for snapshot %s", flags.Name(), snapshot.Metadata.GetIndexShortID())
			errors++
			continue
		}

		pathnameID := snapshot.Filesystem.GetPathnameID(pathname)
		object := snapshot.Index.LookupObjectForPathname(pathnameID)
		if object == nil {
			logger.Error("%s: could not open file '%s'", flags.Name(), pathname)
			errors++
			continue
		}

		if enableFastChecksum {
			fmt.Printf("%064x %s\n", object.Checksum, pathname)
		} else {
			rd, err := repository.NewReader(snapshot.Index, pathname)
			if err != nil {
				logger.Error("%s: %s: %s", flags.Name(), pathname, err)
				errors++
				continue
			}

			hasher := encryption.GetHasher(repository.Configuration().Hashing)
			if _, err := io.Copy(hasher, rd); err != nil {
				logger.Error("%s: %s: %s", flags.Name(), pathname, err)
				errors++
				continue
			}
			fmt.Printf("%064x %s\n", hasher.Sum(nil), pathname)
		}
	}

	return 0
}
