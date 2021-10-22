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
	"os"
	"strings"

	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/snapshot"
)

func cmd_cat(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	flags.Parse(args)

	if len(flags.Args()) == 0 {
		logger.Error("%s: at least one parameter is required", flags.Name())
		return 1
	}

	snapshots := getSnapshotsList(ctx)

	mapSnapshots := make(map[string]*snapshot.Snapshot)

	errors := 0
	for i := 0; i < len(args); i++ {
		prefix, pattern := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			logger.Error("%s: no snapshot with prefix '%s'", flags.Name(), prefix)
			errors++
			continue
		} else if len(res) > 1 {
			logger.Error("%s: snapshot prefix is ambiguous: '%s' matches %d snapshots", flags.Name(), prefix, len(res))
			errors++
			continue
		}

		if pattern == "" {
			logger.Error("%s: missing filename for snapshot %s", flags.Name(), res[0])
			errors++
			continue
		}

		snap, ok := mapSnapshots[res[0]]
		if !ok {
			snap, err := snapshot.Load(ctx.Store(), res[0])
			if err != nil {
				logger.Error("%s: could not open snapshot: %s", flags.Name(), res[0])
				errors++
				continue
			}
			mapSnapshots[snap.Uuid] = snap
		}

		if !strings.HasPrefix(pattern, "/") && !strings.HasPrefix(pattern, ".") {
			objects := make([]string, 0)
			for id := range snap.Objects {
				objects = append(objects, id)
			}
			res = findObjectByPrefix(objects, pattern)
			if len(res) == 0 {
				logger.Error("%s: no object with prefix '%s'", flags.Name(), pattern)
				errors++
				continue
			} else if len(res) > 1 {
				logger.Error("%s: object prefix is ambiguous: '%s' matches %d objects", flags.Name(), prefix, len(res))
				errors++
				continue
			}
		}
	}
	if errors != 0 {
		return 1
	}

	errors = 0
	for i := 0; i < len(args); i++ {
		prefix, pattern := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)

		snapshot := mapSnapshots[res[0]]

		var checksum string
		if strings.HasPrefix(pattern, "/") || strings.HasPrefix(pattern, ".") {
			tmp, ok := snapshot.Pathnames[pattern]
			if !ok {
				fmt.Fprintf(os.Stderr, "%s: %s:%s: %s\n", flag.CommandLine.Name(), res[0], pattern, os.ErrNotExist)
				errors++
				continue
			}
			checksum = tmp
		} else {
			objects := make([]string, 0)
			for id := range snapshot.Objects {
				objects = append(objects, id)
			}
			res = findObjectByPrefix(objects, pattern)
			checksum = res[0]
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
