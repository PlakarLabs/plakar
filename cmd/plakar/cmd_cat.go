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

	"github.com/poolpOrg/plakar/storage"
)

func cmd_cat(store storage.Store, args []string) int {
	flags := flag.NewFlagSet("plakar cat", flag.ExitOnError)
	flags.Parse(args)

	if len(flags.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "%s: need at list one parameter\n", flag.CommandLine.Name())
		return 1
	}

	snapshots, err := store.Snapshots()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not obtain list of snapshots\n", flag.CommandLine.Name())
		return 1
	}

	mapSnapshots := make(map[string]*storage.Snapshot)

	errors := 0
	for i := 0; i < len(args); i++ {
		prefix, pattern := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			fmt.Fprintf(os.Stderr, "%s: no snapshot with prefix: %s\n", flag.CommandLine.Name(), prefix)
			errors++
			continue
		} else if len(res) > 1 {
			fmt.Fprintf(os.Stderr, "%s: snapshot prefix is ambiguous: %s (matches %d snapshots)\n", flag.CommandLine.Name(), prefix, len(res))
			errors++
			continue
		}

		if pattern == "" {
			fmt.Fprintf(os.Stderr, "%s: missing filename\n", flag.CommandLine.Name())
			errors++
			continue
		}

		snapshot, ok := mapSnapshots[res[0]]
		if !ok {
			snapshot, err = store.Snapshot(res[0])
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not open snapshot: %s\n", flag.CommandLine.Name(), res[0])
				errors++
				continue
			}
			mapSnapshots[snapshot.Uuid] = snapshot
		}

		if !strings.HasPrefix(pattern, "/") {
			objects := make([]string, 0)
			for id := range snapshot.Objects {
				objects = append(objects, id)
			}
			res = findObjectByPrefix(objects, pattern)
			if len(res) == 0 {
				fmt.Fprintf(os.Stderr, "%s: no object with prefix: %s\n", flag.CommandLine.Name(), res[0])
				errors++
				continue
			} else if len(res) > 1 {
				fmt.Fprintf(os.Stderr, "%s: object prefix is ambiguous: %s (matches %d objects)\n", flag.CommandLine.Name(), prefix, len(res))
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
		if strings.HasPrefix(pattern, "/") {
			tmp, ok := snapshot.Sums[pattern]
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

		object, err := snapshot.ObjectGet(checksum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s:%s: could not obtain object %s\n", flag.CommandLine.Name(), snapshot.Uuid, pattern, checksum)
			errors++
			continue
		}

		for _, chunk := range object.Chunks {
			data, err := snapshot.ChunkGet(chunk.Checksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: %s:%s: could not obtain chunk %s\n", flag.CommandLine.Name(), snapshot.Uuid, pattern, chunk.Checksum)
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
