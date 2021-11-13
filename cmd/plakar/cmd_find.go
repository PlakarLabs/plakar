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
	"sort"
	"strings"
	"time"

	"github.com/poolpOrg/plakar/snapshot"
)

func cmd_find(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("find", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() < 1 {
		log.Fatalf("%s: need at least a chunk prefix to search", flag.CommandLine.Name())
	}

	result := make(map[*snapshot.Snapshot][]string)
	snapshotsList, err := getSnapshotsList(ctx.Store())
	if err != nil {
		log.Fatal(err)
	}
	for _, snapshotUuid := range snapshotsList {
		snap, err := snapshot.Load(ctx.store, snapshotUuid)
		if err != nil {
			log.Fatal(err)
			return 1
		}

		result[snap] = make([]string, 0)

		for pathname := range snap.Pathnames {
			for _, arg := range flags.Args() {
				if pathname == arg {
					found := false
					for _, e := range result[snap] {
						if e == pathname {
							found = true
							break
						}
					}
					if !found {
						result[snap] = append(result[snap], pathname)
					}
				}
			}
		}

		for pathname := range snap.Files {
			fileinfo, _ := snap.GetInode(pathname)
			for _, arg := range flags.Args() {
				if fileinfo.Name == arg {
					found := false
					for _, e := range result[snap] {
						if e == pathname {
							found = true
							break
						}
					}
					if !found {
						result[snap] = append(result[snap], pathname)
					}
				}
			}
		}

		for _, chunk := range snap.Chunks {
			for _, arg := range flags.Args() {
				if strings.HasPrefix(chunk.Checksum, arg) {
					shortcutFound := false
					for pathname, objectChecksum := range snap.Pathnames {
						if objectChecksum == chunk.Checksum {
							found := false
							for _, e := range result[snap] {
								if e == pathname {
									found = true
									break
								}
							}
							if !found {
								result[snap] = append(result[snap], pathname)
							}
							shortcutFound = true
							break
						}
					}
					if !shortcutFound {
						for objectChecksum, object := range snap.Objects {
							for _, objectChunk := range object.Chunks {
								if objectChunk.Checksum == chunk.Checksum {
									for pathname := range snap.Pathnames {
										if snap.Pathnames[pathname] == objectChecksum {
											found := false
											for _, e := range result[snap] {
												if e == pathname {
													found = true
													break
												}
											}
											if !found {
												result[snap] = append(result[snap], pathname)
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		for _, object := range snap.Objects {
			for _, arg := range flags.Args() {
				if strings.HasPrefix(object.Checksum, arg) || strings.HasPrefix(object.ContentType, arg) {
					for pathname, objectChecksum := range snap.Pathnames {
						if objectChecksum == object.Checksum {
							found := false
							for _, e := range result[snap] {
								if e == pathname {
									found = true
									break
								}
							}
							if !found {
								result[snap] = append(result[snap], pathname)
							}
						}
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
		return snapshots[i].CreationTime.Before(snapshots[j].CreationTime)
	})

	for _, snap := range snapshots {
		files := result[snap]
		sort.Slice(files, func(i, j int) bool {
			return files[i] < files[j]
		})

		for _, pathname := range files {
			fmt.Printf("%s  %s %s\n", snap.CreationTime.UTC().Format(time.RFC3339), snap.Uuid, pathname)
		}
	}

	return 0
}
