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
	"os/user"
	"sort"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/snapshot"
)

func cmd_find(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("plakar find", flag.ExitOnError)
	flags.Parse(args)

	if len(flags.Args()) == 0 {
		fmt.Fprintf(os.Stderr, "%s: need at list one parameter\n", flag.CommandLine.Name())
		return 1
	}

	snapshots := getSnapshotsList(ctx)

	snapshotsList := make([]*snapshot.Snapshot, 0)
	for _, Uuid := range snapshots {
		snap, err := snapshot.Load(ctx.Store(), Uuid)
		if err != nil {
			/* failed to lookup snapshot */
			continue
		}
		snapshotsList = append(snapshotsList, snap)
	}
	helpers.SnapshotsSortedByDate(snapshotsList)

	for _, snapshot := range snapshotsList {

		directories := make([]string, 0)
		for name := range snapshot.Directories {
			directories = append(directories, name)
		}
		sort.Slice(directories, func(i, j int) bool {
			return strings.Compare(directories[i], directories[j]) < 0
		})

		for _, name := range directories {
			fi := snapshot.Directories[name]
			if fi.Name == flag.Arg(1) {
				pwUserLookup, err := user.LookupId(fmt.Sprintf("%d", fi.Uid))
				username := fmt.Sprintf("%d", fi.Uid)
				if err == nil {
					username = pwUserLookup.Username
				}

				grGroupLookup, err := user.LookupGroupId(fmt.Sprintf("%d", fi.Gid))
				groupname := fmt.Sprintf("%d", fi.Gid)
				if err == nil {
					groupname = grGroupLookup.Name
				}
				fmt.Fprintf(os.Stdout, "%s: %s %s % 8s % 8s % 8s %s\n",
					snapshot.Uuid,
					snapshot.Pathnames[name],
					fi.Mode,
					username,
					groupname,
					humanize.Bytes(uint64(fi.Size)),
					name)
			}
		}

		filenames := make([]string, 0)
		for name := range snapshot.Files {
			filenames = append(filenames, name)
		}
		sort.Slice(filenames, func(i, j int) bool {
			return strings.Compare(filenames[i], filenames[j]) < 0
		})

		for _, name := range filenames {
			fi := snapshot.Files[name]
			if fi.Name == flag.Arg(1) {
				pwUserLookup, err := user.LookupId(fmt.Sprintf("%d", fi.Uid))
				username := fmt.Sprintf("%d", fi.Uid)
				if err == nil {
					username = pwUserLookup.Username
				}

				grGroupLookup, err := user.LookupGroupId(fmt.Sprintf("%d", fi.Gid))
				groupname := fmt.Sprintf("%d", fi.Gid)
				if err == nil {
					groupname = grGroupLookup.Name
				}
				fmt.Fprintf(os.Stdout, "%s: %s %s % 8s % 8s % 8s %s\n",
					snapshot.Uuid,
					snapshot.Pathnames[name],
					fi.Mode,
					username,
					groupname,
					humanize.Bytes(uint64(fi.Size)),
					name)
			}
		}
	}

	return 0
}
