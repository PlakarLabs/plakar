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
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

func cmd_ls(ctx Plakar, args []string) int {
	var recursive bool

	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&recursive, "recursive", false, "recursive listing")
	flags.Parse(args)

	if len(flags.Args()) == 0 {
		list_snapshots(ctx.Store())
		return 0
	}

	if recursive {
		list_snapshot_recursive(ctx.Store(), flags.Args())
	} else {
		list_snapshot(ctx.Store(), flags.Args())
	}
	return 0
}

func list_snapshots(store storage.Store) {
	snapshots, err := snapshot.List(store)
	if err != nil {
		log.Fatalf("%s: could not fetch snapshots list", flag.CommandLine.Name())
	}

	snapshotsList := make([]*snapshot.Snapshot, 0)
	for _, Uuid := range snapshots {

		snapshot, err := snapshot.Load(store, Uuid)
		if err != nil {
			/* failed to lookup snapshot */
			continue
		}
		snapshotsList = append(snapshotsList, snapshot)
	}
	helpers.SnapshotsSortedByDate(snapshotsList)

	for _, snapshot := range snapshotsList {
		fmt.Fprintf(os.Stdout, "%s%38s%10s %s\n",
			snapshot.CreationTime.UTC().Format(time.RFC3339),
			snapshot.Uuid,
			humanize.Bytes(snapshot.Size),
			strings.Join(snapshot.Roots, ", "))
	}
}

func list_snapshot(store storage.Store, args []string) {
	snapshots, err := snapshot.List(store)
	if err != nil {
		log.Fatalf("%s: could not fetch snapshots list", flag.CommandLine.Name())
	}

	for i := 0; i < len(args); i++ {
		prefix, _ := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
	}

	for _, arg := range args {
		prefix, pattern := parseSnapshotID(arg)
		res := findSnapshotByPrefix(snapshots, prefix)
		snapshot, err := snapshot.Load(store, res[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
		}

		pattern = filepath.Clean(pattern)

		if pattern == "." || pattern == ".." {
			pattern = "/"
		}
		if !strings.HasPrefix(pattern, "/") {
			pattern = "/" + pattern
		}

		directories := make([]string, 0)
		for name := range snapshot.Directories {
			directories = append(directories, name)
		}
		sort.Slice(directories, func(i, j int) bool {
			return strings.Compare(directories[i], directories[j]) < 0
		})

		for _, name := range directories {
			fi := snapshot.Directories[name]
			if !helpers.PathIsWithin(name, pattern) && name != pattern {
				continue
			}
			if name == "/" || name == pattern {
				continue
			}

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
			fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
				//snapshot.Pathnames[name],
				fi.ModTime.UTC().Format(time.RFC3339),
				fi.Mode,
				username,
				groupname,
				humanize.Bytes(uint64(fi.Size)),
				fi.Name)
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
			if !helpers.PathIsWithin(name, pattern) && name != pattern {
				continue
			}

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
			fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
				//snapshot.Pathnames[name],
				fi.ModTime.UTC().Format(time.RFC3339),
				fi.Mode,
				username,
				groupname,
				humanize.Bytes(uint64(fi.Size)),
				fi.Name)
		}
	}
}

func list_snapshot_recursive(store storage.Store, args []string) {
	snapshots, err := snapshot.List(store)
	if err != nil {
		log.Fatalf("%s: could not fetch snapshots list", flag.CommandLine.Name())
	}

	for i := 0; i < len(args); i++ {
		prefix, _ := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
	}

	for _, arg := range args {
		prefix, pattern := parseSnapshotID(arg)
		res := findSnapshotByPrefix(snapshots, prefix)
		snapshot, err := snapshot.Load(store, res[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
		}

		pattern = filepath.Clean(pattern)

		if pattern == "." || pattern == ".." {
			pattern = "/"
		}
		if !strings.HasPrefix(pattern, "/") {
			pattern = "/" + pattern
		}

		directories := make([]string, 0)
		for name := range snapshot.Directories {
			directories = append(directories, name)
		}
		sort.Slice(directories, func(i, j int) bool {
			return strings.Compare(directories[i], directories[j]) < 0
		})

		for _, name := range directories {
			if !helpers.PathIsWithin(name, pattern) {
				continue
			}
			list_snapshot_recursive_directory(snapshot, name)
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
			if !helpers.PathIsWithin(name, pattern) && name != pattern {
				continue
			}

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
			fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
				//snapshot.Pathnames[name],
				fi.ModTime.UTC().Format(time.RFC3339),
				fi.Mode,
				username,
				groupname,
				humanize.Bytes(uint64(fi.Size)),
				name)
		}
	}
}

func list_snapshot_recursive_directory(snapshot *snapshot.Snapshot, directory string) {
	directories := make([]string, 0)
	for name := range snapshot.Directories {
		directories = append(directories, name)
	}
	sort.Slice(directories, func(i, j int) bool {
		return strings.Compare(directories[i], directories[j]) < 0
	})

	for _, name := range directories {
		fi := snapshot.Directories[name]
		if !helpers.PathIsWithin(name, directory) {
			continue
		}
		if name == "/" || name == directory {
			continue
		}

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
		fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
			//snapshot.Pathnames[name],
			fi.ModTime.UTC().Format(time.RFC3339),
			fi.Mode,
			username,
			groupname,
			humanize.Bytes(uint64(fi.Size)),
			name)
		list_snapshot_recursive_directory(snapshot, name)
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
		if !helpers.PathIsWithin(name, directory) && name != directory {
			continue
		}

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
		fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
			//snapshot.Pathnames[name],
			fi.ModTime.UTC().Format(time.RFC3339),
			fi.Mode,
			username,
			groupname,
			humanize.Bytes(uint64(fi.Size)),
			name)
	}
}
