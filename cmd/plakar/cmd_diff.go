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
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/poolpOrg/plakar/filesystem"
	"github.com/poolpOrg/plakar/snapshot"
)

func init() {
	registerCommand("diff", cmd_diff)
}

func cmd_diff(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("diff", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() < 2 {
		log.Fatalf("%s: needs two snapshot ID and/or snapshot files to cat", flag.CommandLine.Name())
	}

	snapshots, err := getSnapshotsList(ctx.Store())
	if err != nil {
		log.Fatal(err)
	}
	checkSnapshotsArgs(snapshots)

	if len(flags.Args()) == 2 {
		// check if snapshot id's both reference a file
		// if not, stat diff of snapshots, else diff files
		rel1 := strings.Contains(args[0], ":")
		rel2 := strings.Contains(args[1], ":")
		if (rel1 && !rel2) || (!rel1 && rel2) {
			log.Fatalf("%s: snapshot subset delimiter must be used on both snapshots", flag.CommandLine.Name())
		}

		if !rel1 {
			// stat diff
			prefix1, _ := parseSnapshotID(args[0])
			prefix2, _ := parseSnapshotID(args[1])
			res1 := findSnapshotByPrefix(snapshots, prefix1)
			res2 := findSnapshotByPrefix(snapshots, prefix2)
			snapshot1, err := snapshot.Load(ctx.Store(), res1[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res1[0])
			}
			snapshot2, err := snapshot.Load(ctx.Store(), res2[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res2[0])
			}
			for _, dir1 := range snapshot1.Filesystem.ListDirectories() {
				fi1, _ := snapshot1.LookupInodeForDirectory(dir1)
				fi2, ok := snapshot2.LookupInodeForDirectory(dir1)
				if !ok {
					fmt.Println("- ", fiToDiff(*fi1), dir1)
					continue
				}
				if *fi1 != *fi2 {
					fmt.Println("- ", fiToDiff(*fi1), dir1)
					fmt.Println("+ ", fiToDiff(*fi2), dir1)
				}
			}

			for _, dir2 := range snapshot2.Filesystem.ListDirectories() {
				fi2, _ := snapshot2.LookupInodeForDirectory(dir2)
				_, ok := snapshot1.LookupInodeForDirectory(dir2)
				if !ok {
					fmt.Println("+ ", fiToDiff(*fi2), dir2)
				}
			}

			for _, file1 := range snapshot1.Filesystem.ListFiles() {
				fi1, _ := snapshot1.LookupInodeForPathname(file1)
				fi2, ok := snapshot2.LookupInodeForPathname(file1)
				if !ok {
					fmt.Println("- ", fiToDiff(*fi1), file1)
					continue
				}
				if *fi1 != *fi2 {
					fmt.Println("- ", fiToDiff(*fi1), file1)
					fmt.Println("+ ", fiToDiff(*fi2), file1)
				}
			}

			for _, file2 := range snapshot2.Filesystem.ListFiles() {
				fi2, _ := snapshot2.LookupInodeForPathname(file2)
				_, ok := snapshot1.LookupInodeForFilename(file2)
				if !ok {
					fmt.Println("+ ", fiToDiff(*fi2), file2)
				}
			}
		} else {
			// file diff
			prefix1, file1 := parseSnapshotID(args[0])
			prefix2, file2 := parseSnapshotID(args[1])
			res1 := findSnapshotByPrefix(snapshots, prefix1)
			res2 := findSnapshotByPrefix(snapshots, prefix2)
			snapshot1, err := snapshot.Load(ctx.Store(), res1[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res1[0])
			}
			snapshot2, err := snapshot.Load(ctx.Store(), res2[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res2[0])
			}
			diff_files(snapshot1, snapshot2, file1, file2)
		}

	} else {
		if strings.Contains(args[0], ":") || strings.Contains(args[1], ":") {
			log.Fatalf("%s: snapshot subset delimiter not allowed in snapshot ID when diffing common files", flag.CommandLine.Name())
		}

		prefix1, _ := parseSnapshotID(args[0])
		prefix2, _ := parseSnapshotID(args[1])
		res1 := findSnapshotByPrefix(snapshots, prefix1)
		res2 := findSnapshotByPrefix(snapshots, prefix2)
		snapshot1, err := snapshot.Load(ctx.Store(), res1[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res1[0])
		}
		snapshot2, err := snapshot.Load(ctx.Store(), res2[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res2[0])
		}
		for i := 2; i < len(args); i++ {
			_, ok1 := snapshot1.Pathnames[args[i]]
			_, ok2 := snapshot2.Pathnames[args[i]]
			if !ok1 && !ok2 {
				fmt.Fprintf(os.Stderr, "%s: %s: file not found in snapshots\n", flag.CommandLine.Name(), args[i])
			}

			diff_files(snapshot1, snapshot2, args[i], args[i])
		}
	}
	return 0
}

func fiToDiff(fi filesystem.Fileinfo) string {
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

	return fmt.Sprintf("%s % 8s % 8s % 8s %s",
		fi.Mode,
		username,
		groupname,
		humanize.Bytes(uint64(fi.Size)),
		fi.ModTime.UTC())
}

func diff_files(snapshot1 *snapshot.Snapshot, snapshot2 *snapshot.Snapshot, filename1 string, filename2 string) {
	sum1, ok1 := snapshot1.Pathnames[filename1]
	sum2, ok2 := snapshot2.Pathnames[filename2]

	// file does not exist in either snapshot
	if !ok1 && !ok2 {
		return
	}

	if sum1 == sum2 {
		fmt.Printf("%s:%s and %s:%s are identical\n",
			snapshot1.Uuid, filename1, snapshot2.Uuid, filename2)
		return
	}

	buf1 := ""
	buf2 := ""

	// file exists in snapshot1, grab a copy
	if ok1 {
		object, err := snapshot1.GetObject(sum1)
		if err != nil {
		}
		for _, chunkChecksum := range object.Chunks {
			data, err := snapshot2.GetChunk(chunkChecksum)
			if err != nil {
			}
			buf1 = buf1 + string(data)
		}
	}

	if ok2 {
		object, err := snapshot2.GetObject(sum2)
		if err != nil {
		}
		for _, chunkChecksum := range object.Chunks {
			data, err := snapshot2.GetChunk(chunkChecksum)
			if err != nil {
			}
			buf2 = buf2 + string(data)
		}
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(buf1)),
		B:        difflib.SplitLines(string(buf2)),
		FromFile: snapshot1.Uuid + ":" + filename1,
		ToFile:   snapshot2.Uuid + ":" + filename2,
		Context:  3,
	}
	text, _ := difflib.GetUnifiedDiffString(diff)
	fmt.Printf("%s", text)
}
