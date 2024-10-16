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
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/PlakarLabs/plakar/helpers"
	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
	"github.com/dustin/go-humanize"
)

func init() {
	registerCommand("ls", cmd_ls)
}

func cmd_ls(ctx Plakar, repo *repository.Repository, args []string) int {
	var opt_recursive bool
	var opt_tag string
	var opt_uuid bool

	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	flags.BoolVar(&opt_uuid, "uuid", false, "display uuid instead of short ID")
	flags.StringVar(&opt_tag, "tag", "", "filter by tag")
	flags.BoolVar(&opt_recursive, "recursive", false, "recursive listing")
	flags.Parse(args)

	if flags.NArg() == 0 {
		list_snapshots(repo, opt_uuid, opt_tag)
		return 0
	}

	if opt_recursive {
		list_snapshot_recursive(repo, flags.Args())
	} else {
		list_snapshot(repo, flags.Args())
	}
	return 0
}

func list_snapshots(repo *repository.Repository, useUuid bool, tag string) {
	metadatas, err := getHeaders(repo, nil)
	if err != nil {
		log.Fatalf("%s: could not fetch snapshots list", flag.CommandLine.Name())
	}

	for _, metadata := range metadatas {
		if tag != "" {
			found := false
			for _, t := range metadata.Tags {
				if tag == t {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		if !useUuid {
			fmt.Fprintf(os.Stdout, "%s%10s%10s%10s %s\n",
				metadata.CreationTime.UTC().Format(time.RFC3339),
				metadata.GetIndexShortID(),
				humanize.Bytes(metadata.ScanProcessedSize),
				metadata.CreationDuration.Round(time.Second),
				strings.Join(metadata.ScannedDirectories, ", "))
		} else {
			fmt.Fprintf(os.Stdout, "%s%38s%10s%10s %s\n",
				metadata.CreationTime.UTC().Format(time.RFC3339),
				metadata.GetIndexID(),
				humanize.Bytes(metadata.ScanProcessedSize),
				metadata.CreationDuration.Round(time.Second),
				strings.Join(metadata.ScannedDirectories, ", "))
		}
	}
}

func list_snapshot(repo *repository.Repository, args []string) {
	vfss, err := getFilesystems(repo, args)
	if err != nil {
		log.Fatalf("%s: could not fetch vfs list: %s", flag.CommandLine.Name(), err)
	}

	for offset, pvfs := range vfss {
		_, prefix := parseSnapshotID(args[offset])

		prefix = path.Clean(prefix)
		info, _ := pvfs.Stat(prefix)

		content := make([]string, 0)
		children := make(map[string]*objects.FileInfo)

		if info.IsDir() {
			entries := make([]string, 0)
			iter, err := pvfs.Children(prefix)
			if err != nil {
				continue
			}
			for pathname := range iter {
				entries = append(entries, pathname)
			}
			if len(entries) == 0 {
				info, err := pvfs.Stat(prefix)
				if err == nil {
					continue
				}
				children[prefix] = info
				content = append(content, prefix)
			} else {
				for _, name := range entries {
					children[name], _ = pvfs.Stat(path.Clean(fmt.Sprintf("%s/%s", prefix, name)))
					content = append(content, name)
				}
				sort.Slice(content, func(i, j int) bool {
					return strings.Compare(content[i], content[j]) < 0
				})
			}
		} else {
			children[prefix] = info
			content = append(content, prefix)
		}

		for _, item := range content {

			fi := children[item]

			pwUserLookup, err := user.LookupId(fmt.Sprintf("%d", fi.Uid()))
			username := fmt.Sprintf("%d", fi.Uid())
			if err == nil {
				username = pwUserLookup.Username
			}

			grGroupLookup, err := user.LookupGroupId(fmt.Sprintf("%d", fi.Gid()))
			groupname := fmt.Sprintf("%d", fi.Gid())
			if err == nil {
				groupname = grGroupLookup.Name
			}
			fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
				fi.ModTime().UTC().Format(time.RFC3339),
				fi.Mode(),
				username,
				groupname,
				humanize.Bytes(uint64(fi.Size())),
				fi.Name())
		}
	}
}

func list_snapshot_recursive(repo *repository.Repository, args []string) {
	vfss, err := getFilesystems(repo, args)
	if err != nil {
		log.Fatalf("%s: could not fetch vfs list: %s", flag.CommandLine.Name(), err)
	}

	for offset, pvfs := range vfss {
		_, prefix := parseSnapshotID(args[offset])

		prefix = filepath.Clean(prefix)

		if prefix == "." || prefix == ".." {
			prefix = "/"
		}
		if !strings.HasPrefix(prefix, "/") {
			prefix = "/" + prefix
		}

		directories := make([]string, 0)
		for name := range pvfs.Directories() {
			directories = append(directories, name)
		}
		sort.Slice(directories, func(i, j int) bool {
			return strings.Compare(directories[i], directories[j]) < 0
		})

		for _, name := range directories {
			if !helpers.PathIsWithin(name, prefix) {
				continue
			}
			list_snapshot_recursive_directory(pvfs, name)
		}

		filenames := make([]string, 0)
		for filename := range pvfs.Files() {
			filenames = append(filenames, filename)
		}
		sort.Slice(filenames, func(i, j int) bool {
			return strings.Compare(filenames[i], filenames[j]) < 0
		})

		for _, name := range filenames {
			fi, _ := pvfs.Stat(name)
			if !helpers.PathIsWithin(name, prefix) && name != prefix {
				continue
			}

			pwUserLookup, err := user.LookupId(fmt.Sprintf("%d", fi.Uid()))
			username := fmt.Sprintf("%d", fi.Uid())
			if err == nil {
				username = pwUserLookup.Username
			}

			grGroupLookup, err := user.LookupGroupId(fmt.Sprintf("%d", fi.Gid()))
			groupname := fmt.Sprintf("%d", fi.Gid())
			if err == nil {
				groupname = grGroupLookup.Name
			}
			fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
				fi.ModTime().UTC().Format(time.RFC3339),
				fi.Mode(),
				username,
				groupname,
				humanize.Bytes(uint64(fi.Size())),
				name)
		}
	}
}

func list_snapshot_recursive_directory(pvfs *vfs.Filesystem, directory string) {
	directories := make([]string, 0)
	for name := range pvfs.Directories() {
		directories = append(directories, name)
	}

	sort.Slice(directories, func(i, j int) bool {
		return strings.Compare(directories[i], directories[j]) < 0
	})

	for _, name := range directories {
		fi, _ := pvfs.Stat(name)
		if !helpers.PathIsWithin(name, directory) {
			continue
		}
		if name == "/" || name == directory {
			continue
		}

		pwUserLookup, err := user.LookupId(fmt.Sprintf("%d", fi.Uid()))
		username := fmt.Sprintf("%d", fi.Uid())
		if err == nil {
			username = pwUserLookup.Username
		}

		grGroupLookup, err := user.LookupGroupId(fmt.Sprintf("%d", fi.Gid()))
		groupname := fmt.Sprintf("%d", fi.Gid())
		if err == nil {
			groupname = grGroupLookup.Name
		}
		fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
			fi.ModTime().UTC().Format(time.RFC3339),
			fi.Mode(),
			username,
			groupname,
			humanize.Bytes(uint64(fi.Size())),
			name)
		list_snapshot_recursive_directory(pvfs, name)
	}

	filenames := make([]string, 0)
	for filename := range pvfs.Files() {
		filenames = append(filenames, filename)
	}
	sort.Slice(filenames, func(i, j int) bool {
		return strings.Compare(filenames[i], filenames[j]) < 0
	})

	for _, name := range filenames {
		fi, _ := pvfs.Stat(name)
		if !helpers.PathIsWithin(name, directory) && name != directory {
			continue
		}

		pwUserLookup, err := user.LookupId(fmt.Sprintf("%d", fi.Uid()))
		username := fmt.Sprintf("%d", fi.Uid())
		if err == nil {
			username = pwUserLookup.Username
		}

		grGroupLookup, err := user.LookupGroupId(fmt.Sprintf("%d", fi.Gid()))
		groupname := fmt.Sprintf("%d", fi.Gid())
		if err == nil {
			groupname = grGroupLookup.Name
		}
		fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
			fi.ModTime().UTC().Format(time.RFC3339),
			fi.Mode(),
			username,
			groupname,
			humanize.Bytes(uint64(fi.Size())),
			name)
	}
}
