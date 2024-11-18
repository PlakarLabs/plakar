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

package ls

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"time"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/dustin/go-humanize"
)

func init() {
	subcommands.Register("ls", cmd_ls)
}

func cmd_ls(ctx *context.Context, repo *repository.Repository, args []string) int {
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

	list_snapshot(repo, flags.Arg(0), opt_recursive)
	return 0
}

func list_snapshots(repo *repository.Repository, useUuid bool, tag string) {
	metadatas, err := utils.GetHeaders(repo, nil)
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
				hex.EncodeToString(metadata.GetIndexShortID()),
				humanize.Bytes(metadata.Summary.Directory.Size+metadata.Summary.Below.Size),
				metadata.CreationDuration.Round(time.Second),
				metadata.Importer.Directory)
		} else {
			indexID := metadata.GetIndexID()
			fmt.Fprintf(os.Stdout, "%s%38s%10s%10s %s\n",
				metadata.CreationTime.UTC().Format(time.RFC3339),
				hex.EncodeToString(indexID[:]),
				humanize.Bytes(metadata.Summary.Directory.Size+metadata.Summary.Below.Size),
				metadata.CreationDuration.Round(time.Second),
				metadata.Importer.Directory)
		}
	}
}

func _list_snapshot(pvfs *vfs.Filesystem, pathname string, recursive bool) error {
	entry, err := pvfs.Stat(pathname)
	if err != nil {
		log.Fatalf("%s: could not fetch vfs list: %s", flag.CommandLine.Name(), err)
	}

	switch entry := entry.(type) {
	case *vfs.DirEntry:
		for _, child := range entry.Children {
			fi := child.Stat()
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
			if recursive {
				err := _list_snapshot(pvfs, pathname+"/"+fi.Name(), recursive)
				if err != nil {
					log.Println(err)
				}
			}
		}

	case *vfs.FileEntry:
		fi := entry.Stat()
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
	return nil
}

func list_snapshot(repo *repository.Repository, snapshotPath string, recursive bool) error {
	prefix, pathname := utils.ParseSnapshotID(snapshotPath)

	snap, err := utils.OpenSnapshotByPrefix(repo, prefix)
	if err != nil {
		log.Fatalf("%s: could not fetch snapshot: %s", flag.CommandLine.Name(), err)
	}

	pvfs, err := snap.Filesystem()
	if err != nil {
		log.Fatal(err)
	}
	return _list_snapshot(pvfs, pathname, recursive)
}
