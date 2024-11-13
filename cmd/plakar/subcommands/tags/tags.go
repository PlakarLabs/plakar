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
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
)

func init() {
	subcommands.Register("tags", cmd_tags)
}

func cmd_tags(ctx *context.Context, repo *repository.Repository, args []string) int {
	var opt_display string
	flags := flag.NewFlagSet("tags", flag.ExitOnError)
	flags.StringVar(&opt_display, "display", "tags", "display tags")
	flags.Parse(args)

	if opt_display != "tags" && opt_display != "count" && opt_display != "snapshots" {
		fmt.Fprintf(os.Stderr, "unsupported display option: %s\n", opt_display)
		return 1
	}

	list_tags(repo, opt_display)
	return 0
}

func list_tags(repo *repository.Repository, display string) {
	tags := make(map[string][][32]byte)
	for snapshotID := range repo.ListSnapshots() {
		snap, err := snapshot.Load(repo, snapshotID)
		if err != nil {
			continue
		}
		for _, tag := range snap.Header.Tags {
			if _, ok := tags[tag]; !ok {
				tags[tag] = make([][32]byte, 0)
			}
			tags[tag] = append(tags[tag], snapshotID)
		}
	}

	tagsList := make([]string, 0, len(tags))
	for tag := range tags {
		tagsList = append(tagsList, tag)
	}
	sort.Slice(tagsList, func(i, j int) bool {
		return tagsList[i] < tagsList[j]
	})

	if display == "tags" {
		fmt.Println(strings.Join(tagsList, "\n"))
	} else if display == "count" {
		for _, tag := range tagsList {
			fmt.Printf("%s: %d\n", tag, len(tags[tag]))
		}
	} else if display == "snapshots" {
		for _, tag := range tagsList {
			fmt.Printf("%s:\n", tag)
			for _, snapshotID := range tags[tag] {
				fmt.Printf("  - %x\n", snapshotID[:4])
			}
		}
	}
}
