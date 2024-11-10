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

package verify

import (
	"flag"
	"fmt"
	"log"

	"github.com/PlakarLabs/plakar/cmd/plakar/subcommands"
	"github.com/PlakarLabs/plakar/cmd/plakar/utils"
	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot"
)

func init() {
	subcommands.Register("verify", cmd_verify)
}

func cmd_verify(ctx *context.Context, repo *repository.Repository, args []string) int {
	var opt_concurrency uint64
	var opt_fastCheck bool
	var opt_quiet bool

	flags := flag.NewFlagSet("verify", flag.ExitOnError)
	flags.Uint64Var(&opt_concurrency, "concurrency", uint64(ctx.GetNumCPU())*8+1, "maximum number of parallel tasks")
	flags.BoolVar(&opt_fastCheck, "fast", false, "enable fast checking (no checksum verification)")
	flags.BoolVar(&opt_quiet, "quiet", false, "suppress output")
	flags.Parse(args)

	go eventsProcessorStdio(ctx, opt_quiet)

	var snapshots []string
	if flags.NArg() == 0 {
		for snapshotID := range repo.ListSnapshots() {
			snapshots = append(snapshots, fmt.Sprintf("%x", snapshotID))
		}
	} else {
		snapshots = flags.Args()
	}

	opts := &snapshot.CheckOptions{
		MaxConcurrency: opt_concurrency,
		FastCheck:      opt_fastCheck,
	}

	failures := false
	for _, arg := range snapshots {
		snapshotPrefix, pathname := utils.ParseSnapshotID(arg)
		snap, err := utils.OpenSnapshotByPrefix(repo, snapshotPrefix)
		if err != nil {
			log.Fatal(err)
		}
		if ok, err := snap.Verify(pathname, opts); err != nil {
			logger.Warn("%s", err)
		} else if !ok {
			failures = true
		}
	}

	if failures {
		return 1
	}
	return 0
}
