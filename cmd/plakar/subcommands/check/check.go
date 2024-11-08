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

package check

import (
	"flag"
	"fmt"
	"log"

	"github.com/PlakarLabs/plakar/cmd/plakar/subcommands"
	"github.com/PlakarLabs/plakar/cmd/plakar/utils"
	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/events"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot"
)

func init() {
	subcommands.Register("check", cmd_check)
}

func eventsProcessor(ctx *context.Context) chan struct{} {
	done := make(chan struct{})
	go func() {
		for event := range ctx.Events().Listen() {
			switch event := event.(type) {
			case events.Start:
			case events.Done:

			case events.Path:
				logger.Info("checking pathname: %s", event.Pathname)
			case events.Directory:
				logger.Info("checking directory: %s", event.Pathname)
			case events.File:
				logger.Info("checking file: %s", event.Pathname)
			case events.Object:
				logger.Info("checking object: %x", event.Checksum)
			case events.Chunk:
				logger.Info("checking chunk: %x", event.Checksum)
			case events.DirectoryMissing:
				logger.Warn("missing directory: %s", event.Pathname)
			case events.FileMissing:
				logger.Warn("missing file: %s", event.Pathname)
			case events.ObjectMissing:
				logger.Warn("missing object: %x", event.Checksum)
			case events.ChunkMissing:
				logger.Warn("missing chunk: %x", event.Checksum)
			case events.FileCorrupted:
				logger.Warn("corrupted file: %x", event.Pathname)
			case events.ObjectCorrupted:
				logger.Warn("corrupted object: %x", event.Checksum)
			case events.ChunkCorrupted:
				logger.Warn("corrupted chunk: %x", event.Checksum)

			case events.ChunkOK:
			case events.ObjectOK:
			case events.FileOK:

			default:
				fmt.Printf("event: %T\n", event)
			}
		}
		done <- struct{}{}
	}()
	return done
}

func cmd_check(ctx *context.Context, repo *repository.Repository, args []string) int {
	var enableFastCheck bool
	var opt_concurrency uint64

	flags := flag.NewFlagSet("check", flag.ExitOnError)
	flags.BoolVar(&enableFastCheck, "fast", false, "enable fast checking (no checksum verification)")
	flags.Uint64Var(&opt_concurrency, "max-concurrency", uint64(ctx.GetNumCPU())*8+1, "maximum number of parallel tasks")
	flags.Parse(args)

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
		FastCheck:      enableFastCheck,
	}

	go eventsProcessor(ctx)

	failures := false
	for _, arg := range snapshots {
		snapshotPrefix, pathname := utils.ParseSnapshotID(arg)
		snap, err := utils.OpenSnapshotByPrefix(repo, snapshotPrefix)
		if err != nil {
			log.Fatal(err)
		}
		if ok, err := snap.Check(pathname, opts); err != nil {
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
