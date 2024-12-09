//go:build linux || darwin
// +build linux darwin

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

package mount

import (
	"flag"
	"log"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/plakarfs"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/anacrolix/fuse"
	"github.com/anacrolix/fuse/fs"
)

func init() {
	subcommands.Register("mount", cmd_mount)
}

func cmd_mount(ctx *context.Context, repo *repository.Repository, args []string) int {
	flags := flag.NewFlagSet("mount", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() != 1 {
		logging.Error("need mountpoint")
		return 1
	}

	mountpoint := flags.Arg(0)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("plakar"),
		fuse.Subtype("plakarfs"),
		fuse.LocalVolume(),
	)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}
	defer c.Close()
	logging.Info("mounted repository %s at %s", repo.Location(), mountpoint)

	err = fs.Serve(c, plakarfs.NewFS(repo, mountpoint))
	if err != nil {
		log.Fatalf("Serve: %v", err)
	}
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatalf("Mount: %v", err)
	}
	return 0
}
