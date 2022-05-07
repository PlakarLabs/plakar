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

package main

import (
	"context"
	"flag"
	"log"

	"github.com/jacobsa/fuse"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/plakarfs"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("mount", cmd_mount)
}

func cmd_mount(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("mount", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() != 1 {
		logger.Error("need mountpoint")
		return 1
	}

	mountpoint := flags.Arg(0)

	// Create an appropriate file system.
	server, err := plakarfs.NewPlakarFS(repository, mountpoint)
	if err != nil {
		log.Fatalf("makeFS: %v", err)
	}

	cfg := &fuse.MountConfig{
		ReadOnly: true,
	}

	mfs, err := fuse.Mount(mountpoint, server, cfg)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}

	// Wait for it to be unmounted.
	if err = mfs.Join(context.Background()); err != nil {
		log.Fatalf("Join: %v", err)
	}

	return 0
}
