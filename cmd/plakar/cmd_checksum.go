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
	"io"

	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

func init() {
	registerCommand("checksum", cmd_checksum)
}

func cmd_checksum(ctx *context.Context, repo *repository.Repository, args []string) int {
	var enableFastChecksum bool

	flags := flag.NewFlagSet("checksum", flag.ExitOnError)
	flags.BoolVar(&enableFastChecksum, "fast", false, "enable fast checksum (return recorded checksum)")

	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameter is required", flags.Name())
		return 1
	}

	snapshots, err := getSnapshots(repo, flags.Args())
	if err != nil {
		logger.Error("%s: could not obtain snapshots list: %s", flags.Name(), err)
		return 1
	}

	errors := 0
	for offset, snap := range snapshots {
		_, pathname := parseSnapshotID(flags.Args()[offset])
		if pathname == "" {
			logger.Error("%s: missing filename for snapshot %s", flags.Name(), snap.Header.GetIndexShortID())
			errors++
			continue
		}

		fs, err := snap.Filesystem()
		if err != nil {
			logger.Error("%s: %s: %s", flags.Name(), pathname, err)
			errors++
			continue
		}

		fsinfo, err := fs.Stat(pathname)
		if err != nil {
			logger.Error("%s: %s: %s", flags.Name(), pathname, err)
			errors++
			continue
		}

		if _, isDir := fsinfo.(*vfs.DirEntry); isDir {
			logger.Error("%s: %s: is a directory", flags.Name(), pathname)
			errors++
			continue
		}

		if fsinfo, isRegular := fsinfo.(*vfs.FileEntry); !isRegular {
			logger.Error("%s: %s: is not a regular file", flags.Name(), pathname)
			errors++
			continue
		} else if !fsinfo.FileInfo().Mode().IsRegular() {
			logger.Error("%s: %s: is not a regular file", flags.Name(), pathname)
			errors++
			continue
		}

		info := fsinfo.(*vfs.FileEntry)
		object, err := snap.LookupObject(info.Checksum)
		if err != nil {
			logger.Error("%s: %s: %s", flags.Name(), pathname, err)
			errors++
			continue
		}

		checksum := object.Checksum
		if !enableFastChecksum {
			rd, err := snap.NewReader(pathname)
			if err != nil {
				logger.Error("%s: %s: %s", flags.Name(), pathname, err)
				errors++
				continue
			}

			hasher := repo.Hasher()
			if _, err := io.Copy(hasher, rd); err != nil {
				logger.Error("%s: %s: %s", flags.Name(), pathname, err)
				errors++
				continue
			}
		}
		fmt.Printf("SHA256 (%s) = %x\n", pathname, checksum)
	}

	return 0
}
