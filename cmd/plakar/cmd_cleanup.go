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
	"os"
	"time"

	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/PlakarLabs/plakar/storage/locking"
)

func init() {
	registerCommand("cleanup", cmd_cleanup)
}

func cmd_cleanup(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("cleanup", flag.ExitOnError)
	flags.Parse(args)

	lock := locking.New(ctx.Hostname,
		ctx.Username,
		ctx.MachineID,
		os.Getpid(),
		true)
	currentLockID, err := snapshot.PutLock(*repository, lock)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}
	defer repository.DeleteLock(currentLockID)

	locksID, err := repository.GetLocks()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}

	for _, lockID := range locksID {
		if lockID == currentLockID {
			continue
		}
		if lock, err := snapshot.GetLock(repository, lockID); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return 1
		} else if err == nil {
			if !lock.Expired(time.Minute * 15) {
				fmt.Fprintf(os.Stderr, "can't put exclusive lock: %s has ongoing operations\n", repository.Location)
				return 1
			}
		}
	}

	// the cleanup algorithm is a bit tricky and needs to be done in the correct sequence,
	// here's what it has to do:
	//
	// 1. fetch all snapshot indexes to figure out which blobs, objects and chunks are used
	// 2. blobs that are no longer in use can be be removed
	// 3. for each object and chunk, track which packfiles contain them
	// 4. if objects or chunks are present in more than one packfile...
	// 5. decide which one keeps it and a new packfile has to be generated for the other that contains everything BUT the object/chunk
	// 6. update indexes to reflect the new packfile
	// 7. save the new index

	return 0
}
