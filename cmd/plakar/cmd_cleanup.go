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

	"github.com/PlakarLabs/plakar/locking"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
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
		if lock, err := snapshot.GetLock(repository, lockID); err != nil {
			if os.IsNotExist(err) {
				// was removed since we got the list
				continue
			}
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return 1
		} else {
			if !lock.Expired(time.Minute * 15) {
				fmt.Fprintf(os.Stderr, "can't put exclusive lock: %s has ongoing operations\n", repository.Location)
				return 1
			}
		}
	}

	/*

		locksID, err := snapshot.repository.GetLocks()
		if err != nil {
			return err
		}
		for _, lockID := range locksID {
			_ = lockID
			t, _ := uuid.NewRandom()
			if lock, err := GetLock(snapshot.repository, t); err != nil {
				if os.IsNotExist(err) {
					// was removed since we got the list
					continue
				}
				return err
			} else {
				if lock.Exclusive && lock.Expired(time.Minute*15) {
					return fmt.Errorf("can't push: %s is locked", snapshot.repository.Location)
				}
			}
		}


		lockDone := make(chan bool)
		defer close(lockDone)
		go func() {
			for {
				select {
				case <-lockDone:
					return
				case <-time.After(5 * time.Minute):
					snapshot.Lock()
				}
			}
		}()
	*/

	blobs := make(map[[32]byte]bool)

	// cleanup packfiles
	// cleanup indexes

	indexesList, err := repository.GetSnapshots()
	if err != nil {
		return 1
	}

	for _, indexID := range indexesList {
		s, err := snapshot.Load(repository, indexID)
		if err != nil {
			return 1
		}

		var blobID [32]byte
		copy(blobID[:], s.Header.IndexChecksum[:32])
		blobs[blobID] = true

		copy(blobID[:], s.Header.FilesystemChecksum[:32])
		blobs[blobID] = true
	}

	blobList, err := repository.GetBlobs()
	if err != nil {
		return 1
	}

	for _, blobID := range blobList {
		if _, exists := blobs[blobID]; !exists {
			repository.DeleteBlob(blobID)
		}
	}

	return 0
}
