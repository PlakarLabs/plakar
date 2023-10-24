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
)

func init() {
	registerCommand("locks", cmd_locks)
}

func cmd_locks(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("locks", flag.ExitOnError)
	flags.Parse(args)

	locksID, err := repository.GetLocks()
	if err != nil {
		return 1
	}

	for _, lockID := range locksID {
		if lock, err := snapshot.GetLock(repository, lockID); err != nil {
			if os.IsNotExist(err) {
				// was removed since we got the list
				continue
			}
			fmt.Println(err)
		} else {
			var lockType string
			if lock.Exclusive {
				lockType = "X-lock"
			} else {
				lockType = "S-lock"
			}

			var expired string
			if lock.Expired(15 * time.Minute) {
				expired = " (expired)"
			}
			fmt.Printf("[%s] %s on %s by %s@%s%s\n",
				lockID, lockType, lock.Timestamp.UTC().Format(time.RFC3339), lock.Username, lock.Hostname, expired)
		}
	}

	return 0
}
