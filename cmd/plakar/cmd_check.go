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
	"log"
	"sync"

	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

func cmd_check(ctx Plakar, args []string) int {
	var enableFastCheck bool

	flags := flag.NewFlagSet("check", flag.ExitOnError)
	flags.BoolVar(&enableFastCheck, "fast", false, "enable fast checking (no checksum verification)")
	flags.Parse(args)

	if flags.NArg() == 0 {
		return check_plakar(ctx.Store())
	}

	snapshots, err := getSnapshots(ctx.Store(), flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	failures := false
	for offset, snapshot := range snapshots {
		_, pattern := parseSnapshotID(flags.Args()[offset])

		ok, err := snapshot.Check(pattern, enableFastCheck)
		if err != nil {
			logger.Warn("%s", err)
		}

		if !ok {
			failures = true
		}
	}

	if failures {
		return 1
	}
	return 0
}

func check_plakar(store *storage.Store) int {
	indexes, err := store.GetIndexes()
	if err != nil {
		logger.Warn("%s", err)
		return 1
	}

	muChunks := sync.Mutex{}
	chunks := make(map[string]uint16)

	muObjects := sync.Mutex{}
	objects := make(map[string]uint16)

	errors := 0

	for _, index := range indexes {
		snap, err := snapshot.Load(store, index)
		if err != nil {
			logger.Warn("%s", err)
			errors++
			continue
		}

		for chunkChecksum := range snap.Chunks {
			muChunks.Lock()
			if _, exists := chunks[chunkChecksum]; !exists {
				chunks[chunkChecksum] = 0
			}
			chunks[chunkChecksum] = chunks[chunkChecksum] + 1
			muChunks.Unlock()
		}

		for objectChecksum := range snap.Objects {
			muObjects.Lock()
			if _, exists := objects[objectChecksum]; !exists {
				objects[objectChecksum] = 0
			}
			objects[objectChecksum] = objects[objectChecksum] + 1
			muObjects.Unlock()
		}
	}

	chunksChecksums, err := store.GetChunks()
	if err != nil {
		logger.Warn("%s", err)
		errors++
		return 1
	}

	objectsChecksums, err := store.GetObjects()
	if err != nil {
		logger.Warn("%s", err)
		errors++
		return 1
	}

	for _, checksum := range chunksChecksums {
		if _, exists := chunks[checksum]; !exists {
			logger.Warn("orphan chunk: %s", checksum)
			errors++
		}
	}

	for _, checksum := range objectsChecksums {
		if _, exists := objects[checksum]; !exists {
			logger.Warn("orphan object: %s", checksum)
			errors++
		}
	}

	for chunkChecksum, count := range chunks {
		refCount, err := store.GetChunkRefCount(chunkChecksum)
		if err != nil {
			logger.Warn("%s", err)
			errors++
		} else if refCount != uint64(count) {
			logger.Warn("invalid references count: %s", chunkChecksum)
			errors++
		}
	}

	for objectChecksum, count := range objects {
		refCount, err := store.GetObjectRefCount(objectChecksum)
		if err != nil {
			logger.Warn("%s", err)
			errors++
		} else if refCount != uint64(count) {
			logger.Warn("invalid references count: %s", objectChecksum)
			errors++
		}
	}

	return 0
}
