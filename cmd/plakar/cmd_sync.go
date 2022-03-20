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
	"log"
	"os"
	"strings"

	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("sync", cmd_sync)
}

func cmd_sync(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	flags.Parse(args)

	sourceStore := ctx.Store()
	sourceChunkChecksums, err := sourceStore.GetChunks()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get chunks list from store: %s\n", ctx.Repository, err)
		return 1
	}

	sourceObjectChecksums, err := sourceStore.GetObjects()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get objects list from store: %s\n", ctx.Repository, err)
		return 1
	}

	sourceIndexes, err := sourceStore.GetIndexes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get indexes list from store: %s\n", ctx.Repository, err)
		return 1
	}

	for _, repository := range flags.Args() {
		var syncStore *storage.Store
		if !strings.HasPrefix(repository, "/") {
			log.Fatalf("%s: does not support non filesystem plakar destinations for now", flag.CommandLine.Name())
			/*
				if strings.HasPrefix(repository, "plakar://") {
					syncStore, _ = storage.New("client")
				} else if strings.HasPrefix(repository, "sqlite://") {
					syncStore, _ = storage.New("database")
				} else {
					log.Fatalf("%s: unsupported plakar protocol", flag.CommandLine.Name())
				}
			*/
		} else {
			syncStore, _ = storage.New("filesystem")
		}

		err = syncStore.Open(repository)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not open store: %s\n", ctx.Repository, err)
			return 1
		}

		destChunkChecksums, err := syncStore.GetChunks()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get chunks list from store: %s\n", ctx.Repository, err)
			return 1
		}

		destObjectChecksums, err := syncStore.GetObjects()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get objects list from store: %s\n", ctx.Repository, err)
			return 1
		}

		destIndexes, err := syncStore.GetIndexes()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get indexes list from store: %s\n", ctx.Repository, err)
			return 1
		}

		_ = sourceChunkChecksums
		_ = sourceObjectChecksums
		_ = sourceIndexes

		_ = destChunkChecksums
		_ = destObjectChecksums
		_ = destIndexes

		syncChunkChecksums := make([]string, 0)
		syncObjectChecksums := make([]string, 0)
		syncIndexes := make([]string, 0)

		for _, chunkChecksum := range sourceChunkChecksums {
			if !arrayContains(destChunkChecksums, chunkChecksum) {
				syncChunkChecksums = append(syncChunkChecksums, chunkChecksum)
			}
		}

		for _, objectChecksum := range sourceObjectChecksums {
			if !arrayContains(destObjectChecksums, objectChecksum) {
				syncObjectChecksums = append(syncObjectChecksums, objectChecksum)
			}
		}

		for _, index := range sourceIndexes {
			if !arrayContains(destIndexes, index) {
				syncIndexes = append(syncIndexes, index)
			}
		}

		for _, chunkChecksum := range syncChunkChecksums {
			data, err := sourceStore.GetChunk(chunkChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get chunk from store: %s\n", ctx.Repository, err)
				return 1
			}
			err = syncStore.PutChunk(chunkChecksum, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write chunk to store: %s\n", repository, err)
				return 1
			}
		}

		for _, objectChecksum := range syncObjectChecksums {
			data, err := sourceStore.GetObject(objectChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get object from store: %s\n", ctx.Repository, err)
				return 1
			}
			err = syncStore.PutObject(objectChecksum, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write object to store: %s\n", repository, err)
				return 1
			}
		}

		for _, index := range syncIndexes {
			data, err := sourceStore.GetIndex(index)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get index from store: %s\n", ctx.Repository, err)
				return 1
			}
			err = syncStore.PutIndex(index, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write object to store: %s\n", repository, err)
				return 1
			}

			snap, err := snapshot.Load(syncStore, index)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not load index from store: %s\n", repository, err)
				return 1
			}

			for _, chunk := range snap.Chunks {
				err = syncStore.ReferenceIndexChunk(index, chunk.Checksum)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: could not reference chunk in store: %s\n", repository, err)
					return 1
				}
			}

			for _, object := range snap.Objects {
				err = syncStore.ReferenceIndexObject(index, object.Checksum)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: could not reference object in store: %s\n", repository, err)
					return 1
				}
			}

		}

	}

	return 0
}
