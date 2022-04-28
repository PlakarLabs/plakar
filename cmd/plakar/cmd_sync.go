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
	"sync"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("sync", cmd_sync)
}

func cmd_sync(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	flags.Parse(args)

	sourceRepository := repository

	var muChunkChecksum sync.Mutex
	chunkChecksum := make(map[[32]byte]bool)

	var muObjectChecksum sync.Mutex
	objectChecksum := make(map[[32]byte]bool)

	sourceIndexes, err := sourceRepository.GetIndexes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get indexes list from repository: %s\n", ctx.Repository, err)
		return 1
	}

	for _, repository := range flags.Args() {
		var syncRepository *storage.Repository
		if !strings.HasPrefix(repository, "/") {
			log.Fatalf("%s: does not support non filesystem plakar destinations for now", flag.CommandLine.Name())
			/*
				if strings.HasPrefix(repository, "plakar://") {
					syncrepository, _ = storage.New("client")
				} else if strings.HasPrefix(repository, "sqlite://") {
					syncrepository, _ = storage.New("database")
				} else {
					log.Fatalf("%s: unsupported plakar protocol", flag.CommandLine.Name())
				}
			*/
		}

		syncRepository, err = storage.Open(repository)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not open repository: %s\n", ctx.Repository, err)
			return 1
		}

		destIndexes, err := syncRepository.GetIndexes()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get indexes list from repository: %s\n", ctx.Repository, err)
			return 1
		}

		_ = sourceIndexes
		_ = destIndexes

		syncIndexes := make([]uuid.UUID, 0)

		for _, index := range sourceIndexes {
			if !indexArrayContains(destIndexes, index) {
				syncIndexes = append(syncIndexes, index)
			}
		}

		for _, indexID := range syncIndexes {

			sourceSnapshot, err := snapshot.Load(sourceRepository, indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not load snapshot from repository: %s\n", ctx.Repository, err)
				return 1
			}

			copySnapshot, err := snapshot.New(syncRepository)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not create snapshot in repository: %s\n", syncRepository, err)
				return 1
			}

			// rebuild a new snapshot w/ identical fs, but destination specific index and rebuilt metadata
			// should share same UUID but take into account configuration differnces
			copySnapshot.Metadata = sourceSnapshot.Metadata
			copySnapshot.Filesystem = sourceSnapshot.Filesystem
			copySnapshot.Index = sourceSnapshot.Index

			for _, chunkID := range sourceSnapshot.Index.ListChunks() {
				muChunkChecksum.Lock()
				if _, exists := chunkChecksum[chunkID]; !exists {
					exists, err := sourceSnapshot.CheckChunk(chunkID)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s: could not check chunk from repository: %s\n", ctx.Repository, err)
						return 1
					}
					if !exists {
						data, err := sourceSnapshot.GetChunk(chunkID)
						if err != nil {
							fmt.Fprintf(os.Stderr, "%s: could not get chunk from repository: %s\n", ctx.Repository, err)
							return 1
						}
						err = copySnapshot.PutChunk(chunkID, data)
						if err != nil {
							fmt.Fprintf(os.Stderr, "%s: could not put chunk to repository: %s\n", repository, err)
							return 1
						}
					}
				}
				chunkChecksum[chunkID] = true
				muChunkChecksum.Unlock()
			}

			for _, objectID := range sourceSnapshot.Index.ListObjects() {
				muObjectChecksum.Lock()
				if _, exists := objectChecksum[objectID]; !exists {
					exists, err := sourceSnapshot.CheckObject(objectID)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s: could not check object from repository: %s\n", ctx.Repository, err)
						return 1
					}
					if !exists {
						object, err := sourceSnapshot.GetObject(objectID)
						if err != nil {
							fmt.Fprintf(os.Stderr, "%s: could not get object from repository: %s\n", ctx.Repository, err)
							return 1
						}
						err = copySnapshot.PutObject(object)
						if err != nil {
							fmt.Fprintf(os.Stderr, "%s: could not put object to repository: %s\n", repository, err)
							return 1
						}
					}
				}
				objectChecksum[objectID] = true
				muObjectChecksum.Unlock()
			}
			err = copySnapshot.Commit()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not commit object to repository: %s\n", repository, err)
				return 1
			}
		}

	}

	return 0
}
