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
	"sync"

	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/helpers"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
)

func init() {
	registerCommand("sync", cmd_sync)
}

func cmd_sync(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	flags.Parse(args)

	snapshotID := ""
	direction := ""
	syncRepository := ""
	switch flags.NArg() {
	case 2:
		direction = flags.Arg(0)
		syncRepository = flags.Arg(1)

	case 3:
		snapshotID = flags.Arg(0)
		direction = flags.Arg(1)
		syncRepository = flags.Arg(2)

	default:
		logger.Error("usage: %s [snapshotID] to|from repository", flags.Name())
		return 1
	}

	var srcRepository *storage.Repository
	var dstRepository *storage.Repository
	var err error
	if direction == "to" {
		srcRepository = repository
		dstRepository, err = storage.Open(syncRepository)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not open repository: %s\n", ctx.Repository, err)
			return 1
		}
		repositoryIndex, err := loadRepositoryIndex(dstRepository)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not fetch repository index: %s\n", dstRepository.Location, err)
			return 1

		}
		dstRepository.SetRepositoryIndex(repositoryIndex)

	} else if direction == "from" {
		dstRepository = repository
		srcRepository, err = storage.Open(syncRepository)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not open repository: %s\n", ctx.Repository, err)
			return 1
		}
		repositoryIndex, err := loadRepositoryIndex(srcRepository)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not fetch repository index: %s\n", srcRepository.Location, err)
			return 1
		}
		srcRepository.SetRepositoryIndex(repositoryIndex)
	} else {
		logger.Error("usage: %s [snapshotID] to|from repository", flags.Name())
		return 1
	}

	var muChunkChecksum sync.Mutex
	chunkChecksum := make(map[[32]byte]bool)

	var muObjectChecksum sync.Mutex
	objectChecksum := make(map[[32]byte]bool)

	sourceIndexes, err := srcRepository.GetSnapshots()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get indexes list from repository: %s\n", ctx.Repository, err)
		return 1
	}

	if dstRepository.Configuration().Encryption != "" {
		for {
			passphrase, err := helpers.GetPassphrase("destination repository")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			secret, err := encryption.DeriveSecret(passphrase, dstRepository.Configuration().EncryptionKey)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			dstRepository.SetSecret(secret)
			break
		}
	}

	destIndexes, err := dstRepository.GetSnapshots()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get indexes list from repository: %s\n", ctx.Repository, err)
		return 1
	}

	syncIndexes := make([]uuid.UUID, 0)

	for _, index := range findSnapshotByPrefix(sourceIndexes, snapshotID) {
		if !indexArrayContains(destIndexes, index) {
			syncIndexes = append(syncIndexes, index)
		}
	}

	wg := sync.WaitGroup{}
	for _, _indexID := range syncIndexes {
		wg.Add(1)
		go func(indexID uuid.UUID) {
			defer wg.Done()
			sourceSnapshot, err := snapshot.Load(srcRepository, indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not load snapshot from repository: %s\n", ctx.Repository, err)
				return
			}

			copySnapshot, err := snapshot.New(dstRepository, indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not create snapshot in repository: %s\n", syncRepository, err)
				return
			}

			// rebuild a new snapshot w/ identical fs, but destination specific index and rebuilt metadata
			// should share same UUID but take into account configuration differnces
			copySnapshot.Header = sourceSnapshot.Header
			copySnapshot.Filesystem = sourceSnapshot.Filesystem
			copySnapshot.Index = sourceSnapshot.Index
			copySnapshot.Metadata = sourceSnapshot.Metadata

			fmt.Println(copySnapshot)

			wg2 := sync.WaitGroup{}
			for _, _chunkID := range sourceSnapshot.Index.ListChunks() {
				wg2.Add(1)
				go func(chunkID [32]byte) {
					defer wg2.Done()
					muChunkChecksum.Lock()
					_, exists := chunkChecksum[chunkID]
					muChunkChecksum.Unlock()
					if !exists {
						exists := copySnapshot.CheckChunk(chunkID)
						if !exists {
							data, err := sourceSnapshot.GetChunk(chunkID)
							if err != nil {
								fmt.Fprintf(os.Stderr, "%s: could not get chunk from repository: %s\n", ctx.Repository, err)
								return
							}
							err = copySnapshot.PutChunk(chunkID, data)
							if err != nil {
								fmt.Fprintf(os.Stderr, "%s: could not put chunk to repository: %s\n", syncRepository, err)
								return
							}
						}
						muChunkChecksum.Lock()
						chunkChecksum[chunkID] = true
						muChunkChecksum.Unlock()
					}
				}(_chunkID)
			}
			wg2.Wait()

			wg3 := sync.WaitGroup{}
			for _, _objectID := range sourceSnapshot.Index.ListObjects() {
				wg3.Add(1)
				go func(objectID [32]byte) {
					defer wg3.Done()
					muObjectChecksum.Lock()
					_, exists := objectChecksum[objectID]
					muObjectChecksum.Unlock()

					if !exists {
						exists := copySnapshot.CheckObject(objectID)
						if !exists {
							object := sourceSnapshot.Index.LookupObject(objectID)
							err = copySnapshot.PutObject(object)
							if err != nil {
								fmt.Fprintf(os.Stderr, "%s: could not put object to repository: %s\n", syncRepository, err)
								return
							}
						}
						muObjectChecksum.Lock()
						objectChecksum[objectID] = true
						muObjectChecksum.Unlock()
					}
				}(_objectID)
			}
			wg3.Wait()

			err = copySnapshot.Commit()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not commit object to repository: %s\n", syncRepository, err)
				return
			}
		}(_indexID)
	}
	wg.Wait()
	return 0
}
