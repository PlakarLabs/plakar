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

	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("clone", cmd_clone)
}

func cmd_clone(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("clone", flag.ExitOnError)
	flags.Parse(args)

	sourceRepository := repository
	repositoryConfig := sourceRepository.Configuration()

	var muChunkChecksum sync.Mutex
	chunkChecksum := make(map[[32]byte]bool)

	var muObjectChecksum sync.Mutex
	objectChecksum := make(map[[32]byte]bool)

	indexes, err := sourceRepository.GetIndexes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get indexes list from repository: %s\n", ctx.Repository, err)
		return 1
	}

	for _, repository := range flags.Args() {
		var cloneRepository *storage.Repository
		if !strings.HasPrefix(repository, "/") {
			log.Fatalf("%s: does not support non filesystem plakar destinations for now", flag.CommandLine.Name())
			/*
				if strings.HasPrefix(repository, "plakar://") {
					cloneRepository, _ = storage.New("client")
				} else if strings.HasPrefix(repository, "sqlite://") {
					cloneRepository, _ = storage.New("database")
				} else {
					log.Fatalf("%s: unsupported plakar protocol", flag.CommandLine.Name())
				}
			*/
		}

		cloneRepository, err := storage.Create(repository, repositoryConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not create repository: %s\n", ctx.Repository, err)
			return 1
		}

		// XXX - this can be parallelized
		for _, indexID := range indexes {
			metadataBytes, err := sourceRepository.GetMetadata(indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get metadata from repository: %s\n", ctx.Repository, err)
				return 1
			}
			err = cloneRepository.PutMetadata(indexID, metadataBytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write metadata to repository: %s\n", repository, err)
				return 1
			}

			indexBytes, err := sourceRepository.GetIndex(indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get index from repository: %s\n", ctx.Repository, err)
				return 1
			}
			err = cloneRepository.PutIndex(indexID, indexBytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write index to repository: %s\n", repository, err)
				return 1
			}

			filesystemBytes, err := sourceRepository.GetFilesystem(indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get index from repository: %s\n", ctx.Repository, err)
				return 1
			}
			err = cloneRepository.PutFilesystem(indexID, filesystemBytes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write index to repository: %s\n", repository, err)
				return 1
			}

			index, _, err := snapshot.GetIndex(cloneRepository, indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not load index %s: %s\n", repository, indexID, err)
				return 1
			}

			for _, chunkID := range index.ListChunks() {
				muChunkChecksum.Lock()
				if _, exists := chunkChecksum[chunkID]; !exists {
					data, err := sourceRepository.GetChunk(chunkID)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s: could not get chunk from repository: %s\n", ctx.Repository, err)
						return 1
					}
					err = cloneRepository.PutChunk(chunkID, data)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s: could not put chunk to repository: %s\n", repository, err)
						return 1
					}
					chunkChecksum[chunkID] = true
				}
				muChunkChecksum.Unlock()
			}

			for _, objectID := range index.ListObjects() {
				muObjectChecksum.Lock()
				if _, exists := objectChecksum[objectID]; !exists {
					data, err := sourceRepository.GetObject(objectID)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s: could not get object from repository: %s\n", ctx.Repository, err)
						return 1
					}
					err = cloneRepository.PutObject(objectID, data)
					if err != nil {
						fmt.Fprintf(os.Stderr, "%s: could not put object to repository: %s\n", repository, err)
						return 1
					}
					objectChecksum[objectID] = true
				}
				muObjectChecksum.Unlock()
			}
		}
	}

	return 0
}
