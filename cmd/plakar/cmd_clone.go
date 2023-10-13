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

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("clone", cmd_clone)
}

func cmd_clone(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("clone", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() != 2 || flags.Arg(0) != "to" {
		logger.Error("usage: %s to repository", flags.Name())
		return 1
	}

	cloneRepositoryName := flags.Arg(0)

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

	cloneRepository, err := storage.Create(flags.Arg(1), repositoryConfig)
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
		_, err = cloneRepository.PutMetadata(indexID, metadataBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not write metadata to repository: %s\n", cloneRepositoryName, err)
			return 1
		}

		indexBytes, err := sourceRepository.GetIndex(indexID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get index from repository: %s\n", ctx.Repository, err)
			return 1
		}
		_, err = cloneRepository.PutIndex(indexID, indexBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not write index to repository: %s\n", cloneRepositoryName, err)
			return 1
		}

		filesystemBytes, err := sourceRepository.GetFilesystem(indexID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not get index from repository: %s\n", ctx.Repository, err)
			return 1
		}
		_, err = cloneRepository.PutFilesystem(indexID, filesystemBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not write index to repository: %s\n", cloneRepositoryName, err)
			return 1
		}

		sourceSnapshot, err := snapshot.Load(sourceRepository, indexID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not load index %s: %s\n", cloneRepositoryName, indexID, err)
			return 1
		}

		for _, chunkID := range sourceSnapshot.Index.ListChunks() {
			muChunkChecksum.Lock()
			if _, exists := chunkChecksum[chunkID]; !exists {
				data, err := sourceRepository.GetChunk(chunkID)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: could not get chunk from repository: %s\n", ctx.Repository, err)
					return 1
				}
				_, err = cloneRepository.PutChunk(chunkID, data)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: could not put chunk to repository: %s\n", cloneRepositoryName, err)
					return 1
				}
				chunkChecksum[chunkID] = true
			}
			muChunkChecksum.Unlock()
		}

		for _, objectID := range sourceSnapshot.Index.ListObjects() {
			muObjectChecksum.Lock()
			if _, exists := objectChecksum[objectID]; !exists {
				data, err := sourceRepository.GetObject(objectID)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: could not get object from repository: %s\n", ctx.Repository, err)
					return 1
				}
				_, err = cloneRepository.PutObject(objectID, data)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: could not put object to repository: %s\n", cloneRepositoryName, err)
					return 1
				}
				objectChecksum[objectID] = true
			}
			muObjectChecksum.Unlock()
		}
	}

	return 0
}
