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
	registerCommand("clone", cmd_clone)
}

func cmd_clone(ctx Plakar, store *storage.Store, args []string) int {
	flags := flag.NewFlagSet("clone", flag.ExitOnError)
	flags.Parse(args)

	sourceStore := store
	storeConfig := sourceStore.Configuration()

	chunkChecksums, err := sourceStore.GetChunks()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get chunks list from store: %s\n", ctx.Repository, err)
		return 1
	}

	objectChecksums, err := sourceStore.GetObjects()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get objects list from store: %s\n", ctx.Repository, err)
		return 1
	}

	indexes, err := sourceStore.GetIndexes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get indexes list from store: %s\n", ctx.Repository, err)
		return 1
	}

	for _, repository := range flags.Args() {
		var cloneStore *storage.Store
		if !strings.HasPrefix(repository, "/") {
			log.Fatalf("%s: does not support non filesystem plakar destinations for now", flag.CommandLine.Name())
			/*
				if strings.HasPrefix(repository, "plakar://") {
					cloneStore, _ = storage.New("client")
				} else if strings.HasPrefix(repository, "sqlite://") {
					cloneStore, _ = storage.New("database")
				} else {
					log.Fatalf("%s: unsupported plakar protocol", flag.CommandLine.Name())
				}
			*/
		}

		cloneStore, err := storage.Create(repository, storeConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not create store: %s\n", ctx.Repository, err)
			return 1
		}

		for _, chunkChecksum := range chunkChecksums {
			data, err := sourceStore.GetChunk(chunkChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get chunk from store: %s\n", ctx.Repository, err)
				return 1
			}
			err = cloneStore.PutChunk(chunkChecksum, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write chunk to store: %s\n", repository, err)
				return 1
			}
		}

		for _, objectChecksum := range objectChecksums {
			data, err := sourceStore.GetObject(objectChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get object from store: %s\n", ctx.Repository, err)
				return 1
			}
			err = cloneStore.PutObject(objectChecksum, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write object to store: %s\n", repository, err)
				return 1
			}
		}

		for _, indexID := range indexes {
			data, err := sourceStore.GetMetadata(indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get metadata from store: %s\n", ctx.Repository, err)
				return 1
			}
			err = cloneStore.PutMetadata(indexID, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write metadata to store: %s\n", repository, err)
				return 1
			}

			data, err = sourceStore.GetIndex(indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get index from store: %s\n", ctx.Repository, err)
				return 1
			}
			err = cloneStore.PutIndex(indexID, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not write index to store: %s\n", repository, err)
				return 1
			}

			snap, err := snapshot.Load(cloneStore, indexID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not load index from store: %s\n", repository, err)
				return 1
			}

			for _, chunk := range snap.Index.Chunks {
				err = cloneStore.ReferenceIndexChunk(indexID, chunk.Checksum)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: could not reference chunk in store: %s\n", repository, err)
					return 1
				}
			}

			for _, object := range snap.Index.Objects {
				err = cloneStore.ReferenceIndexObject(indexID, object.Checksum)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%s: could not reference object in store: %s\n", repository, err)
					return 1
				}
			}

		}

	}

	return 0
}
