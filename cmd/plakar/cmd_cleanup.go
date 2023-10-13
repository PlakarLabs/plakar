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

	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("cleanup", cmd_cleanup)
}

func cmd_cleanup(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("cleanup", flag.ExitOnError)
	flags.Parse(args)

	chunks := make(map[[32]byte]bool)
	objects := make(map[[32]byte]bool)

	indexesList, err := repository.GetIndexes()
	if err != nil {
		return 1
	}

	for _, indexID := range indexesList {
		s, err := snapshot.Load(repository, indexID)
		if err != nil {
			return 1
		}
		for _, objectID := range s.Index.ListObjects() {
			objects[objectID] = true
		}
		for _, chunkID := range s.Index.ListChunks() {
			chunks[chunkID] = true
		}
	}

	objectList, err := repository.GetObjects()
	if err != nil {
		return 1
	}

	chunkList, err := repository.GetChunks()
	if err != nil {
		return 1
	}

	for _, objectID := range objectList {
		if _, exists := objects[objectID]; !exists {
			repository.DeleteObject(objectID)
		}
	}

	for _, chunkID := range chunkList {
		if _, exists := chunks[chunkID]; !exists {
			repository.DeleteChunk(chunkID)
		}
	}

	return 0
}
