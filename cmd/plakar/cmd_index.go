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
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/poolpOrg/plakar/storage"
)

type JSONChunk struct {
	Start  uint
	Length uint
}

type JSONObject struct {
	Chunks      []uint64
	ContentType uint64
}

type JSONIndex struct {

	// Pathnames -> Object checksum
	Pathnames map[string]uint64

	ContentTypes map[string]uint64

	// Object checksum -> Object
	Objects map[uint64]*JSONObject

	// Chunk checksum -> Chunk
	Chunks map[uint64]*JSONChunk

	// Chunk checksum -> Object checksums
	ChunkToObjects map[uint64][]uint64

	// Object checksum -> Filenames
	ObjectToPathnames map[uint64][]uint64

	// Content Type -> Object checksums
	ContentTypeToObjects map[uint64][]uint64
}

func init() {
	registerCommand("index", cmd_index)
}

func cmd_index(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("index", flag.ExitOnError)
	flags.Parse(args)

	indexes, err := getIndexes(repository, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for _, index := range indexes {
		jindex := JSONIndex{}
		jindex.Pathnames = make(map[string]uint64)
		jindex.ContentTypes = make(map[string]uint64)
		jindex.Objects = make(map[uint64]*JSONObject)
		jindex.Chunks = make(map[uint64]*JSONChunk)
		jindex.ChunkToObjects = make(map[uint64][]uint64)
		jindex.ObjectToPathnames = make(map[uint64][]uint64)
		jindex.ContentTypeToObjects = make(map[uint64][]uint64)

		for pathname, checksumID := range index.Pathnames {
			jindex.Pathnames[pathname] = checksumID
		}

		for pathname, checksumID := range index.ContentTypes {
			jindex.ContentTypes[pathname] = checksumID
		}

		for checksumID, object := range index.Objects {
			jobject := &JSONObject{
				Chunks:      make([]uint64, 0),
				ContentType: object.ContentType,
			}

			for _, chunkChecksum := range object.Chunks {
				jobject.Chunks = append(jobject.Chunks, chunkChecksum)
			}

			jindex.Objects[checksumID] = jobject
		}

		for checksumID, chunk := range index.Chunks {
			jchunk := &JSONChunk{
				Start:  chunk.Start,
				Length: chunk.Length,
			}

			jindex.Chunks[checksumID] = jchunk
		}

		for checksum, objects := range index.ChunkToObjects {
			jindex.ChunkToObjects[checksum] = make([]uint64, 0)
			for _, objChecksum := range objects {
				jindex.ChunkToObjects[checksum] = append(jindex.ChunkToObjects[checksum], objChecksum)
			}
		}

		for checksumID, pathnames := range index.ObjectToPathnames {
			jindex.ObjectToPathnames[checksumID] = pathnames
		}

		for contentType, objects := range index.ContentTypeToObjects {
			jindex.ContentTypeToObjects[contentType] = make([]uint64, 0)
			for _, objChecksum := range objects {
				jindex.ContentTypeToObjects[contentType] = append(jindex.ContentTypeToObjects[contentType], objChecksum)
			}
		}

		serialized, err := json.Marshal(jindex)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(serialized))
	}

	return 0
}
