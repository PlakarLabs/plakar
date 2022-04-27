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

	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

type JSONChunk struct {
	Checksum string
	Start    uint
	Length   uint
}

type JSONObject struct {
	Checksum    string
	Chunks      []string
	ContentType string
}

type JSONIndex struct {
	Filesystem *snapshot.Filesystem

	// Pathnames -> Object checksum
	Pathnames map[string]string

	// Object checksum -> Object
	Objects map[string]*JSONObject

	// Chunk checksum -> Chunk
	Chunks map[string]*JSONChunk

	// Chunk checksum -> Object checksums
	ChunkToObjects map[string][]string

	// Object checksum -> Filenames
	ObjectToPathnames map[string][]string

	// Content Type -> Object checksums
	ContentTypeToObjects map[string][]string
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
		jindex.Pathnames = make(map[string]string)
		jindex.Objects = make(map[string]*JSONObject)
		jindex.Chunks = make(map[string]*JSONChunk)
		jindex.ChunkToObjects = make(map[string][]string)
		jindex.ObjectToPathnames = make(map[string][]string)
		jindex.ContentTypeToObjects = make(map[string][]string)

		for pathname, checksum := range index.Pathnames {
			jindex.Pathnames[pathname] = fmt.Sprintf("%064x", checksum)
		}

		for checksum, object := range index.Objects {
			jobject := &JSONObject{
				Checksum:    fmt.Sprintf("%064x", checksum),
				Chunks:      make([]string, 0),
				ContentType: object.ContentType,
			}

			for _, chunkChecksum := range object.Chunks {
				jobject.Chunks = append(jobject.Chunks, fmt.Sprintf("%064x", chunkChecksum))
			}

			jindex.Objects[fmt.Sprintf("%064x", checksum)] = jobject
		}

		for checksum, chunk := range index.Chunks {
			jchunk := &JSONChunk{
				Checksum: fmt.Sprintf("%064x", checksum),
				Start:    chunk.Start,
				Length:   chunk.Length,
			}

			jindex.Chunks[fmt.Sprintf("%064x", checksum)] = jchunk
		}

		for checksum, objects := range index.ChunkToObjects {
			jindex.ChunkToObjects[fmt.Sprintf("%064x", checksum)] = make([]string, 0)
			for _, objChecksum := range objects {
				jindex.ChunkToObjects[fmt.Sprintf("%064x", checksum)] = append(jindex.ChunkToObjects[fmt.Sprintf("%064x", checksum)], fmt.Sprintf("%064x", objChecksum))
			}
		}

		for checksum, pathnames := range index.ObjectToPathnames {
			jindex.ObjectToPathnames[fmt.Sprintf("%064x", checksum)] = pathnames
		}

		for contentType, objects := range index.ContentTypeToObjects {
			jindex.ContentTypeToObjects[contentType] = make([]string, 0)
			for _, objChecksum := range objects {
				jindex.ContentTypeToObjects[contentType] = append(jindex.ContentTypeToObjects[contentType], fmt.Sprintf("%064x", objChecksum))
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
