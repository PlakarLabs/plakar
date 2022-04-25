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
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/snapshot"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("info", cmd_info)
}

func cmd_info(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("info", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		return info_plakar(repository)
	}

	metadatas, err := getMetadatas(repository, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for _, metadata := range metadatas {
		fmt.Printf("Uuid: %s\n", metadata.Uuid)
		fmt.Printf("CreationTime: %s\n", metadata.CreationTime)
		fmt.Printf("Version: %s\n", metadata.Version)
		fmt.Printf("Hostname: %s\n", metadata.Hostname)
		fmt.Printf("Username: %s\n", metadata.Username)
		fmt.Printf("CommandLine: %s\n", metadata.CommandLine)
		fmt.Printf("MachineID: %s\n", metadata.MachineID)
		fmt.Printf("PublicKey: %s\n", metadata.PublicKey)
		fmt.Printf("Directories: %d\n", metadata.Statistics.Directories)
		fmt.Printf("Files: %d\n", metadata.Statistics.Files)
		fmt.Printf("NonRegular: %d\n", metadata.Statistics.NonRegular)
		fmt.Printf("Pathnames: %d\n", metadata.Statistics.Pathnames)
		fmt.Printf("Objects: %d\n", metadata.Statistics.Objects)
		fmt.Printf("Chunks: %d\n", metadata.Statistics.Chunks)
		fmt.Printf("Duration: %s\n", metadata.Statistics.Duration)
		fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(metadata.Size), metadata.Size)
		fmt.Printf("Index Size: %s (%d bytes)\n", humanize.Bytes(metadata.IndexSize), metadata.Size)
	}

	return 0
}

func info_plakar(repository *storage.Repository) int {
	indexes, err := repository.GetIndexes()
	if err != nil {
		logger.Warn("%s", err)
		return 1
	}

	muChunks := sync.Mutex{}
	chunks := make(map[[32]byte]uint16)

	muObjects := sync.Mutex{}
	objects := make(map[[32]byte]uint16)

	errors := 0

	chunksSize := uint64(0)
	dedupedChunksSize := uint64(0)
	objectsSize := uint64(0)
	dedupedObjectsSize := uint64(0)
	for _, indexID := range indexes {
		snap, err := snapshot.Load(repository, indexID)
		if err != nil {
			logger.Warn("%s", err)
			errors++
			continue
		}

		for chunkChecksum := range snap.Index.Chunks {
			muChunks.Lock()
			if _, exists := chunks[chunkChecksum]; !exists {
				chunks[chunkChecksum] = 0
			}
			chunks[chunkChecksum] = chunks[chunkChecksum] + 1
			muChunks.Unlock()
		}

		for objectChecksum := range snap.Index.Objects {
			muObjects.Lock()
			if _, exists := objects[objectChecksum]; !exists {
				objects[objectChecksum] = 0
			}
			objects[objectChecksum] = objects[objectChecksum] + 1
			muObjects.Unlock()
		}
	}

	chunksChecksums, err := repository.GetChunks()
	if err != nil {
		logger.Warn("%s", err)
		errors++
		return 1
	}

	objectsChecksums, err := repository.GetObjects()
	if err != nil {
		logger.Warn("%s", err)
		errors++
		return 1
	}

	for _, checksum := range chunksChecksums {
		if _, exists := chunks[checksum]; !exists {
			errors++
		}
	}

	for _, checksum := range objectsChecksums {
		if _, exists := objects[checksum]; !exists {
			errors++
		}
	}

	for chunkChecksum, count := range chunks {
		refCount, err := repository.GetChunkRefCount(chunkChecksum)
		if err != nil {
			logger.Warn("%s", err)
			errors++
		} else if refCount != uint64(count) {
			errors++
		}
		size, err := repository.GetChunkSize(chunkChecksum)
		if err != nil {
			logger.Warn("%s", err)
			errors++
		} else {
			chunksSize += size
			dedupedChunksSize += (size * uint64(chunks[chunkChecksum]))
		}
	}

	for objectChecksum, count := range objects {
		refCount, err := repository.GetObjectRefCount(objectChecksum)
		if err != nil {
			logger.Warn("%s", err)
			errors++
		} else if refCount != uint64(count) {
			errors++
		}
		size, err := repository.GetObjectSize(objectChecksum)
		if err != nil {
			logger.Warn("%s", err)
			errors++
		} else {
			objectsSize += size
			dedupedObjectsSize += (size * uint64(chunks[objectChecksum]))
		}
	}

	dedupedChunksPercentage := 0.0
	if dedupedChunksSize != 0 {
		dedupedChunksPercentage = float64(dedupedChunksSize-chunksSize) / float64(dedupedChunksSize) * 100
	}
	dedupedObjectsPercentage := 0.0
	if dedupedObjectsSize != 0 {
		dedupedObjectsPercentage = float64(dedupedObjectsSize-objectsSize) / float64(dedupedObjectsSize) * 100
	}

	if repository.Configuration().Encryption != "" {
		fmt.Println("Encryption:", repository.Configuration().Encryption)
	} else {
		fmt.Println("Encryption:", "no")
	}
	if repository.Configuration().Compression != "" {
		fmt.Println("Compression:", repository.Configuration().Compression)
	} else {
		fmt.Println("Compression:", "no")
	}
	fmt.Println("Snapshots:", len(indexes))
	fmt.Printf("Chunks: %d (repository size: %s, real: %s, saved: %.02f%%)\n", len(chunks), humanize.Bytes(chunksSize), humanize.Bytes(dedupedChunksSize), dedupedChunksPercentage)
	fmt.Printf("Objects: %d (repository size: %s, real: %s, saved: %.02f%%)\n", len(objects), humanize.Bytes(objectsSize), humanize.Bytes(dedupedObjectsSize), dedupedObjectsPercentage)

	return 0
}
