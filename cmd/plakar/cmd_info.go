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

	"github.com/dustin/go-humanize"
	"github.com/poolpOrg/plakar/logger"
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
	registerCommand("info", cmd_info)
}

func cmd_info(ctx Plakar, repository *storage.Repository, args []string) int {
	if len(args) == 0 {
		return info_plakar(repository)
	}

	var opt_metadata bool
	var opt_index bool
	var opt_filesystem bool

	flags := flag.NewFlagSet("info", flag.ExitOnError)
	flags.BoolVar(&opt_metadata, "metadata", false, "display metadata")
	flags.BoolVar(&opt_index, "index", false, "display index")
	flags.BoolVar(&opt_filesystem, "filesystem", false, "display filesystem")
	flags.Parse(args)

	if opt_metadata {
		metadatas, err := getMetadatas(repository, flags.Args())
		if err != nil {
			log.Fatal(err)
		}

		for _, metadata := range metadatas {
			serialized, err := json.Marshal(metadata)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(string(serialized))
		}
	}

	if opt_index {
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
	}

	if opt_filesystem {
		filesystems, err := getFilesystems(repository, flags.Args())
		if err != nil {
			log.Fatal(err)
		}

		for _, filesystem := range filesystems {
			serialized, err := json.Marshal(filesystem)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(string(serialized))
		}
	}

	if !opt_metadata && !opt_index && !opt_filesystem {
		metadatas, err := getMetadatas(repository, flags.Args())
		if err != nil {
			log.Fatal(err)
		}

		for _, metadata := range metadatas {
			fmt.Printf("IndexID: %s\n", metadata.GetIndexID())
			fmt.Printf("CreationTime: %s\n", metadata.CreationTime)
			fmt.Printf("CreationDuration: %s\n", metadata.CreationDuration)

			fmt.Printf("Version: %s\n", repository.Configuration().Version)
			fmt.Printf("Hostname: %s\n", metadata.Hostname)
			fmt.Printf("Username: %s\n", metadata.Username)
			fmt.Printf("CommandLine: %s\n", metadata.CommandLine)
			fmt.Printf("OperatingSystem: %s\n", metadata.OperatingSystem)
			fmt.Printf("MachineID: %s\n", metadata.MachineID)
			fmt.Printf("PublicKey: %s\n", metadata.PublicKey)
			fmt.Printf("Directories: %d\n", metadata.DirectoriesCount)
			fmt.Printf("Files: %d\n", metadata.FilesCount)
			fmt.Printf("NonRegular: %d\n", metadata.NonRegularCount)
			fmt.Printf("Pathnames: %d\n", metadata.PathnamesCount)

			fmt.Printf("Objects: %d\n", metadata.ObjectsCount)
			fmt.Printf("ObjectsTransferCount: %d\n", metadata.ObjectsTransferCount)
			fmt.Printf("ObjectsTransferSize: %s (%d bytes)\n", humanize.Bytes(metadata.ObjectsTransferSize), metadata.ObjectsTransferSize)

			fmt.Printf("Chunks: %d\n", metadata.ChunksCount)
			fmt.Printf("ChunksSize: %d\n", metadata.ChunksSize)
			fmt.Printf("ChunksTransferCount: %d\n", metadata.ChunksTransferCount)
			fmt.Printf("ChunksTransferSize: %s (%d bytes)\n", humanize.Bytes(metadata.ChunksTransferSize), metadata.ChunksTransferSize)

			fmt.Printf("SnapshotSize: %s (%d bytes)\n", humanize.Bytes(metadata.ScanProcessedSize), metadata.ScanProcessedSize)

			fmt.Printf("MappingIndexChecksum: %064x\n", metadata.IndexChecksum)
			fmt.Printf("MappingIndexDiskSize: %s (%d bytes)\n", humanize.Bytes(metadata.IndexDiskSize), metadata.IndexDiskSize)
			fmt.Printf("MappingIndexMemorySize: %s (%d bytes)\n", humanize.Bytes(metadata.IndexMemorySize), metadata.IndexMemorySize)

			fmt.Printf("FilesystemIndexChecksum: %064x\n", metadata.FilesystemChecksum)
			fmt.Printf("FilesystemIndexDiskSize: %s (%d bytes)\n", humanize.Bytes(metadata.FilesystemDiskSize), metadata.FilesystemDiskSize)
			fmt.Printf("FilesystemIndexMemorySize: %s (%d bytes)\n", humanize.Bytes(metadata.FilesystemMemorySize), metadata.FilesystemMemorySize)

		}
	}
	return 0
}

func info_plakar(repository *storage.Repository) int {
	metadatas, err := getMetadatas(repository, nil)
	if err != nil {
		logger.Warn("%s", err)
		return 1
	}

	fmt.Println("RepositoryID:", repository.Configuration().RepositoryID)
	fmt.Printf("CreationTime: %s\n", repository.Configuration().CreationTime)
	fmt.Println("Version:", repository.Configuration().Version)

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

	fmt.Println("Snapshots:", len(metadatas))
	totalSize := uint64(0)
	totalIndexSize := uint64(0)
	totalFilesystemSize := uint64(0)
	for _, metadata := range metadatas {
		totalSize += metadata.ScanProcessedSize
		totalIndexSize += metadata.IndexDiskSize
		totalFilesystemSize += metadata.FilesystemDiskSize

	}
	fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(totalSize), totalSize)
	fmt.Printf("Index Size: %s (%d bytes)\n", humanize.Bytes(totalIndexSize), totalIndexSize)
	fmt.Printf("Filesystem Size: %s (%d bytes)\n", humanize.Bytes(totalFilesystemSize), totalFilesystemSize)

	return 0
}
