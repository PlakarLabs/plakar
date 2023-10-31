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
	"strings"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/dustin/go-humanize"
)

type JSONChunk struct {
	Length uint32
}

type JSONObject struct {
	Chunks      []uint32
	ContentType uint32
}

type JSONIndex struct {

	// Pathnames -> Object checksum
	Pathnames map[uint32]uint64

	ContentTypes map[string]uint32

	// Object checksum -> Object
	Objects map[uint32]*JSONObject

	// Chunk checksum -> Chunk
	Chunks map[uint32]*JSONChunk

	// Chunk checksum -> Object checksums
	ChunkToObjects map[uint32][]uint32

	// Object checksum -> Filenames
	ObjectToPathnames map[uint32][]uint32

	// Content Type -> Object checksums
	ContentTypeToObjects map[uint32][]uint32
}

func init() {
	registerCommand("info", cmd_info)
}

func cmd_info(ctx Plakar, repository *storage.Repository, args []string) int {
	if len(args) == 0 {
		return info_plakar(repository)
	}

	flags := flag.NewFlagSet("info", flag.ExitOnError)

	metadatas, err := getHeaders(repository, flags.Args())
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
		fmt.Printf("Tags: %s\n", strings.Join(metadata.Tags, ", "))
		fmt.Printf("Directories: %d\n", metadata.DirectoriesCount)
		fmt.Printf("Files: %d\n", metadata.FilesCount)
		fmt.Printf("NonRegular: %d\n", metadata.NonRegularCount)
		fmt.Printf("Pathnames: %d\n", metadata.PathnamesCount)

		fmt.Printf("Objects.Count: %d\n", metadata.ObjectsCount)
		fmt.Printf("Objects.TransferCount: %d\n", metadata.ObjectsTransferCount)
		fmt.Printf("Objects.TransferSize: %s (%d bytes)\n", humanize.Bytes(metadata.ObjectsTransferSize), metadata.ObjectsTransferSize)

		fmt.Printf("Chunks.Count: %d\n", metadata.ChunksCount)
		fmt.Printf("Chunks.Size: %d\n", metadata.ChunksSize)
		fmt.Printf("Chunks,TransferCount: %d\n", metadata.ChunksTransferCount)
		fmt.Printf("Chunks.TransferSize: %s (%d bytes)\n", humanize.Bytes(metadata.ChunksTransferSize), metadata.ChunksTransferSize)

		fmt.Printf("Snapshot.Size: %s (%d bytes)\n", humanize.Bytes(metadata.ScanProcessedSize), metadata.ScanProcessedSize)

		fmt.Printf("Index.Version: %s\n", metadata.IndexVersion)
		fmt.Printf("Index.Checksum: %064x\n", metadata.IndexChecksum)
		fmt.Printf("Index.Size: %s (%d bytes)\n", humanize.Bytes(metadata.IndexSize), metadata.IndexSize)

		fmt.Printf("VFS.Version: %s\n", metadata.FilesystemVersion)
		fmt.Printf("VFS.Checksum: %064x\n", metadata.FilesystemChecksum)
		fmt.Printf("VFS.Size: %s (%d bytes)\n", humanize.Bytes(metadata.FilesystemSize), metadata.FilesystemSize)

		fmt.Printf("Metadata.Version: %s\n", metadata.MetadataVersion)
		fmt.Printf("Metadata.Checksum: %064x\n", metadata.MetadataChecksum)
		fmt.Printf("Metadata.Size: %s (%d bytes)\n", humanize.Bytes(metadata.MetadataSize), metadata.MetadataSize)

	}

	return 0
}

func info_plakar(repository *storage.Repository) int {
	metadatas, err := getHeaders(repository, nil)
	if err != nil {
		logger.Warn("%s", err)
		return 1
	}

	fmt.Println("RepositoryID:", repository.Configuration().RepositoryID)
	fmt.Printf("CreationTime: %s\n", repository.Configuration().CreationTime)
	fmt.Println("Version:", repository.Configuration().Version)

	if repository.Configuration().Encryption != "" {
		fmt.Println("Encryption:", repository.Configuration().Encryption)
		fmt.Println("EncryptionKey:", repository.Configuration().EncryptionKey)
	} else {
		fmt.Println("Encryption:", "no")
	}

	if repository.Configuration().Compression != "" {
		fmt.Println("Compression:", repository.Configuration().Compression)
	} else {
		fmt.Println("Compression:", "no")
	}

	fmt.Println("Hashing:", repository.Configuration().Hashing)

	fmt.Println("Chunking:", repository.Configuration().Chunking)
	fmt.Printf("ChunkingMin: %s (%d bytes)\n",
		humanize.Bytes(uint64(repository.Configuration().ChunkingMin)), repository.Configuration().ChunkingMin)
	fmt.Printf("ChunkingNormal: %s (%d bytes)\n",
		humanize.Bytes(uint64(repository.Configuration().ChunkingNormal)), repository.Configuration().ChunkingNormal)
	fmt.Printf("ChunkingMax: %s (%d bytes)\n",
		humanize.Bytes(uint64(repository.Configuration().ChunkingMax)), repository.Configuration().ChunkingMax)

	fmt.Println("Snapshots:", len(metadatas))
	totalSize := uint64(0)
	totalIndexSize := uint64(0)
	totalFilesystemSize := uint64(0)
	totalMetadataSize := uint64(0)
	for _, metadata := range metadatas {
		totalSize += metadata.ScanProcessedSize
		totalIndexSize += metadata.IndexSize
		totalFilesystemSize += metadata.FilesystemSize
		totalMetadataSize += metadata.MetadataSize
	}
	fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(totalSize), totalSize)
	fmt.Printf("Index Size: %s (%d bytes)\n", humanize.Bytes(totalIndexSize), totalIndexSize)
	fmt.Printf("Filesystem Size: %s (%d bytes)\n", humanize.Bytes(totalFilesystemSize), totalFilesystemSize)

	return 0
}
