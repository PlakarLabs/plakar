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
	"github.com/PlakarLabs/plakar/repository"
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

func cmd_info(ctx Plakar, repo *repository.Repository, args []string) int {
	if len(args) == 0 {
		return info_plakar(repo)
	}

	flags := flag.NewFlagSet("info", flag.ExitOnError)
	flags.Parse(args)

	metadatas, err := getHeaders(repo, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for _, metadata := range metadatas {
		fmt.Printf("IndexID: %s\n", metadata.GetIndexID())
		fmt.Printf("CreationTime: %s\n", metadata.CreationTime)
		fmt.Printf("CreationDuration: %s\n", metadata.CreationDuration)

		fmt.Printf("Version: %s\n", repo.Configuration().Version)
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

		fmt.Printf("Index.Version: %s\n", metadata.Index.Version)
		fmt.Printf("Index.Checksum: %064x\n", metadata.Index.Checksum)
		fmt.Printf("Index.Size: %s (%d bytes)\n", humanize.Bytes(metadata.Index.Size), metadata.Index.Size)

		fmt.Printf("VFS.Version: %s\n", metadata.VFS.Version)
		fmt.Printf("VFS.Checksum: %064x\n", metadata.VFS.Checksum)
		fmt.Printf("VFS.Size: %s (%d bytes)\n", humanize.Bytes(metadata.VFS.Size), metadata.VFS.Size)

		fmt.Printf("Metadata.Version: %s\n", metadata.Metadata.Version)
		fmt.Printf("Metadata.Checksum: %064x\n", metadata.Metadata.Checksum)
		fmt.Printf("Metadata.Size: %s (%d bytes)\n", humanize.Bytes(metadata.Metadata.Size), metadata.Metadata.Size)
	}

	return 0
}

func info_plakar(repo *repository.Repository) int {
	metadatas, err := getHeaders(repo, nil)
	if err != nil {
		logger.Warn("%s", err)
		return 1
	}

	fmt.Println("StoreID:", repo.Configuration().StoreID)
	fmt.Printf("CreationTime: %s\n", repo.Configuration().CreationTime)
	fmt.Println("Version:", repo.Configuration().Version)

	if repo.Configuration().Encryption != "" {
		fmt.Println("Encryption:", repo.Configuration().Encryption)
		fmt.Println("EncryptionKey:", repo.Configuration().EncryptionKey)
	} else {
		fmt.Println("Encryption:", "no")
	}

	if repo.Configuration().Compression != "" {
		fmt.Println("Compression:", repo.Configuration().Compression)
	} else {
		fmt.Println("Compression:", "no")
	}

	fmt.Println("Hashing:", repo.Configuration().Hashing)

	fmt.Println("Chunking:", repo.Configuration().Chunking)
	fmt.Printf("ChunkingMin: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().ChunkingMin)), repo.Configuration().ChunkingMin)
	fmt.Printf("ChunkingNormal: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().ChunkingNormal)), repo.Configuration().ChunkingNormal)
	fmt.Printf("ChunkingMax: %s (%d bytes)\n",
		humanize.Bytes(uint64(repo.Configuration().ChunkingMax)), repo.Configuration().ChunkingMax)

	fmt.Println("Snapshots:", len(metadatas))
	totalSize := uint64(0)
	totalIndexSize := uint64(0)
	totalFilesystemSize := uint64(0)
	totalMetadataSize := uint64(0)
	for _, metadata := range metadatas {
		totalSize += metadata.ScanProcessedSize
		totalIndexSize += metadata.Index.Size
		totalFilesystemSize += metadata.VFS.Size
		totalMetadataSize += metadata.Metadata.Size
	}
	fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(totalSize), totalSize)
	fmt.Printf("Index Size: %s (%d bytes)\n", humanize.Bytes(totalIndexSize), totalIndexSize)
	fmt.Printf("Filesystem Size: %s (%d bytes)\n", humanize.Bytes(totalFilesystemSize), totalFilesystemSize)
	fmt.Printf("Metadata Size: %s (%d bytes)\n", humanize.Bytes(totalMetadataSize), totalMetadataSize)

	return 0
}
