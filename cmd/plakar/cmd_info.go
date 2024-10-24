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
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/dustin/go-humanize"
)

func init() {
	registerCommand("info", cmd_info)
}

func cmd_info(ctx *context.Context, repo *repository.Repository, args []string) int {
	if len(args) == 0 {
		return info_plakar(repo)
	}

	flags := flag.NewFlagSet("info", flag.ExitOnError)
	flags.Parse(args)

	headers, err := getHeaders(repo, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for _, header := range headers {
		indexID := header.GetIndexID()
		fmt.Printf("IndexID: %s\n", hex.EncodeToString(indexID[:]))
		fmt.Printf("CreationTime: %s\n", header.CreationTime)
		fmt.Printf("CreationDuration: %s\n", header.CreationDuration)

		fmt.Printf("Root: %x\n", header.Root)
		fmt.Printf("Metadata: %x\n", header.Metadata)
		fmt.Printf("Statistics: %x\n", header.Statistics)

		fmt.Printf("Version: %s\n", repo.Configuration().Version)
		fmt.Printf("Hostname: %s\n", header.Hostname)
		fmt.Printf("Username: %s\n", header.Username)
		fmt.Printf("CommandLine: %s\n", header.CommandLine)
		fmt.Printf("OperatingSystem: %s\n", header.OperatingSystem)
		fmt.Printf("Architecture: %s\n", header.Architecture)
		fmt.Printf("NumCPU: %d\n", header.NumCPU)

		fmt.Printf("MachineID: %s\n", header.MachineID)
		fmt.Printf("PublicKey: %s\n", header.PublicKey)
		fmt.Printf("Tags: %s\n", strings.Join(header.Tags, ", "))
		fmt.Printf("Directories: %d\n", header.DirectoriesCount)
		fmt.Printf("Files: %d\n", header.FilesCount)

		fmt.Printf("Snapshot.Size: %s (%d bytes)\n", humanize.Bytes(header.ScanProcessedSize), header.ScanProcessedSize)
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
	totalMetadataSize := uint64(0)
	for _, metadata := range metadatas {
		totalSize += metadata.ScanProcessedSize
	}
	fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(totalSize), totalSize)
	fmt.Printf("Index Size: %s (%d bytes)\n", humanize.Bytes(totalIndexSize), totalIndexSize)
	fmt.Printf("Metadata Size: %s (%d bytes)\n", humanize.Bytes(totalMetadataSize), totalMetadataSize)

	return 0
}
