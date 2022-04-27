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

	"github.com/dustin/go-humanize"
	"github.com/poolpOrg/plakar/logger"
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
		fmt.Printf("ShortID: %s\n", metadata.GetIndexShortID())
		fmt.Printf("IndexID: %s\n", metadata.GetIndexID())
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
	metadatas, err := getMetadatas(repository, nil)
	if err != nil {
		logger.Warn("%s", err)
		return 1
	}

	fmt.Println("RepositoryID:", repository.Configuration().RepositoryID)
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
	for _, metadata := range metadatas {
		totalSize += metadata.Size
		totalIndexSize += metadata.IndexSize
	}
	fmt.Printf("Size: %s (%d bytes)\n", humanize.Bytes(totalSize), totalSize)
	fmt.Printf("Index Size: %s (%d bytes)\n", humanize.Bytes(totalIndexSize), totalIndexSize)

	return 0
}
