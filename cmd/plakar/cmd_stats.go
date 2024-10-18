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

	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot/statistics"
	"github.com/dustin/go-humanize"
)

func init() {
	registerCommand("stats", cmd_stats)
}

func printCompressionRatio(size, transferSize uint64, label string) {
	if size > 0 {
		deltaSize := size - transferSize
		ratio := float64(transferSize) / float64(size)
		savings := 100 - (ratio * 100)
		fmt.Printf("%sCompressionRatio: %s (%d bytes), saved: %.2f%%\n",
			label, humanize.Bytes(deltaSize), deltaSize, savings)
	}
}

func cmd_stats(ctx Plakar, repo *repository.Repository, args []string) int {
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
		packfileID, offset, length, exists := repo.State().GetSubpartForData(header.Statistics)
		if !exists {
			fmt.Println("  Subpart: (not found)")
		}

		blob, err := repo.GetPackfileBlob(packfileID, offset, length)
		if err != nil {
			log.Fatal(err)
		}

		stats, err := statistics.FromBytes(blob)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("ImporterStart:", stats.ImporterStart)
		fmt.Println("ImporterDuration:", stats.ImporterDuration)
		fmt.Println("ImporterRecords:", stats.ImporterRecords)
		fmt.Println("ImporterFiles:", stats.ImporterFiles)
		fmt.Println("ImporterDirectories:", stats.ImporterDirectories)
		fmt.Println("ImporterSymlinks:", stats.ImporterSymlinks)
		fmt.Println("ImporterDevices:", stats.ImporterDevices)
		fmt.Println("ImporterPipes:", stats.ImporterPipes)
		fmt.Println("ImporterSockets:", stats.ImporterSockets)
		fmt.Println("ImporterLinks:", stats.ImporterLinks)
		fmt.Printf("ImporterSize: %s (%d bytes)\n", humanize.Bytes(stats.ImporterSize), stats.ImporterSize)
		fmt.Println("ImporterErrors:", stats.ImporterErrors)

		fmt.Println("ScannerStart:", stats.ScannerStart)
		fmt.Println("ScannerDuration:", stats.ScannerDuration)
		fmt.Printf("ScannerProcessedSize: %s (%d bytes)\n", humanize.Bytes(stats.ScannerProcessedSize), stats.ScannerProcessedSize)

		fmt.Println("ChunkerFiles:", stats.ChunkerFiles)
		fmt.Println("ChunkerChunks:", stats.ChunkerChunks)
		fmt.Println("ChunkerObjects:", stats.ChunkerObjects)
		fmt.Printf("ChunkerSize: %s (%d bytes)\n", humanize.Bytes(stats.ChunkerSize), stats.ChunkerSize)
		fmt.Println("ChunkerErrors:", stats.ChunkerErrors)

		fmt.Println("ChunksCount:", stats.ChunksCount)
		fmt.Printf("ChunksSize: %s (%d bytes)\n", humanize.Bytes(stats.ChunksSize), stats.ChunksSize)
		fmt.Println("ChunksTransferCount:", stats.ChunksTransferCount)
		fmt.Printf("ChunksTransferSize: %s (%d bytes)\n", humanize.Bytes(stats.ChunksTransferSize), stats.ChunksTransferSize)
		printCompressionRatio(stats.ChunksSize, stats.ChunksTransferSize, "Chunks")

		fmt.Println("ObjectsCount:", stats.ObjectsCount)
		fmt.Printf("ObjectsSize: %s (%d bytes)\n", humanize.Bytes(stats.ObjectsSize), stats.ObjectsSize)
		fmt.Println("ObjectsTransferCount:", stats.ObjectsTransferCount)
		fmt.Printf("ObjectsTransferSize: %s (%d bytes)\n", humanize.Bytes(stats.ObjectsTransferSize), stats.ObjectsTransferSize)
		printCompressionRatio(stats.ObjectsSize, stats.ObjectsTransferSize, "Objects")

		fmt.Println("DataCount:", stats.DataCount)
		fmt.Printf("DataSize: %s (%d bytes)\n", humanize.Bytes(stats.DataSize), stats.DataSize)
		fmt.Println("DataTransferCount:", stats.DataTransferCount)
		fmt.Printf("DataTransferSize: %s (%d bytes)\n", humanize.Bytes(stats.DataTransferSize), stats.DataTransferSize)
		printCompressionRatio(stats.DataSize, stats.DataTransferSize, "Data")

		fmt.Println("VFSFilesCount:", stats.VFSFilesCount)
		fmt.Printf("VFSFilesSize: %s (%d bytes)\n", humanize.Bytes(stats.VFSFilesSize), stats.VFSFilesSize)
		fmt.Println("VFSFilesTransferCount:", stats.VFSFilesTransferCount)
		fmt.Printf("VFSFilesTransferSize: %s (%d bytes)\n", humanize.Bytes(stats.VFSFilesTransferSize), stats.VFSFilesTransferSize)
		printCompressionRatio(stats.VFSFilesSize, stats.VFSFilesTransferSize, "VFSFiles")

		fmt.Println("VFSDirectoriesCount:", stats.VFSDirectoriesCount)
		fmt.Printf("VFSDirectoriesSize: %s (%d bytes)\n", humanize.Bytes(stats.VFSDirectoriesSize), stats.VFSDirectoriesSize)
		fmt.Println("VFSDirectoriesTransferCount:", stats.VFSDirectoriesTransferCount)
		fmt.Printf("VFSDirectoriesTransferSize: %s (%d bytes)\n", humanize.Bytes(stats.VFSDirectoriesTransferSize), stats.VFSDirectoriesTransferSize)
		printCompressionRatio(stats.VFSDirectoriesSize, stats.VFSDirectoriesTransferSize, "VFSDirectories")

		fmt.Println("PackfilesCount:", stats.PackfilesCount)
		fmt.Printf("PackfilesSize: %s (%d bytes)\n", humanize.Bytes(stats.PackfilesSize), stats.PackfilesSize)
		fmt.Println("PackfilesTransferCount:", stats.PackfilesTransferCount)
		fmt.Printf("PackfilesTransferSize: %s (%d bytes)\n", humanize.Bytes(stats.PackfilesTransferSize), stats.PackfilesTransferSize)
		printCompressionRatio(stats.PackfilesSize, stats.PackfilesTransferSize, "Packfiles")
	}

	return 0
}
