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
	"os"
	"sync"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"
)

func init() {
	registerCommand("clone", cmd_clone)
}

func cmd_clone(ctx Plakar, repository *storage.Store, args []string) int {
	flags := flag.NewFlagSet("clone", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() != 2 || flags.Arg(0) != "to" {
		logger.Error("usage: %s to repository", flags.Name())
		return 1
	}

	sourceRepository := repository
	repositoryConfig := sourceRepository.Configuration()

	cloneRepository, err := storage.Create(flags.Arg(1), repositoryConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not create repository: %s\n", flags.Arg(1), err)
		return 1
	}

	packfileChecksums, err := sourceRepository.GetPackfiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get paclfiles list from repository: %s\n", sourceRepository.Location, err)
		return 1
	}

	wg := sync.WaitGroup{}
	for _, _packfileChecksum := range packfileChecksums {
		wg.Add(1)
		go func(packfileChecksum [32]byte) {
			defer wg.Done()

			data, err := sourceRepository.GetPackfile(packfileChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get packfile from repository: %s\n", sourceRepository.Location, err)
				return
			}

			err = cloneRepository.PutPackfile(packfileChecksum, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not put packfile to repository: %s\n", cloneRepository.Location, err)
				return
			}
		}(_packfileChecksum)
	}
	wg.Wait()

	indexesChecksums, err := sourceRepository.GetStates()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get paclfiles list from repository: %s\n", sourceRepository.Location, err)
		return 1
	}

	wg = sync.WaitGroup{}
	for _, _indexChecksum := range indexesChecksums {
		wg.Add(1)
		go func(indexChecksum [32]byte) {
			defer wg.Done()

			data, err := sourceRepository.GetState(indexChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get index from repository: %s\n", sourceRepository.Location, err)
				return
			}

			err = cloneRepository.PutState(indexChecksum, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not put packfile to repository: %s\n", cloneRepository.Location, err)
				return
			}
		}(_indexChecksum)
	}
	wg.Wait()

	wg = sync.WaitGroup{}
	blobsChecksums, err := sourceRepository.GetBlobs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get blobs list from repository: %s\n", sourceRepository.Location, err)
		return 1
	}
	for _, _blobChecksum := range blobsChecksums {
		wg.Add(1)
		go func(blobChecksum [32]byte) {
			defer wg.Done()

			data, err := sourceRepository.GetBlob(blobChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get blob from repository: %s\n", sourceRepository.Location, err)
				return
			}

			err = cloneRepository.PutBlob(blobChecksum, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not put blob to repository: %s\n", cloneRepository.Location, err)
				return
			}
		}(_blobChecksum)
	}
	wg.Wait()

	wg = sync.WaitGroup{}
	snapshots, err := sourceRepository.GetSnapshots()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get snapshots list from repository: %s\n", sourceRepository.Location, err)
		return 1
	}
	for _, _snapshotID := range snapshots {
		wg.Add(1)
		go func(snapshotID uuid.UUID) {
			defer wg.Done()

			data, err := sourceRepository.GetSnapshot(snapshotID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not get snapshot from repository: %s\n", sourceRepository.Location, err)
				return
			}

			err = cloneRepository.PutSnapshot(snapshotID, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not put snapshot to repository: %s\n", cloneRepository.Location, err)
				return
			}
		}(_snapshotID)
	}
	wg.Wait()

	return 0
}
