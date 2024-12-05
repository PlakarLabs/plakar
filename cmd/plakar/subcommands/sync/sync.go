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

package sync

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/encryption"
	"github.com/PlakarKorp/plakar/packfile"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/storage"
)

func init() {
	subcommands.Register("sync", cmd_sync)
}

func cmd_sync(ctx *context.Context, repo *repository.Repository, args []string) int {
	logger := ctx.Logger

	flags := flag.NewFlagSet("sync", flag.ExitOnError)
	flags.Parse(args)

	syncSnapshotID := ""
	direction := ""
	peerRepositoryPath := ""
	switch flags.NArg() {
	case 2:
		direction = flags.Arg(0)
		peerRepositoryPath = flags.Arg(1)

	case 3:
		syncSnapshotID = flags.Arg(0)
		direction = flags.Arg(1)
		peerRepositoryPath = flags.Arg(2)

	default:
		logger.Error("usage: %s [snapshotID] to|from repository", flags.Name())
		return 1
	}

	peerStore, err := storage.Open(ctx, peerRepositoryPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not open repository: %s\n", peerRepositoryPath, err)
		return 1
	}

	var peerSecret []byte
	if peerStore.Configuration().Encryption != nil {
		for {
			passphrase, err := utils.GetPassphrase("destination repository")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			secret, err := encryption.DeriveSecret(passphrase, peerStore.Configuration().Encryption.Key)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			peerSecret = secret
			break
		}
	}
	peerRepository, err := repository.New(peerStore, peerSecret)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not open repository: %s\n", peerStore.Location(), err)
		return 1
	}

	var srcRepository *repository.Repository
	var dstRepository *repository.Repository

	if direction == "to" {
		srcRepository = repo
		dstRepository = peerRepository
	} else if direction == "from" {
		srcRepository = peerRepository
		dstRepository = repo
	} else if direction == "with" {
		srcRepository = repo
		dstRepository = peerRepository
	} else {
		fmt.Fprintf(os.Stderr, "%s: invalid direction, must be to, from or with\n", peerStore.Location())
		return 1
	}

	srcSnapshots, err := srcRepository.GetSnapshots()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get snapshots from repository: %s\n", srcRepository.Location(), err)
		return 1
	}

	dstSnapshots, err := dstRepository.GetSnapshots()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: could not get snapshots list from repository: %s\n", dstRepository.Location(), err)
		return 1
	}

	_ = syncSnapshotID

	srcSnapshotsMap := make(map[[32]byte]struct{})
	dstSnapshotsMap := make(map[[32]byte]struct{})

	for _, snapshotID := range srcSnapshots {
		srcSnapshotsMap[snapshotID] = struct{}{}
	}

	for _, snapshotID := range dstSnapshots {
		dstSnapshotsMap[snapshotID] = struct{}{}
	}

	srcSyncList := make([][32]byte, 0)
	for snapshotID := range srcSnapshotsMap {
		if syncSnapshotID != "" {
			hexSnapshotID := hex.EncodeToString(snapshotID[:])
			if !strings.HasPrefix(hexSnapshotID, syncSnapshotID) {
				continue
			}
		}
		if _, exists := dstSnapshotsMap[snapshotID]; !exists {
			srcSyncList = append(srcSyncList, snapshotID)
		}
	}

	fmt.Printf("Synchronizing %d snapshots\n", len(srcSyncList))

	for _, snapshotID := range srcSyncList {
		err := synchronize(srcRepository, dstRepository, snapshotID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not synchronize snapshot %x from repository: %s\n", srcRepository.Location(), snapshotID, err)
		}
	}

	if direction == "with" {
		dstSyncList := make([][32]byte, 0)
		for snapshotID := range dstSnapshotsMap {
			if syncSnapshotID != "" {
				hexSnapshotID := hex.EncodeToString(snapshotID[:])
				if !strings.HasPrefix(hexSnapshotID, syncSnapshotID) {
					continue
				}
			}
			if _, exists := srcSnapshotsMap[snapshotID]; !exists {
				dstSyncList = append(dstSyncList, snapshotID)
			}
		}

		for _, snapshotID := range dstSyncList {
			err := synchronize(dstRepository, srcRepository, snapshotID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: could not synchronize snapshot %x from repository: %s\n", dstRepository.Location(), snapshotID, err)
			}
		}
	}

	return 0
}

func synchronize(srcRepository *repository.Repository, dstRepository *repository.Repository, snapshotID [32]byte) error {
	srcSnapshot, err := snapshot.Load(srcRepository, snapshotID)
	if err != nil {
		return err
	}

	dstSnapshot, err := snapshot.Clone(dstRepository, snapshotID)
	if err != nil {
		return err
	}

	dstSnapshot.Header = srcSnapshot.Header

	c, err := srcSnapshot.ListChunks()
	if err != nil {
		return err
	}
	for chunkID := range c {
		if !dstRepository.BlobExists(packfile.TYPE_CHUNK, chunkID) {
			chunkData, err := srcSnapshot.GetBlob(packfile.TYPE_CHUNK, chunkID)
			if err != nil {
				return err
			}
			dstSnapshot.PutBlob(packfile.TYPE_CHUNK, chunkID, chunkData)
		}
	}

	c, err = srcSnapshot.ListObjects()
	if err != nil {
		return err
	}
	for objectID := range c {
		if !dstRepository.BlobExists(packfile.TYPE_OBJECT, objectID) {
			objectData, err := srcSnapshot.GetBlob(packfile.TYPE_OBJECT, objectID)
			if err != nil {
				return err
			}
			dstSnapshot.PutBlob(packfile.TYPE_OBJECT, objectID, objectData)
		}
	}

	c, err = srcSnapshot.ListFiles()
	if err != nil {
		return err
	}
	for fileID := range c {
		if !dstRepository.BlobExists(packfile.TYPE_FILE, fileID) {
			fileData, err := srcSnapshot.GetBlob(packfile.TYPE_FILE, fileID)
			if err != nil {
				return err
			}
			dstSnapshot.PutBlob(packfile.TYPE_FILE, fileID, fileData)
		}
	}

	c, err = srcSnapshot.ListDirectories()
	if err != nil {
		return err
	}
	for directoryID := range c {
		if !dstRepository.BlobExists(packfile.TYPE_DIRECTORY, directoryID) {
			directoryData, err := srcSnapshot.GetBlob(packfile.TYPE_DIRECTORY, directoryID)
			if err != nil {
				return err
			}
			dstSnapshot.PutBlob(packfile.TYPE_DIRECTORY, directoryID, directoryData)
		}
	}

	c, err = srcSnapshot.ListDatas()
	if err != nil {
		return err
	}
	for dataID := range c {
		if !dstRepository.BlobExists(packfile.TYPE_DATA, dataID) {
			dataData, err := srcSnapshot.GetBlob(packfile.TYPE_DATA, dataID)
			if err != nil {
				return err
			}
			dstSnapshot.PutBlob(packfile.TYPE_DATA, dataID, dataData)
		}
	}

	return dstSnapshot.Commit()
}
