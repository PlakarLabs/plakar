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
	"crypto/sha256"
	"flag"
	"fmt"
	"log"

	"github.com/poolpOrg/plakar/logger"
)

func cmd_check(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("check", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameter is required", flags.Name())
		return 1
	}

	snapshots, err := getSnapshots(ctx.Store(), flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	failures := false
	for offset, snapshot := range snapshots {
		_, pattern := parseSnapshotID(args[offset])

		snapshotOk := true
		if pattern != "" {
			checksum, ok := snapshot.Pathnames[pattern]
			if !ok {
				logger.Warn("%s: unlisted file %s", snapshot.Uuid, pattern)
				snapshotOk = false
				continue
			}
			object, ok := snapshot.Objects[checksum]
			if !ok {
				logger.Warn("%s: unlisted object %s", snapshot.Uuid, checksum)
				snapshotOk = false
				continue
			}

			objectHash := sha256.New()
			for _, chunk := range object.Chunks {
				data, err := snapshot.GetChunk(chunk.Checksum)
				if err != nil {
					logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
					snapshotOk = false
					continue
				}
				objectHash.Write(data)
			}
			if fmt.Sprintf("%032x", objectHash.Sum(nil)) != checksum {
				logger.Warn("%s: corrupted object %s", snapshot.Uuid, checksum)
				snapshotOk = false
				continue
			}

		} else {

			for _, chunk := range snapshot.Chunks {

				data, err := snapshot.GetChunk(chunk.Checksum)
				if err != nil {
					logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
					snapshotOk = false
					continue
				}
				chunkHash := sha256.New()
				chunkHash.Write(data)
				if fmt.Sprintf("%032x", chunkHash.Sum(nil)) != chunk.Checksum {
					logger.Warn("%s: corrupted chunk %s", snapshot.Uuid, chunk.Checksum)
					snapshotOk = false
					continue
				}

			}

			for checksum := range snapshot.Objects {
				object, err := snapshot.GetObject(checksum)
				if err != nil {
					logger.Warn("%s: missing object %s", snapshot.Uuid, checksum)
					snapshotOk = false
					continue
				}
				objectHash := sha256.New()

				for _, chunk := range object.Chunks {
					_, ok := snapshot.Chunks[chunk.Checksum]
					if !ok {
						logger.Warn("%s: unlisted chunk %s", snapshot.Uuid, chunk.Checksum)
						snapshotOk = false
						continue
					}

					data, err := snapshot.GetChunk(chunk.Checksum)
					if err != nil {
						logger.Warn("%s: missing chunk %s", snapshot.Uuid, chunk.Checksum)
						snapshotOk = false
						continue
					}
					objectHash.Write(data)
				}
				if fmt.Sprintf("%032x", objectHash.Sum(nil)) != checksum {
					logger.Warn("%s: corrupted object %s", snapshot.Uuid, checksum)
					snapshotOk = false
					continue
				}
			}

			for file := range snapshot.Files {
				checksum, ok := snapshot.Pathnames[file]
				if !ok {
					logger.Warn("%s: unlisted file %s", snapshot.Uuid, file)
					snapshotOk = false
					continue
				}
				_, ok = snapshot.Objects[checksum]
				if !ok {
					logger.Warn("%s: unlisted object %s", snapshot.Uuid, checksum)
					snapshotOk = false
					continue
				}
			}
		}

		key := snapshot.Uuid
		if pattern != "" {
			key = fmt.Sprintf("%s:%s", snapshot.Uuid, pattern)
		}
		_ = key

		if snapshotOk {
			logger.Info("%s: OK", snapshot.Uuid)
		} else {
			logger.Info("%s: KO", snapshot.Uuid)
			failures = true
		}
	}

	if failures {
		return 1
	}
	return 0
}
