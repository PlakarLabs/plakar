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
	"os"

	"github.com/poolpOrg/plakar/repository"
)

func cmd_check(store repository.Store, args []string) {
	if len(args) == 0 {
		log.Fatalf("%s: need at least one snapshot ID to check", flag.CommandLine.Name())
	}

	snapshots, err := store.Snapshots()
	if err != nil {
		log.Fatalf("%s: could not fetch snapshots list", flag.CommandLine.Name())
	}

	for i := 0; i < len(args); i++ {
		prefix, _ := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
	}

	for i := 0; i < len(args); i++ {
		unlistedChunk := make([]string, 0)
		missingChunks := make([]string, 0)
		corruptedChunks := make([]string, 0)
		unlistedObject := make([]string, 0)
		missingObjects := make([]string, 0)
		corruptedObjects := make([]string, 0)
		unlistedFile := make([]string, 0)

		prefix, pattern := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		snapshot, err := store.Snapshot(res[0])
		if err != nil {
			fmt.Println(err)
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
		}

		if pattern != "" {
			checksum, ok := snapshot.Sums[pattern]
			if !ok {
				unlistedFile = append(unlistedFile, pattern)
				continue
			}
			object, ok := snapshot.Objects[checksum]
			if !ok {
				unlistedObject = append(unlistedObject, checksum)
				continue
			}

			objectHash := sha256.New()
			for _, chunk := range object.Chunks {
				data, err := snapshot.ChunkGet(chunk.Checksum)
				if err != nil {
					missingChunks = append(missingChunks, chunk.Checksum)
					continue
				}
				objectHash.Write(data)
			}
			if fmt.Sprintf("%032x", objectHash.Sum(nil)) != checksum {
				corruptedObjects = append(corruptedObjects, checksum)
				continue
			}

		} else {

			cCount := 0
			for _, chunk := range snapshot.Chunks {
				data, err := snapshot.ChunkGet(chunk.Checksum)
				if err != nil {
					missingChunks = append(missingChunks, chunk.Checksum)
					continue
				}
				chunkHash := sha256.New()
				chunkHash.Write(data)
				if fmt.Sprintf("%032x", chunkHash.Sum(nil)) != chunk.Checksum {
					corruptedChunks = append(corruptedChunks, chunk.Checksum)
					continue
				}
				cCount += 1

				if !quiet {
					fmt.Fprintf(os.Stdout, "\r%s: chunks: %2d%%",
						snapshot.Uuid,
						(cCount*100)/len(snapshot.Chunks))
				}
			}

			oCount := 0
			for checksum := range snapshot.Objects {
				object, err := snapshot.ObjectGet(checksum)
				if err != nil {
					missingObjects = append(missingObjects, checksum)
					continue
				}
				objectHash := sha256.New()

				for _, chunk := range object.Chunks {
					_, ok := snapshot.Chunks[chunk.Checksum]
					if !ok {
						unlistedChunk = append(unlistedChunk, chunk.Checksum)
						continue
					}

					data, err := snapshot.ChunkGet(chunk.Checksum)
					if err != nil {
						missingChunks = append(missingChunks, chunk.Checksum)
						continue
					}
					objectHash.Write(data)
				}
				if fmt.Sprintf("%032x", objectHash.Sum(nil)) != checksum {
					corruptedObjects = append(corruptedObjects, checksum)
					continue
				}

				oCount += 1
				if !quiet {
					fmt.Fprintf(os.Stdout, "\r%s: objects: %2d%%",
						snapshot.Uuid,
						(oCount*100)/len(snapshot.Objects))
				}
			}

			fCount := 0
			for file := range snapshot.Files {
				checksum, ok := snapshot.Sums[file]
				if !ok {
					unlistedFile = append(unlistedFile, file)
					continue
				}
				_, ok = snapshot.Objects[checksum]
				if !ok {
					unlistedObject = append(unlistedObject, checksum)
					continue
				}

				fCount += 1
				if !quiet {
					fmt.Fprintf(os.Stdout, "\r%s: files: %2d%%  ",
						snapshot.Uuid,
						(fCount*100)/len(snapshot.Files))
				}
			}
		}

		if !quiet {
			errors := 0
			errors += len(missingChunks)
			errors += len(corruptedChunks)
			errors += len(missingObjects)
			errors += len(corruptedObjects)
			errors += len(unlistedChunk)
			errors += len(unlistedFile)

			key := snapshot.Uuid
			if pattern != "" {
				key = fmt.Sprintf("%s:%s", snapshot.Uuid, pattern)
			}

			if errors == 0 {
				fmt.Fprintf(os.Stdout, "\r%s: OK         \n", key)
			} else {
				fmt.Fprintf(os.Stdout, "\r%s: KO         \n", key)
			}
		}
	}
}
