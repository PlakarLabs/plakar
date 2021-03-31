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
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/poolpOrg/plakar/repository"
)

func cmd_tarball(store repository.Store, args []string) {
	if len(args) == 0 {
		log.Fatalf("%s: need at least one snapshot ID to pull", flag.CommandLine.Name())
	}

	_, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
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

	gzipWriter := gzip.NewWriter(os.Stdout)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	for i := 0; i < len(args); i++ {
		prefix, _ := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		snapshot, err := store.Snapshot(res[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
		}

		for file, checksum := range snapshot.Sums {
			info := snapshot.Files[file]
			header := &tar.Header{
				Name:    file,
				Size:    info.Size,
				Mode:    int64(info.Mode),
				ModTime: info.ModTime,
			}

			err = tarWriter.WriteHeader(header)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not write header for file %s\n", file)
				continue
			}

			obj := snapshot.Objects[checksum]
			for _, chunk := range obj.Chunks {
				data, err := snapshot.ChunkGet(chunk.Checksum)
				if err != nil {
					fmt.Fprintf(os.Stderr, "corrupted file %s\n", file)
					continue
				}

				_, err = io.WriteString(tarWriter, string(data))
				if err != nil {
					fmt.Fprintf(os.Stderr, "could not write file %s\n", file)
					continue
				}
			}

		}
	}
}
