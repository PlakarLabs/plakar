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
)

func cmd_tarball(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("tarball", flag.ExitOnError)
	flags.Parse(args)

	if len(flag.Args()) == 0 {
		log.Fatalf("%s: need at least one snapshot ID to pull", flag.CommandLine.Name())
	}

	_, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	snapshots, err := getSnapshots(ctx.Store(), flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	gzipWriter := gzip.NewWriter(os.Stdout)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	for _, snapshot := range snapshots {
		for file, checksum := range snapshot.Pathnames {
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
				data, err := snapshot.GetChunk(chunk.Checksum)
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

	return 0
}
