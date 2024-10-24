/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
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
	"archive/zip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

func init() {
	registerCommand("zip", cmd_zip)
}
func cmd_zip(ctx *context.Context, repo *repository.Repository, args []string) int {
	var zipPath string
	var zipRebase bool

	flags := flag.NewFlagSet("zip", flag.ExitOnError)
	flags.StringVar(&zipPath, "output", fmt.Sprintf("plakar-%s.zip", time.Now().UTC().Format(time.RFC3339)), "zip pathname")
	flags.BoolVar(&zipRebase, "rebase", false, "strip pathname when pulling")
	flags.Parse(args)

	if flags.NArg() == 0 {
		log.Fatalf("%s: need at least one snapshot ID to pull", flag.CommandLine.Name())
	}

	snapshots, err := getSnapshots(repo, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	zipFile, err := os.Create(zipPath)
	if err != nil {
		log.Fatal(err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	for offset, snapshot := range snapshots {
		_, prefix := parseSnapshotID(flags.Args()[offset])

		fs, err := snapshot.Filesystem()
		if err != nil {
			log.Fatal(err)
			return 1
		}

		for file := range fs.Pathnames() {

			if prefix != "" {
				if !pathIsWithin(file, prefix) {
					continue
				}
			}
			info, _ := fs.Stat(file)
			filepath := file
			if zipRebase {
				filepath = strings.TrimPrefix(filepath, prefix)
			}

			if _, isDir := info.(*vfs.DirEntry); isDir {
				continue
			}

			header, err := zip.FileInfoHeader(info.(*vfs.FileEntry).FileInfo())
			if err != nil {
				log.Printf("could not create header for file %s: %s", file, err)
				continue
			}
			header.Name = filepath
			header.Method = zip.Deflate

			rd, err := snapshot.NewReader(file)
			if err != nil {
				log.Printf("could not find file %s", file)
				continue
			}

			writer, err := zipWriter.CreateHeader(header)
			if err != nil {
				log.Printf("could not create zip entry for file %s: %s", file, err)
				rd.Close()
				continue
			}

			_, err = io.Copy(writer, rd)
			if err != nil {
				log.Printf("could not write file %s: %s", file, err)
				rd.Close()
				continue
			}
			rd.Close()
		}
	}

	log.Printf("created zip %s", zipPath)
	return 0
}
