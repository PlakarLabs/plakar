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

package archive

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
)

func init() {
	subcommands.Register("archive", cmd_archive)
}

func cmd_archive(ctx *context.Context, repo *repository.Repository, args []string) int {

	var opt_rebase bool
	var opt_output string
	var opt_format string

	flags := flag.NewFlagSet("archive", flag.ExitOnError)
	flags.StringVar(&opt_output, "output", "", "archive pathname")
	flags.BoolVar(&opt_rebase, "rebase", false, "strip pathname when pulling")
	flags.StringVar(&opt_format, "format", "tarball", "archive format")
	flags.Parse(args)

	if flags.NArg() == 0 {
		log.Fatalf("%s: need at least one snapshot ID to pull", flag.CommandLine.Name())
	}

	supportedFormats := map[string]string{
		"tar":     ".tar",
		"tarball": ".tar.gz",
		"zip":     ".zip",
	}
	if _, ok := supportedFormats[opt_format]; !ok {
		log.Fatalf("%s: unsupported format %s", flag.CommandLine.Name(), opt_format)
	}

	snapshotPrefix, pathname := utils.ParseSnapshotID(flags.Arg(0))
	snap, err := utils.OpenSnapshotByPrefix(repo, snapshotPrefix)
	if err != nil {
		log.Fatalf("%s: could not open snapshot: %s", flag.CommandLine.Name(), snapshotPrefix)
	}

	fs, err := snap.Filesystem()
	if err != nil {
		log.Fatalf("%s: %s: %s", flag.CommandLine.Name(), pathname, err)
	}

	if opt_output == "" {
		opt_output = fmt.Sprintf("plakar-%s.%s", time.Now().UTC().Format(time.RFC3339), supportedFormats[opt_format])
	}

	var out io.WriteCloser
	if opt_output == "-" {
		out = os.Stdout
	} else {
		tmp, err := os.CreateTemp("", "plakar-archive-")
		if err != nil {
			log.Fatalf("%s: %s: %s", flag.CommandLine.Name(), pathname, err)
		}
		defer os.Remove(tmp.Name())
		out = tmp
	}

	switch opt_format {
	case "tar":
		if err := archiveTarball(snap, out, fs, pathname, opt_rebase); err != nil {
			log.Fatal(err)
		}
	case "tarball":
		gzipWriter := gzip.NewWriter(out)
		defer gzipWriter.Close()
		if err := archiveTarball(snap, gzipWriter, fs, pathname, opt_rebase); err != nil {
			log.Fatal(err)
		}
	case "zip":
		if err := archiveZip(snap, out, fs, pathname, opt_rebase); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("%s: unsupported format %s", flag.CommandLine.Name(), opt_format)
	}
	if err := out.Close(); err != nil {
		return 1
	}
	if out, isFile := out.(*os.File); isFile {
		if err := os.Rename(out.Name(), opt_output); err != nil {
			return 1
		}
	}

	return 0
}

func archiveTarball(snap *snapshot.Snapshot, out io.Writer, fs *vfs.Filesystem, path string, rebase bool) error {
	logger := snap.Repository().Context().Logger
	tarWriter := tar.NewWriter(out)
	defer tarWriter.Close()

	for file := range fs.Pathnames() {
		if path != "" && !utils.PathIsWithin(file, path) {
			continue
		}

		info, err := fs.Stat(file)
		if err != nil {
			logger.Error("could not stat file %s: %s", file, err)
			continue
		}

		filepath := file
		if rebase {
			filepath = strings.TrimPrefix(filepath, path)
		}

		var header *tar.Header
		switch entry := info.(type) {
		case *vfs.FileEntry:
			header = &tar.Header{
				Name:    filepath,
				Size:    entry.Stat().Size(),
				Mode:    int64(entry.Stat().Mode()),
				ModTime: entry.Stat().ModTime(),
			}
		case *vfs.DirEntry:
			header = &tar.Header{
				Name:    filepath,
				Size:    entry.Stat().Size(),
				Mode:    int64(entry.Stat().Mode()),
				ModTime: entry.Stat().ModTime(),
			}
		default:
			logger.Error("could not stat file %T: %s %s", file, file, err)
		}

		if _, isDir := info.(*vfs.DirEntry); isDir {
			continue
		}

		rd, err := snap.NewReader(file)
		if err != nil {
			logger.Error("could not find file %s", file)
			continue
		}

		err = tarWriter.WriteHeader(header)
		if err != nil {
			logger.Error("could not write header for file %s: %s", file, err)
			rd.Close()
			continue
		}

		_, err = io.Copy(tarWriter, rd)
		if err != nil {
			logger.Error("could not write file %s: %s", file, err)
			rd.Close()
			return err
		}
		rd.Close()
	}

	return nil
}

func archiveZip(snap *snapshot.Snapshot, out io.Writer, fs *vfs.Filesystem, path string, rebase bool) error {
	zipWriter := zip.NewWriter(out)
	defer zipWriter.Close()

	for file := range fs.Pathnames() {

		if path != "" {
			if !utils.PathIsWithin(file, path) {
				continue
			}
		}
		info, _ := fs.Stat(file)
		filepath := file
		if rebase {
			filepath = strings.TrimPrefix(filepath, path)
		}

		if _, isDir := info.(*vfs.DirEntry); isDir {
			continue
		}

		header, err := zip.FileInfoHeader(info.(*vfs.FileEntry).Stat())
		if err != nil {
			log.Printf("could not create header for file %s: %s", file, err)
			continue
		}
		header.Name = strings.TrimLeft(filepath, "/")
		header.Method = zip.Deflate

		rd, err := snap.NewReader(file)
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
	return nil
}
