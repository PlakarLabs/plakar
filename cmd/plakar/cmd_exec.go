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
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

func init() {
	registerCommand("exec", cmd_exec)
}

func cmd_exec(ctx *context.Context, repo *repository.Repository, args []string) int {
	flags := flag.NewFlagSet("exec", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameters is required", flags.Name())
		return 1
	}

	snapshots, err := getSnapshots(repo, []string{flags.Args()[0]})
	if err != nil {
		log.Fatal(err)
	}
	if len(snapshots) != 1 {
		return 0
	}
	snap := snapshots[0]

	_, pathname := parseSnapshotID(flags.Args()[0])

	fs, err := snap.Filesystem()
	if err != nil {
		logger.Error("%s: %s: %s", flags.Name(), pathname, err)
		return 0
	}

	fsinfo, err := fs.Stat(pathname)
	if err != nil {
		logger.Error("%s: %s: %s", flags.Name(), pathname, err)
		return 0
	}

	if _, isDir := fsinfo.(*vfs.DirEntry); isDir {
		logger.Error("%s: %s: is a directory", flags.Name(), pathname)
		return 0
	}

	if fsinfo, isRegular := fsinfo.(*vfs.FileEntry); !isRegular {
		logger.Error("%s: %s: is not a regular file", flags.Name(), pathname)
		return 0
	} else if fsinfo.FileInfo().Mode().IsRegular() {
		logger.Error("%s: %s: is not a regular file", flags.Name(), pathname)
		return 0
	}

	info := fsinfo.(*vfs.FileEntry)
	object, err := snap.LookupObject(info.Checksum)

	file, err := os.CreateTemp(os.TempDir(), "plakar")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())
	file.Chmod(0500)

	errors := 0
	for _, chunk := range object.Chunks {
		data, err := snap.GetChunk(chunk.Checksum)
		if err != nil {
			logger.Error("%s: could not obtain chunk '%s'", flags.Name(), chunk.Checksum)
			errors++
			break
		}
		file.Write(data)
	}
	file.Close()

	if errors != 0 {
		return 1
	}

	cmd := exec.Command(file.Name(), flags.Args()[1:]...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		defer stdin.Close()
		io.Copy(stdin, os.Stdin)
	}()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		defer stdout.Close()
		io.Copy(os.Stdout, stdout)
	}()

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		defer stdout.Close()
		io.Copy(os.Stderr, stderr)
	}()
	if cmd.Start() == nil {
		cmd.Wait()
		return cmd.ProcessState.ExitCode()
	}
	return 1
}
