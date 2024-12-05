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

package exec

import (
	"flag"
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
)

func init() {
	subcommands.Register("exec", cmd_exec)
}

func cmd_exec(ctx *context.Context, repo *repository.Repository, args []string) int {
	logger := ctx.Logger
	flags := flag.NewFlagSet("exec", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameters is required", flags.Name())
		return 1
	}

	snapshots, err := utils.GetSnapshots(repo, []string{flags.Args()[0]})
	if err != nil {
		log.Fatal(err)
	}
	if len(snapshots) != 1 {
		return 0
	}
	snap := snapshots[0]

	_, pathname := utils.ParseSnapshotID(flags.Args()[0])

	rd, err := snap.NewReader(pathname)
	if err != nil {
		logger.Error("%s: %s: failed to open: %s", flags.Name(), pathname, err)
		return 1
	}
	defer rd.Close()

	file, err := os.CreateTemp(os.TempDir(), "plakar")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(file.Name())
	file.Chmod(0500)

	_, err = io.Copy(file, rd)
	if err != nil {
		log.Fatal(err)
	}
	file.Close()

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
