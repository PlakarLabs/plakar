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
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/snapshot/importer"
	"github.com/gobwas/glob"
	"github.com/google/uuid"
)

func init() {
	registerCommand("push", cmd_push)
}

type excludeFlags []string

func (e *excludeFlags) String() string {
	return strings.Join(*e, ",")
}

func (e *excludeFlags) Set(value string) error {
	*e = append(*e, value)
	return nil
}

func cmd_push(ctx Plakar, repo *repository.Repository, args []string) int {
	var opt_tags string
	var opt_excludes string
	var opt_exclude excludeFlags
	var opt_concurrency uint64

	excludes := []glob.Glob{}
	flags := flag.NewFlagSet("push", flag.ExitOnError)
	flags.Uint64Var(&opt_concurrency, "max-concurrency", uint64(ctx.NumCPU)*8+1, "maximum number of parallel tasks")
	flags.StringVar(&opt_tags, "tag", "", "tag to assign to this snapshot")
	flags.StringVar(&opt_excludes, "excludes", "", "file containing a list of exclusions")
	flags.Var(&opt_exclude, "exclude", "file containing a list of exclusions")
	flags.Parse(args)

	for _, item := range opt_exclude {
		excludes = append(excludes, glob.MustCompile(item))
	}

	dir, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}

	if opt_excludes != "" {
		fp, err := os.Open(opt_excludes)
		if err != nil {
			logger.Error("%s", err)
			return 1
		}
		defer fp.Close()

		scanner := bufio.NewScanner(fp)
		for scanner.Scan() {
			pattern, err := glob.Compile(scanner.Text())
			if err != nil {
				logger.Error("%s", err)
				return 1
			}
			excludes = append(excludes, pattern)
		}
		if err := scanner.Err(); err != nil {
			logger.Error("%s", err)
			return 1
		}
	}
	_ = excludes

	snap, err := snapshot.New(repo, uuid.Must(uuid.NewRandom()))
	if err != nil {
		logger.Error("%s", err)
		return 1
	}

	snap.Header.Hostname = ctx.Hostname
	snap.Header.Username = ctx.Username
	snap.Header.OperatingSystem = runtime.GOOS
	snap.Header.MachineID = ctx.MachineID
	snap.Header.CommandLine = ctx.CommandLine
	snap.Header.ProcessID = os.Getpid()

	var tags []string
	if opt_tags == "" {
		tags = []string{}
	} else {
		tags = []string{opt_tags}
	}
	snap.Header.Tags = tags

	opts := &snapshot.PushOptions{
		MaxConcurrency: opt_concurrency,
		Excludes:       excludes,
	}

	if flags.NArg() == 0 {
		err = snap.Push(dir, opts)
	} else if flags.NArg() == 1 {
		var cleanPath string

		if !strings.HasPrefix(flags.Arg(0), "/") {
			_, err := importer.NewImporter(flags.Arg(0))
			if err != nil {
				cleanPath = path.Clean(dir + "/" + flags.Arg(0))
			} else {
				cleanPath = flags.Arg(0)
			}
		} else {
			cleanPath = path.Clean(flags.Arg(0))
		}
		err = snap.Push(cleanPath, opts)
	} else {
		log.Fatal("only one directory pushable")
	}

	if err != nil {
		logger.Error("%s", err)
		return 1
	}

	logger.Info("created snapshot %s", snap.Header.GetIndexShortID())
	return 0
}
