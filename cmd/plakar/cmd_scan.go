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
	"regexp"
	"strings"
	"time"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/PlakarLabs/plakar/vfs"
)

func init() {
	registerCommand("scan", cmd_scan)
}

func cmd_scan(ctx Plakar, repository *storage.Repository, args []string) int {
	var opt_exclude excludeFlags
	var opt_excludes string

	excludes := []*regexp.Regexp{}

	flags := flag.NewFlagSet("scan", flag.ExitOnError)
	flags.Var(&opt_exclude, "exclude", "file containing a list of exclusions")
	flags.StringVar(&opt_excludes, "excludes", "", "file containing a list of exclusions")
	flags.Parse(args)

	for _, item := range opt_exclude {
		excludes = append(excludes, regexp.MustCompile(item))
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
			pattern, err := regexp.Compile(scanner.Text())
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

	dir, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}

	var fs *vfs.Filesystem
	var t0 time.Time
	if flags.NArg() == 0 {
		t0 = time.Now()
		fs, err = vfs.NewFilesystemFromScan(repository.Location, dir, excludes)
	} else if flags.NArg() == 1 {
		var cleanPath string

		if !strings.HasPrefix(flags.Arg(0), "/") {
			cleanPath = path.Clean(dir + "/" + flags.Arg(0))
		} else {
			cleanPath = path.Clean(flags.Arg(0))
		}
		t0 = time.Now()
		fs, err = vfs.NewFilesystemFromScan(repository.Location, cleanPath, excludes)
	} else {
		log.Fatal("only one directory pushable")
	}
	if err != nil {
		logger.Error("%s", err)
		return 1
	}
	fmt.Printf("%d directories, %d files, %d symlinks in %s\n", fs.NDirectories(), fs.NFiles(), len(fs.ListNonRegular()), time.Since(t0))
	return 0
}
