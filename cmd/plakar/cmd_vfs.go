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
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

func init() {
	registerCommand("vfs", cmd_vfs)
}

func cmd_vfs(ctx Plakar, repo *repository.Repository, args []string) int {
	opt_recursive := false
	flags := flag.NewFlagSet("vfs", flag.ExitOnError)
	flags.BoolVar(&opt_recursive, "recursive", false, "list directories recursively")
	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameters is required", flags.Name())
		return 1
	}

	checksum, err := hex.DecodeString(flags.Args()[0])
	if err != nil {
		logger.Error("%s: invalid checksum: %s", flags.Name(), err)
		return 1
	}
	if len(checksum) != 32 {
		logger.Error("%s: invalid checksum: %s", flags.Name(),
			"checksum must be 32 bytes long")
		return 1
	}

	var checksum32 [32]byte
	copy(checksum32[:], checksum)

	fs, err := vfs.NewFilesystem(repo, checksum32)
	if err != nil {
		logger.Error("%s: could not create filesystem: %s", flags.Name(), err)
		return 1
	}

	//	directoriesIter := fs.Directories()
	//	for directory := range directoriesIter {
	//		fmt.Println(directory)
	//	}

	///filesIter := fs.Files()
	//for file := range filesIter {
	//	fmt.Println("\t", file)
	//}

	//pathnamesIter := fs.Pathnames()
	//for pathname := range pathnamesIter {
	//	fmt.Println(pathname)
	//}

	childrenIter, err := fs.Children(os.Args[3])
	if err != nil {
		logger.Error("%s: could not list children: %s", flags.Name(), err)
		return 1
	}
	for child := range childrenIter {
		fmt.Println(child)
	}

	return 1
}
