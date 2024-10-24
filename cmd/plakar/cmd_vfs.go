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
	"fmt"
	"path/filepath"

	"github.com/PlakarLabs/plakar/context"
	"github.com/PlakarLabs/plakar/logger"
	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
)

func init() {
	registerCommand("vfs", cmd_vfs)
}

func cmd_vfs(ctx *context.Context, repo *repository.Repository, args []string) int {
	flags := flag.NewFlagSet("vfs", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameters is required", flags.Name())
		return 1
	}

	snapshotPrefix, pathname := parseSnapshotID(flags.Arg(0))
	snap1, err := openSnapshotByPrefix(repo, snapshotPrefix)
	if err != nil {
		logger.Error("%s: could not open snapshot: %s", flags.Name(), flags.Arg(0))
		return 1
	}
	fs, err := snap1.Filesystem()
	if err != nil {
		logger.Error("%s: could not obtain filesystem: %s", flags.Name(), err)
		return 1
	}

	fsinfo, err := fs.Stat(filepath.Clean(pathname))
	if err != nil {
		logger.Error("%s: could not stat %s: %s", flags.Name(), pathname, err)
		return 1
	}

	if dirEntry, isDir := fsinfo.(*vfs.DirEntry); isDir {
		fmt.Printf("[DirEntry]\n")
		fmt.Printf("Version: %d\n", dirEntry.Version)
		fmt.Printf("ParentPath: %s\n", dirEntry.ParentPath)
		fmt.Printf("Name: %s\n", dirEntry.FileInfo().Name())
		fmt.Printf("Type: %d\n", dirEntry.Type)
		fmt.Printf("Size: %d\n", dirEntry.FileInfo().Size())
		fmt.Printf("Permissions: %s\n", dirEntry.FileInfo().Mode())
		fmt.Printf("ModTime: %s\n", dirEntry.FileInfo().ModTime())
		fmt.Printf("DeviceID: %d\n", dirEntry.DeviceID)
		fmt.Printf("InodeID: %d\n", dirEntry.InodeID)
		fmt.Printf("UserID: %d\n", dirEntry.UserID)
		fmt.Printf("GroupID: %d\n", dirEntry.GroupID)
		fmt.Printf("NumLinks: %d\n", dirEntry.NumLinks)
		fmt.Printf("ExtendedAttributes: %s\n", dirEntry.ExtendedAttributes)
		fmt.Printf("CustomMetadata: %s\n", dirEntry.CustomMetadata)
		fmt.Printf("Tags: %s\n", dirEntry.Tags)
		for offset, child := range dirEntry.Children {
			fmt.Printf("Child[%d].Checksum: %x\n", offset, child.Checksum)
			fmt.Printf("Child[%d].FileInfo.Name(): %s\n", offset, child.FileInfo.Name())
			fmt.Printf("Child[%d].FileInfo.Size(): %d\n", offset, child.FileInfo.Size())
			fmt.Printf("Child[%d].FileInfo.Mode(): %s\n", offset, child.FileInfo.Mode())
			fmt.Printf("Child[%d].FileInfo.Dev(): %d\n", offset, child.FileInfo.Dev())
			fmt.Printf("Child[%d].FileInfo.Ino(): %d\n", offset, child.FileInfo.Ino())
			fmt.Printf("Child[%d].FileInfo.Uid(): %d\n", offset, child.FileInfo.Uid())
			fmt.Printf("Child[%d].FileInfo.Gid(): %d\n", offset, child.FileInfo.Gid())
			fmt.Printf("Child[%d].FileInfo.Nlink(): %d\n", offset, child.FileInfo.Nlink())
		}

	} else if fileEntry, isFile := fsinfo.(*vfs.FileEntry); isFile {
		fmt.Printf("[FileEntry]\n")
		fmt.Printf("Version: %d\n", fileEntry.Version)
		fmt.Printf("ParentPath: %s\n", fileEntry.ParentPath)
		fmt.Printf("Name: %s\n", fileEntry.FileInfo().Name())
		fmt.Printf("Type: %d\n", fileEntry.Type)
		fmt.Printf("Size: %d\n", fileEntry.FileInfo().Size())
		fmt.Printf("Permissions: %s\n", fileEntry.FileInfo().Mode())
		fmt.Printf("ModTime: %s\n", fileEntry.FileInfo().ModTime())
		fmt.Printf("DeviceID: %d\n", fileEntry.DeviceID)
		fmt.Printf("InodeID: %d\n", fileEntry.InodeID)
		fmt.Printf("UserID: %d\n", fileEntry.UserID)
		fmt.Printf("GroupID: %d\n", fileEntry.GroupID)
		fmt.Printf("NumLinks: %d\n", fileEntry.NumLinks)
		fmt.Printf("ExtendedAttributes: %s\n", fileEntry.ExtendedAttributes)
		fmt.Printf("FileAttributes: %v\n", fileEntry.FileAttributes)
		if fileEntry.SymlinkTarget != "" {
			fmt.Printf("SymlinkTarget: %s\n", fileEntry.SymlinkTarget)
		}
		fmt.Printf("CustomMetadata: %s\n", fileEntry.CustomMetadata)
		fmt.Printf("Tags: %s\n", fileEntry.Tags)
		fmt.Printf("Checksum: %x\n", fileEntry.Checksum)
		for offset, chunk := range fileEntry.Chunks {
			fmt.Printf("Chunk[%d].Checksum: %x\n", offset, chunk.Checksum)
			fmt.Printf("Chunk[%d].Length: %d\n", offset, chunk.Length)
		}
	}

	return 0
}
