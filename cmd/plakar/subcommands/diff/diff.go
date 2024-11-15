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

package diff

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/alecthomas/chroma/quick"
	"github.com/pmezard/go-difflib/difflib"
)

func init() {
	subcommands.Register("diff", cmd_diff)
}

func cmd_diff(ctx *context.Context, repo *repository.Repository, args []string) int {
	var opt_highlight bool
	flags := flag.NewFlagSet("diff", flag.ExitOnError)
	flags.BoolVar(&opt_highlight, "highlight", false, "highlight output")
	flags.Parse(args)

	if flags.NArg() != 2 {
		fmt.Println("args", flags.Args())
		log.Fatalf("%s: needs two snapshot ID and/or snapshot files to diff", flag.CommandLine.Name())
	}

	snapshotPrefix1, pathname1 := utils.ParseSnapshotID(flags.Arg(0))
	snapshotPrefix2, pathname2 := utils.ParseSnapshotID(flags.Arg(1))

	snap1, err := utils.OpenSnapshotByPrefix(repo, snapshotPrefix1)
	if err != nil {
		log.Fatalf("%s: could not open snapshot: %s", flag.CommandLine.Name(), snapshotPrefix1)
	}

	snap2, err := utils.OpenSnapshotByPrefix(repo, snapshotPrefix2)
	if err != nil {
		log.Fatalf("%s: could not open snapshot: %s", flag.CommandLine.Name(), snapshotPrefix2)
	}

	var diff string
	if pathname1 == "" && pathname2 == "" {
		diff, err = diff_filesystems(snap1, snap2)
		if err != nil {
			log.Fatalf("%s: could not diff snapshots: %s", flag.CommandLine.Name(), err)
		}
	} else {
		if pathname1 == "" {
			pathname1 = pathname2
		}
		if pathname2 == "" {
			pathname2 = pathname1
		}
		diff, err = diff_pathnames(snap1, pathname1, snap2, pathname2)
		if err != nil {
			log.Fatalf("%s: could not diff pathnames: %s", flag.CommandLine.Name(), err)
		}
	}

	if opt_highlight {
		err = quick.Highlight(os.Stdout, diff, "diff", "terminal", "dracula")
		if err != nil {
			log.Fatalf("%s: could not highlight diff: %s", flag.CommandLine.Name(), err)
		}
	} else {
		fmt.Printf("%s", diff)
	}
	return 0
}

func diff_filesystems(snap1 *snapshot.Snapshot, snap2 *snapshot.Snapshot) (string, error) {
	vfs1, err := snap1.Filesystem()
	if err != nil {
		return "", err
	}

	vfs2, err := snap2.Filesystem()
	if err != nil {
		return "", err
	}

	stat1, err1 := vfs1.Stat("/")
	stat2, err2 := vfs2.Stat("/")
	if err1 != nil && err2 != nil {
		return "", fmt.Errorf("root not found in both snapshots")
	}
	return diff_directories(stat1.(*vfs.DirEntry), stat2.(*vfs.DirEntry))
}

func diff_pathnames(snap1 *snapshot.Snapshot, pathname1 string, snap2 *snapshot.Snapshot, pathname2 string) (string, error) {
	vfs1, err := snap1.Filesystem()
	if err != nil {
		return "", err
	}

	vfs2, err := snap2.Filesystem()
	if err != nil {
		return "", err
	}

	stat1, err1 := vfs1.Stat(pathname1)
	stat2, err2 := vfs2.Stat(pathname2)
	if err1 != nil && err2 != nil {
		return "", fmt.Errorf("file not found in both snapshots")
	}

	dirEntry1, isDir1 := stat1.(*vfs.DirEntry)
	dirEntry2, isDir2 := stat2.(*vfs.DirEntry)
	if isDir1 && isDir2 {
		return diff_directories(dirEntry1, dirEntry2)
	}

	fileEntry1, isFile1 := stat1.(*vfs.FileEntry)
	fileEntry2, isFile2 := stat2.(*vfs.FileEntry)
	if isFile1 && isFile2 {
		return diff_files(snap1, fileEntry1, snap2, fileEntry2)
	}

	return "", fmt.Errorf("not implemented yet")
}

func diff_directories(dirEntry1 *vfs.DirEntry, dirEntry2 *vfs.DirEntry) (string, error) {
	_ = dirEntry1
	_ = dirEntry2
	return "", fmt.Errorf("not implemented yet")
}

func diff_files(snap1 *snapshot.Snapshot, fileEntry1 *vfs.FileEntry, snap2 *snapshot.Snapshot, fileEntry2 *vfs.FileEntry) (string, error) {
	_ = fileEntry1
	_ = fileEntry2

	if fileEntry1.Object.Checksum == fileEntry2.Object.Checksum {
		fmt.Printf("%s:%s and %s:%s are identical\n",
			fmt.Sprintf("%x", snap1.Header.GetIndexShortID()), filepath.Join(fileEntry1.ParentPath, fileEntry1.Stat().Name()),
			fmt.Sprintf("%x", snap2.Header.GetIndexShortID()), filepath.Join(fileEntry2.ParentPath, fileEntry2.Stat().Name()))
		return "", nil
	}

	filename1 := filepath.Join(fileEntry1.ParentPath, fileEntry1.Stat().Name())
	filename2 := filepath.Join(fileEntry2.ParentPath, fileEntry2.Stat().Name())

	buf1 := make([]byte, 0)
	rd1, err := snap1.NewReader(filename1)
	if err == nil {
		buf1, err = io.ReadAll(rd1)
		if err != nil {
			return "", err
		}
	}

	buf2 := make([]byte, 0)
	rd2, err := snap2.NewReader(filename2)
	if err == nil {
		buf2, err = io.ReadAll(rd2)
		if err != nil {
			return "", err
		}
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(buf1)),
		B:        difflib.SplitLines(string(buf2)),
		FromFile: fmt.Sprintf("%x", snap1.Header.GetIndexShortID()) + ":" + filename1,
		ToFile:   fmt.Sprintf("%x", snap2.Header.GetIndexShortID()) + ":" + filename2,
		Context:  3,
	}
	text, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return "", err
	}
	return text, nil
}
