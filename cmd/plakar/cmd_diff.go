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
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/snapshot"
	"github.com/PlakarLabs/plakar/snapshot/vfs"
	"github.com/alecthomas/chroma/quick"
	"github.com/pmezard/go-difflib/difflib"
)

func init() {
	registerCommand("diff", cmd_diff)
}

func cmd_diff(ctx Plakar, repo *repository.Repository, args []string) int {
	var opt_highlight bool
	flags := flag.NewFlagSet("diff", flag.ExitOnError)
	flags.BoolVar(&opt_highlight, "highlight", false, "highlight output")
	flags.Parse(args)

	if flags.NArg() != 2 {
		fmt.Println("args", flags.Args())
		log.Fatalf("%s: needs two snapshot ID and/or snapshot files to diff", flag.CommandLine.Name())
	}

	snapshotPrefix1, pathname1 := parseSnapshotID(flags.Arg(0))
	snapshotPrefix2, pathname2 := parseSnapshotID(flags.Arg(1))

	snap1, err := openSnapshotByPrefix(repo, snapshotPrefix1)
	if err != nil {
		log.Fatalf("%s: could not open snapshot: %s", flag.CommandLine.Name(), snapshotPrefix1)
	}

	snap2, err := openSnapshotByPrefix(repo, snapshotPrefix2)
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

	if fileEntry1.Checksum == fileEntry2.Checksum {
		fmt.Printf("%s:%s and %s:%s are identical\n",
			fmt.Sprintf("%x", snap1.Header.GetIndexShortID()), filepath.Join(fileEntry1.ParentPath, fileEntry1.Name),
			fmt.Sprintf("%x", snap2.Header.GetIndexShortID()), filepath.Join(fileEntry2.ParentPath, fileEntry2.Name))
		return "", nil
	}

	filename1 := filepath.Join(fileEntry1.ParentPath, fileEntry1.Name)
	filename2 := filepath.Join(fileEntry2.ParentPath, fileEntry2.Name)

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

/*
	if len(flags.Args()) == 2 {
		// check if snapshot id's both reference a file
		// if not, stat diff of snapshots, else diff files
		rel1 := strings.Contains(args[0], ":")
		rel2 := strings.Contains(args[1], ":")
		if (rel1 && !rel2) || (!rel1 && rel2) {
			log.Fatalf("%s: snapshot subset delimiter must be used on both snapshots", flag.CommandLine.Name())
		}

		if !rel1 {
			// stat diff
			prefix1, _ := parseSnapshotID(args[0])
			prefix2, _ := parseSnapshotID(args[1])
			res1 := findSnapshotByPrefix(snapshots, prefix1)
			res2 := findSnapshotByPrefix(snapshots, prefix2)
			snapshot1, err := snapshot.Load(repo, res1[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res1[0])
			}
			fs1, err := vfs.NewFilesystem(repo, snapshot1.Header.Root)
			if err != nil {
				log.Fatalf("%s: could not create filesystem: %s", flag.CommandLine.Name(), err)
			}

			snapshot2, err := snapshot.Load(repo, res2[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res2[0])
			}
			fs2, err := vfs.NewFilesystem(repo, snapshot2.Header.Root)
			if err != nil {
				log.Fatalf("%s: could not create filesystem: %s", flag.CommandLine.Name(), err)
			}

			for dir1 := range fs1.Directories() {
				fi1, _ := fs1.Stat(dir1)
				fi2, err := fs2.Stat(dir1)

				if err != nil {
					fmt.Println("- ", fiToDiff(*fi1.Stat), dir1)
					continue
				}
				if *fi1 != *fi2 {
					fmt.Println("- ", fiToDiff(*fi1), dir1)
					fmt.Println("+ ", fiToDiff(*fi2), dir1)
				}
			}

			for dir2 := range snapshot2.Filesystem.Directories() {
				fi2, _ := snapshot2.Filesystem.Stat(dir2)
				_, err := snapshot1.Filesystem.Stat(dir2)
				if err != nil {
					fmt.Println("+ ", fiToDiff(*fi2), dir2)
				}
			}

			for file1 := range snapshot1.Filesystem.Files() {
				fi1, _ := snapshot1.Filesystem.Stat(file1)
				fi2, err := snapshot2.Filesystem.Stat(file1)
				if err != nil {
					fmt.Println("- ", fiToDiff(*fi1), file1)
					continue
				}
				if *fi1 != *fi2 {
					fmt.Println("- ", fiToDiff(*fi1), file1)
					fmt.Println("+ ", fiToDiff(*fi2), file1)
				}
			}

			for file2 := range snapshot2.Filesystem.Files() {
				fi2, _ := snapshot2.Filesystem.Stat(file2)
				_, err := snapshot1.Filesystem.Stat(file2)
				if err != nil {
					fmt.Println("+ ", fiToDiff(*fi2), file2)
				}
			}
		} else {
			// file diff
			prefix1, file1 := parseSnapshotID(args[0])
			prefix2, file2 := parseSnapshotID(args[1])
			res1 := findSnapshotByPrefix(snapshots, prefix1)
			res2 := findSnapshotByPrefix(snapshots, prefix2)
			snapshot1, err := snapshot.Load(repo, res1[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res1[0])
			}
			snapshot2, err := snapshot.Load(repo, res2[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res2[0])
			}
			diff_files(snapshot1, snapshot2, file1, file2)
		}

	} else {
		if strings.Contains(args[0], ":") || strings.Contains(args[1], ":") {
			log.Fatalf("%s: snapshot subset delimiter not allowed in snapshot ID when diffing common files", flag.CommandLine.Name())
		}

		prefix1, _ := parseSnapshotID(args[0])
		prefix2, _ := parseSnapshotID(args[1])
		res1 := findSnapshotByPrefix(snapshots, prefix1)
		res2 := findSnapshotByPrefix(snapshots, prefix2)
		snapshot1, err := snapshot.Load(repo, res1[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res1[0])
		}
		snapshot2, err := snapshot.Load(repo, res2[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res2[0])
		}
		for i := 2; i < len(args); i++ {
			pathnameChecksum := repo.Checksum([]byte(args[i]))
			object1, err := snapshot1.Index.LookupObjectForPathnameChecksum(pathnameChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), args[i], err)
				return 1
			}
			object2, err := snapshot2.Index.LookupObjectForPathnameChecksum(pathnameChecksum)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), args[i], err)
				return 1
			}

			if object1 == nil && object2 == nil {
				fmt.Fprintf(os.Stderr, "%s: %s: file not found in snapshots\n", flag.CommandLine.Name(), args[i])
			}

			diff_files(snapshot1, snapshot2, args[i], args[i])
		}
	}
	return 0
}

func fiToDiff(fi objects.FileInfo) string {
	pwUserLookup, err := user.LookupId(fmt.Sprintf("%d", fi.Uid()))
	username := fmt.Sprintf("%d", fi.Uid())
	if err == nil {
		username = pwUserLookup.Username
	}

	grGroupLookup, err := user.LookupGroupId(fmt.Sprintf("%d", fi.Gid()))
	groupname := fmt.Sprintf("%d", fi.Gid())
	if err == nil {
		groupname = grGroupLookup.Name
	}

	return fmt.Sprintf("%s % 8s % 8s % 8s %s",
		fi.Mode(),
		username,
		groupname,
		humanize.Bytes(uint64(fi.Size())),
		fi.ModTime().UTC())
}

func diff_files(snapshot1 *snapshot.Snapshot, snapshot2 *snapshot.Snapshot, filename1 string, filename2 string) {
	pathnameChecksum := snapshot1.Repository().Checksum([]byte(filename1))
	object1, err := snapshot1.Index.LookupObjectForPathnameChecksum(pathnameChecksum)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), filename1, err)
		return
	}

	pathnameChecksum = snapshot2.Repository().Checksum([]byte(filename2))
	object2, err := snapshot2.Index.LookupObjectForPathnameChecksum(pathnameChecksum)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s: %s\n", flag.CommandLine.Name(), filename2, err)
		return
	}

	// file does not exist in either snapshot
	if object1 == nil && object2 == nil {
		return
	}

	if bytes.Equal(object1.Checksum[:], object2.Checksum[:]) {
		fmt.Printf("%s:%s and %s:%s are identical\n",
			snapshot1.Header.GetIndexShortID(), filename1, snapshot2.Header.GetIndexShortID(), filename2)
		return
	}

	buf1 := make([]byte, 0)
	rd1, err := snapshot1.NewReader(filename1)
	if err == nil {
		buf1, err = io.ReadAll(rd1)
		if err != nil {
			return
		}
	}

	buf2 := make([]byte, 0)
	rd2, err := snapshot2.NewReader(filename2)
	if err == nil {
		buf2, err = io.ReadAll(rd2)
		if err != nil {
			return
		}
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(buf1)),
		B:        difflib.SplitLines(string(buf2)),
		FromFile: snapshot1.Header.GetIndexShortID() + ":" + filename1,
		ToFile:   snapshot2.Header.GetIndexShortID() + ":" + filename2,
		Context:  3,
	}
	text, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return
	}
	fmt.Printf("%s", text)
}
*/
