package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/poolpOrg/plakar/store"
)

func fiToDiff(fi store.FileInfo) string {
	pwUserLookup, err := user.LookupId(fmt.Sprintf("%d", fi.Uid))
	username := fmt.Sprintf("%d", fi.Uid)
	if err == nil {
		username = pwUserLookup.Username
	}

	grGroupLookup, err := user.LookupGroupId(fmt.Sprintf("%d", fi.Gid))
	groupname := fmt.Sprintf("%d", fi.Gid)
	if err == nil {
		groupname = grGroupLookup.Name
	}

	return fmt.Sprintf("%s % 8s % 8s % 8s %s",
		fi.Mode,
		username,
		groupname,
		humanize.Bytes(uint64(fi.Size)),
		fi.ModTime.UTC())
}

func cmd_diff(pstore store.Store, args []string) {
	if len(args) < 2 {
		log.Fatalf("%s: needs two snapshot ID and/or snapshot files to cat", flag.CommandLine.Name())
	}

	snapshots := make([]string, 0)
	for id, _ := range pstore.Snapshots() {
		snapshots = append(snapshots, id)
	}

	for i := 0; i < 2; i++ {
		prefix, _ := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
	}

	if len(args) == 2 {
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
			snapshot1 := pstore.Snapshot(res1[0])
			snapshot2 := pstore.Snapshot(res2[0])
			for dir1, fi1 := range snapshot1.Directories {
				fi2, ok := snapshot2.Directories[dir1]
				if !ok {
					fmt.Println("- ", fiToDiff(*fi1), dir1)
				}
				if *fi1 != *fi2 {
					fmt.Println("- ", fiToDiff(*fi1), dir1)
					fmt.Println("+ ", fiToDiff(*fi2), dir1)
				}
			}

			for dir2, fi2 := range snapshot2.Directories {
				_, ok := snapshot1.Directories[dir2]
				if !ok {
					fmt.Println("+ ", fiToDiff(*fi2), dir2)
				}
			}

			for file1, fi1 := range snapshot1.Files {
				fi2, ok := snapshot2.Files[file1]
				if !ok {
					fmt.Println("- ", fiToDiff(*fi1), file1)
				}
				if *fi1 != *fi2 {
					fmt.Println("- ", fiToDiff(*fi1), file1)
					fmt.Println("+ ", fiToDiff(*fi2), file1)
				}
			}

			for file2, fi2 := range snapshot2.Files {
				_, ok := snapshot1.Files[file2]
				if !ok {
					fmt.Println("+ ", fiToDiff(*fi2), file2)
				}
			}
		} else {
			// file diff
			prefix1, file1 := parseSnapshotID(args[0])
			prefix2, file2 := parseSnapshotID(args[1])
			res1 := findSnapshotByPrefix(snapshots, prefix1)
			res2 := findSnapshotByPrefix(snapshots, prefix2)
			snapshot1 := pstore.Snapshot(res1[0])
			snapshot2 := pstore.Snapshot(res2[0])
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
		snapshot1 := pstore.Snapshot(res1[0])
		snapshot2 := pstore.Snapshot(res2[0])

		for i := 2; i < len(args); i++ {
			_, ok1 := snapshot1.Sums[args[i]]
			_, ok2 := snapshot2.Sums[args[i]]
			if !ok1 && !ok2 {
				fmt.Fprintf(os.Stderr, "%s: %s: file not found in snapshots\n", flag.CommandLine.Name(), args[i])
			}

			diff_files(snapshot1, snapshot2, args[i], args[i])
		}
	}

}

func cmd_diff2(pstore store.Store, args []string) {
	if len(args) < 2 {
		log.Fatalf("%s: needs two snapshot ID and/or snapshot files to cat", flag.CommandLine.Name())
	}

	snapshots := make([]string, 0)
	for id, _ := range pstore.Snapshots() {
		snapshots = append(snapshots, id)
	}

	for i := 0; i < len(args); i++ {
		prefix, pattern := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
		_ = pattern
	}

	prefix1, pattern1 := parseSnapshotID(args[0])
	prefix2, pattern2 := parseSnapshotID(args[1])
	res1 := findSnapshotByPrefix(snapshots, prefix1)
	res2 := findSnapshotByPrefix(snapshots, prefix2)

	diffFiles := false
	if strings.Contains(args[0], ":") {
		if !strings.Contains(args[1], ":") {
			log.Fatalf("%s: needs two snapshot files to diff files", flag.CommandLine.Name())
		}
		diffFiles = true
	}

	filepath1 := path.Clean(pattern1)
	filepath2 := path.Clean(pattern2)

	if filepath1 == "." {
		filepath1 = ""
	}
	if filepath2 == "." {
		filepath2 = ""
	}

	snapshot1 := pstore.Snapshot(res1[0])
	snapshot2 := pstore.Snapshot(res2[0])

	for dir1, fi1 := range snapshot1.Directories {
		fi2, ok := snapshot2.Directories[dir1]
		if !ok {
			fmt.Println("- ", fiToDiff(*fi1), dir1)
		}
		if *fi1 != *fi2 {
			fmt.Println("- ", fiToDiff(*fi1), dir1)
			fmt.Println("+ ", fiToDiff(*fi2), dir1)
		}
	}

	for dir2, fi2 := range snapshot2.Directories {
		_, ok := snapshot1.Directories[dir2]
		if !ok {
			fmt.Println("+ ", fiToDiff(*fi2), dir2)
		}
	}

	for file1, fi1 := range snapshot1.Files {
		fi2, ok := snapshot2.Files[file1]
		if !ok {
			fmt.Println("- ", fiToDiff(*fi1), file1)
		}
		if *fi1 != *fi2 {
			fmt.Println("- ", fiToDiff(*fi1), file1)
			fmt.Println("+ ", fiToDiff(*fi2), file1)
		}
	}

	for file2, fi2 := range snapshot2.Files {
		_, ok := snapshot1.Files[file2]
		if !ok {
			fmt.Println("+ ", fiToDiff(*fi2), file2)
		}
	}

	if !diffFiles {
		return
	}

	if filepath1 != "" {
		diff_files(snapshot1, snapshot2, filepath1, filepath2)
		return
	}

	for path1, _ := range snapshot1.Sums {
		diff_files(snapshot1, snapshot2, path1, path1)
	}

	for path2, _ := range snapshot2.Sums {
		_, ok := snapshot1.Sums[path2]
		if !ok {
			diff_files(snapshot1, snapshot2, path2, path2)
		}
	}
}

func diff_files(snapshot1 *store.Snapshot, snapshot2 *store.Snapshot, filename1 string, filename2 string) {
	sum1, ok1 := snapshot1.Sums[filename1]
	sum2, ok2 := snapshot2.Sums[filename2]

	// file does not exist in either snapshot
	if !ok1 && !ok2 {
		return
	}

	if sum1 == sum2 {
		fmt.Printf("%s:%s and %s:%s are identical\n",
			snapshot1.Uuid, filename1, snapshot2.Uuid, filename2)
		return
	}

	buf1 := make([]byte, 0)
	buf2 := make([]byte, 0)

	// file exists in snapshot1, grab a copy
	if ok1 {
		object, err := snapshot1.ObjectGet(sum1)
		if err != nil {
		}
		for _, chunk := range object.Chunks {
			data, err := snapshot2.ChunkGet(chunk.Checksum)
			if err != nil {
			}
			buf1 = append(data)
		}
	}

	if ok2 {
		object, err := snapshot2.ObjectGet(sum2)
		if err != nil {
		}
		for _, chunk := range object.Chunks {
			data, err := snapshot2.ChunkGet(chunk.Checksum)
			if err != nil {
			}
			buf2 = append(data)
		}
	}

	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(buf1)),
		B:        difflib.SplitLines(string(buf2)),
		FromFile: snapshot1.Uuid + ":" + filename1,
		ToFile:   snapshot2.Uuid + ":" + filename2,
		Context:  3,
	}
	text, _ := difflib.GetUnifiedDiffString(diff)
	fmt.Printf("%s", text)
}
