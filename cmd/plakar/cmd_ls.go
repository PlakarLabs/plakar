package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/poolpOrg/plakar/store"
)

func cmd_ls(pstore store.Store, args []string) {
	if len(args) == 0 {
		list_snapshots(pstore)
		return
	}

	snapshots := make([]string, 0)
	for id := range pstore.Snapshots() {
		snapshots = append(snapshots, id)
	}

	for i := 0; i < len(args); i++ {
		prefix, _ := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
	}

	for _, arg := range args {
		prefix, pattern := parseSnapshotID(arg)
		res := findSnapshotByPrefix(snapshots, prefix)
		snapshot, err := pstore.Snapshot(res[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
		}
		for name, fi := range snapshot.Files {
			if !strings.HasPrefix(name, pattern) {
				continue
			}

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
			fmt.Fprintf(os.Stdout, "%s %s % 8s % 8s % 8s %s\n",
				snapshot.Sums[name],
				fi.Mode,
				username,
				groupname,
				humanize.Bytes(uint64(fi.Size)),
				name)
		}
	}
}

func list_snapshots(pstore store.Store) {
	snapshots := pstore.Snapshots()
	ids := make([]string, 0)
	for id := range snapshots {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		return snapshots[ids[i]].ModTime().Before(snapshots[ids[j]].ModTime())
	})
	for _, id := range ids {
		fi := snapshots[id]
		snapshot, err := pstore.Snapshot(id)
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), id)
		}
		fmt.Fprintf(os.Stdout, "%s [%s] (size: %s, files: %d, dirs: %d)\n",
			id,
			fi.ModTime().UTC().Format(time.RFC3339),
			humanize.Bytes(snapshot.Size),
			len(snapshot.Files),
			len(snapshot.Directories))
	}
}
