package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/poolpOrg/plakar/repository"
)

func cmd_rm(store repository.Store, args []string) {
	if len(args) == 0 {
		log.Fatalf("%s: need at least one snapshot ID to rm", flag.CommandLine.Name())
	}

	snapshots := make([]string, 0)
	for id := range store.Snapshots() {
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

	for i := 0; i < len(args); i++ {
		prefix, _ := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		snapshot, err := store.Snapshot(res[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
		}
		snapshot.Purge()
		if !quiet {
			fmt.Fprintf(os.Stdout, "%s: OK\n", snapshot.Uuid)
		}
	}
}
