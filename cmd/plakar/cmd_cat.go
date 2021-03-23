package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/poolpOrg/plakar/store"
)

func cmd_cat(pstore store.Store, args []string) {
	if len(args) == 0 {
		log.Fatalf("%s: need at least one snapshot ID and file or object to cat", flag.CommandLine.Name())
	}

	snapshots := make([]string, 0)
	for id := range pstore.Snapshots() {
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
		if pattern == "" {
			log.Fatalf("%s: missing filename.", flag.CommandLine.Name())
		}

		if !strings.HasPrefix(pattern, "/") {
			objects := make([]string, 0)
			snapshot, err := pstore.Snapshot(res[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
			}
			for id := range snapshot.Objects {
				objects = append(objects, id)
			}
			res = findObjectByPrefix(objects, pattern)
			if len(res) == 0 {
				log.Fatalf("%s: no object has prefix: %s", flag.CommandLine.Name(), pattern)
			} else if len(res) > 1 {
				log.Fatalf("%s: object ID is ambigous: %s (matches %d objects)", flag.CommandLine.Name(), pattern, len(res))
			}
		}
	}

	for i := 0; i < len(args); i++ {
		prefix, pattern := parseSnapshotID(args[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		snapshot, err := pstore.Snapshot(res[0])
		if err != nil {
			log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
		}

		var checksum string
		if strings.HasPrefix(pattern, "/") {
			tmp, ok := snapshot.Sums[pattern]
			if !ok {
				log.Fatalf("%s: %s: no such file in snapshot.", flag.CommandLine.Name(), pattern)
			}
			checksum = tmp
		} else {
			objects := make([]string, 0)
			snapshot, err := pstore.Snapshot(res[0])
			if err != nil {
				log.Fatalf("%s: could not open snapshot %s", flag.CommandLine.Name(), res[0])
			}
			for id := range snapshot.Objects {
				objects = append(objects, id)
			}
			res = findObjectByPrefix(objects, pattern)
			checksum = res[0]
		}

		object, err := snapshot.ObjectGet(checksum)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not open object", flag.CommandLine.Name())
			continue
		}

		for _, chunk := range object.Chunks {
			data, err := snapshot.ChunkGet(chunk.Checksum)
			if err != nil {
				log.Fatalf("%s: %s: failed to obtain chunk %s.", flag.CommandLine.Name(), pattern, chunk.Checksum)
			}
			os.Stdout.Write(data)
		}
	}
}
