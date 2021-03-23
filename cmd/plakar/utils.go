package main

import (
	"strings"
)

func parseSnapshotID(id string) (string, string) {
	tmp := strings.Split(id, ":")
	prefix := id
	pattern := ""
	if len(tmp) != 0 {
		prefix = tmp[0]
		pattern = strings.Join(tmp[1:], ":")
	}
	return prefix, pattern
}

func findSnapshotByPrefix(snapshots []string, prefix string) []string {
	ret := make([]string, 0)
	for _, snapshot := range snapshots {
		if strings.HasPrefix(snapshot, prefix) {
			ret = append(ret, snapshot)
		}
	}
	return ret
}

func findObjectByPrefix(objects []string, prefix string) []string {
	ret := make([]string, 0)
	for _, snapshot := range objects {
		if strings.HasPrefix(snapshot, prefix) {
			ret = append(ret, snapshot)
		}
	}
	return ret
}
