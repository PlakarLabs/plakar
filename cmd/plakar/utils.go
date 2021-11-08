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
	"log"
	"strings"

	"github.com/poolpOrg/plakar/snapshot"
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

func getSnapshotsList(ctx Plakar) []string {
	snapshots, err := snapshot.List(ctx.Store())
	if err != nil {
		log.Fatalf("%s: could not fetch snapshots list", flag.CommandLine.Name())
	}
	return snapshots
}

func checkSnapshotsArgs(snapshots []string) {
	for i := 0; i < len(snapshots); i++ {
		prefix, _ := parseSnapshotID(snapshots[i])
		res := findSnapshotByPrefix(snapshots, prefix)
		if len(res) == 0 {
			log.Fatalf("%s: no snapshot has prefix: %s", flag.CommandLine.Name(), prefix)
		} else if len(res) > 1 {
			log.Fatalf("%s: snapshot ID is ambigous: %s (matches %d snapshots)", flag.CommandLine.Name(), prefix, len(res))
		}
	}
}

func executeCommand(ctx Plakar, command string, args []string) (int, error) {
	var exitCode int

	switch command {
	case "cat":
		exitCode = cmd_cat(ctx, args)

	case "check":
		exitCode = cmd_check(ctx, args)

	case "diff":
		exitCode = cmd_diff(ctx, args)

	case "find":
		exitCode = cmd_find(ctx, args)

	case "info":
		exitCode = cmd_info(ctx, args)

	case "keep":
		exitCode = cmd_keep(ctx, args)

	case "key":
		exitCode = cmd_key(ctx, args)

	case "ls":
		exitCode = cmd_ls(ctx, args)

	case "pull":
		exitCode = cmd_pull(ctx, args)

	case "push":
		exitCode = cmd_push(ctx, args)

	case "rm":
		exitCode = cmd_rm(ctx, args)

	case "search":
		exitCode = cmd_search(ctx, args)

	case "server":
		exitCode = cmd_server(ctx, args)

	case "shell":
		exitCode = cmd_shell(ctx, args)

	case "tarball":
		exitCode = cmd_tarball(ctx, args)

	case "ui":
		exitCode = cmd_ui(ctx, args)

	case "version":
		exitCode = cmd_version(ctx, args)

	default:
		return -1, nil
	}

	return exitCode, nil
}
