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
	"log"
	"os"
	"os/user"
	"strings"

	"github.com/poolpOrg/plakar/repository"
	"github.com/poolpOrg/plakar/repository/client"
	"github.com/poolpOrg/plakar/repository/fs"
)

var namespace string
var hostname string
var storeloc string
var quiet bool

const VERSION = "0.0.1"

func main() {

	hostbuf, err := os.Hostname()
	if err != nil {
		hostbuf = "localhost"
	}

	pwUser, err := user.Current()
	if err != nil {
		log.Fatalf("%s: user %s has turned into Casper", flag.CommandLine.Name(), pwUser.Username)
	}

	flag.StringVar(&storeloc, "store", fmt.Sprintf("%s/.plakar", pwUser.HomeDir), "data store")
	flag.StringVar(&namespace, "namespace", "default", "storage namespace")
	flag.StringVar(&hostname, "hostname", strings.ToLower(hostbuf), "local hostname")
	flag.BoolVar(&quiet, "quiet", false, "quiet mode")

	flag.Parse()

	namespace = strings.ToLower(namespace)
	hostname = strings.ToLower(hostname)

	if len(flag.Args()) == 0 {
		fmt.Println("valid subcommands:")
		fmt.Println("\tcat <snapshot>:<file>")
		fmt.Println("\tcat <snapshot>:<object>")
		fmt.Println("\tcheck <snapshot> [<snapshot>]")
		fmt.Println("\tdiff <snapshot> <snapshot>")
		fmt.Println("\tdiff <snapshot> <snapshot> <file>")
		fmt.Println("\tls <snapshot> <snapshot> <file>")
		fmt.Println("\tpull <snapshot> [<snapshot> ...]")
		fmt.Println("\tpush <path> [<path> ...]")
		fmt.Println("\trm <snapshot> [<snapshot> ...]")
		fmt.Println("\tversion")
		log.Fatalf("%s: missing command", flag.CommandLine.Name())
	}

	command, args := flag.Arg(0), flag.Args()[1:]

	if len(args) > 1 {
		if command != "init" {
			if command == "push" {
				if args[len(args)-2] == "to" {
					storeloc = args[len(args)-1]
					args = args[:len(args)-2]
				}
			} else {
				if args[len(args)-2] == "from" {
					storeloc = args[len(args)-1]
					args = args[:len(args)-2]
				}
			}
		}
	}

	var store repository.Store
	if strings.HasPrefix(storeloc, "plakar://") {
		pstore := &client.ClientStore{}
		pstore.Namespace = namespace
		pstore.Repository = storeloc
		store = pstore

	} else {
		pstore := &fs.FSStore{}
		pstore.Namespace = namespace
		pstore.Repository = storeloc
		store = pstore
	}
	store.Init()

	switch command {
	case "cat":
		cmd_cat(store, args)

	case "check":
		cmd_check(store, args)

	case "diff":
		cmd_diff(store, args)

	case "ls":
		cmd_ls(store, args)

	case "pull":
		cmd_pull(store, args)

	case "push":
		cmd_push(store, args)

	case "rm":
		cmd_rm(store, args)

	case "server":
		cmd_server(store, args)

	case "ui":
		cmd_ui(store, args)

	case "version":
		cmd_version(store, args)

	default:
		log.Fatalf("%s: unsupported command: %s", flag.CommandLine.Name(), command)
	}
}
