package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strings"

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

	store := &fs.FSStore{}
	store.Namespace = namespace
	store.Repository = storeloc
	store.Init()

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

	case "version":
		cmd_version(store, args)

	default:
		log.Fatalf("%s: unsupported command: %s", flag.CommandLine.Name(), command)
	}
}
