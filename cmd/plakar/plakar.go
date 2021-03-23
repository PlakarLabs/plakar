package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strings"

	"github.com/poolpOrg/plakar/store"
)

var namespace string
var hostname string
var storeloc string
var quiet bool

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

	pstore := &store.FSStore{}
	pstore.Namespace = namespace
	pstore.Repository = storeloc
	pstore.Init()

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
		log.Fatalf("%s: missing command", flag.CommandLine.Name())
	}

	command, args := flag.Arg(0), flag.Args()[1:]
	switch command {
	case "cat":
		cmd_cat(pstore, args)

	case "diff":
		cmd_diff(pstore, args)

	case "ls":
		cmd_ls(pstore, args)

	case "pull":
		cmd_pull(pstore, args)

	case "push":
		cmd_push(pstore, args)

	case "rm":
		cmd_rm(pstore, args)

	case "verify":
		cmd_verify(pstore, args)

	default:
		log.Fatalf("%s: unsupported command: %s", flag.CommandLine.Name(), command)
	}
}
