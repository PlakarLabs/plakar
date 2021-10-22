package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"

	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/local"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage/fs"
)

type Plakar struct {
	Hostname   string
	Username   string
	Workdir    string
	Repository string

	EncryptedKeypair []byte
	keypair          *encryption.Keypair

	store *fs.FSStore

	StdoutChannel  chan string
	StderrChannel  chan string
	VerboseChannel chan string
	TraceChannel   chan string
}

func (plakar *Plakar) Store() *fs.FSStore {
	return plakar.store
}

func (plakar *Plakar) Keypair() *encryption.Keypair {
	return plakar.keypair
}

func main() {
	ctx := Plakar{}

	currentHostname, err := os.Hostname()
	if err != nil {
		currentHostname = "localhost"
	}

	currentUser, err := user.Current()
	if err != nil {
		logger.Stderr()
		log.Fatalf("%s: user %s has turned into Casper", flag.CommandLine.Name(), currentUser.Username)
	}

	flag.Parse()
	if len(flag.Args()) == 0 {
		log.Fatalf("%s: missing command", flag.CommandLine.Name())
	}

	//
	ctx.Username = currentUser.Username
	ctx.Hostname = currentHostname
	ctx.Workdir = fmt.Sprintf("%s/.plakar", currentUser.HomeDir)
	ctx.Repository = fmt.Sprintf("%s/store", ctx.Workdir)

	// start logger and defer done return function to end of execution
	defer logger.Start()()

	command, args := flag.Arg(0), flag.Args()[1:]

	if flag.Arg(0) == "on" {
		if len(flag.Args()) < 2 {
			log.Fatalf("%s: missing plakar repository", flag.CommandLine.Name())
		}
		if len(flag.Args()) < 3 {
			log.Fatalf("%s: missing command", flag.CommandLine.Name())
		}
		ctx.Repository = flag.Arg(1)
		command, args = flag.Arg(2), flag.Args()[3:]
	}

	local.Init(ctx.Workdir)

	/* keygen command needs to be handled very early */
	if command == "keygen" {
		err = cmd_keygen(ctx, args)
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	/* load keypair from plakar */
	encryptedKeypair, err := local.GetEncryptedKeypair(ctx.Workdir)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "key not found, run `plakar keygen`\n")
			os.Exit(1)
		} else {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			os.Exit(1)
		}
	}
	ctx.EncryptedKeypair = encryptedKeypair

	// create command needs to be handled early _after_ key is available
	if command == "create" {
		cmd_create(ctx, args)
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	store := fs.FSStore{}
	err = store.Open(ctx.Repository)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "store does not seem to exist: run `plakar create`\n")
		} else {
			fmt.Fprintf(os.Stderr, "%s\n", err)
		}
		return
	}

	if store.Configuration().Encrypted != "" {
		var keypair *encryption.Keypair
		for {
			passphrase, err := helpers.GetPassphrase()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			keypair, err = encryption.Keyload(passphrase, encryptedKeypair)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}
			break
		}
		ctx.keypair = keypair
	}

	ctx.store = &store
	ctx.store.Keypair = ctx.keypair

	switch command {
	case "cat":
		cmd_cat(ctx, args)

	case "check":
		cmd_check(ctx, args)

	case "find":
		cmd_find(ctx, args)

	case "info":
		cmd_info(ctx, args)

	case "key":
		cmd_key(ctx, args)

	case "ls":
		cmd_ls(ctx, args)

	case "rm":
		cmd_rm(ctx, args)

	case "tarball":
		cmd_tarball(ctx, args)

	case "ui":
		cmd_ui(ctx, args)

	case "diff":
		cmd_diff(ctx, args)

	case "pull":
		cmd_pull(ctx, args)

	case "push":
		cmd_push(ctx, args)

	case "version":
		cmd_version(ctx, args)

	default:
		log.Fatalf("%s: unsupported command: %s", flag.CommandLine.Name(), command)
	}

	//	loggerDone()
}
