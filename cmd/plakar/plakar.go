package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/local"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/network"
	"github.com/poolpOrg/plakar/storage"
	"github.com/poolpOrg/plakar/storage/client"
	"github.com/poolpOrg/plakar/storage/database"
	"github.com/poolpOrg/plakar/storage/fs"
)

type Plakar struct {
	Hostname    string
	Username    string
	Workdir     string
	Repository  string
	CommandLine string

	EncryptedKeypair []byte
	keypair          *encryption.Keypair

	store storage.Store

	StdoutChannel  chan string
	StderrChannel  chan string
	VerboseChannel chan string
	TraceChannel   chan string

	localCache *cache.Cache
}

func (plakar *Plakar) Store() storage.Store {
	return plakar.store
}

func (plakar *Plakar) Cache() *cache.Cache {
	return plakar.localCache
}

func (plakar *Plakar) Keypair() *encryption.Keypair {
	return plakar.keypair
}

func main() {
	var enableTime bool
	var enableTracing bool
	var enableInfoOutput bool
	var enableProfiling bool
	var disableCache bool

	ctx := Plakar{}
	currentHostname, err := os.Hostname()
	if err != nil {
		currentHostname = "localhost"
	}

	currentUser, err := user.Current()
	if err != nil {
		log.Fatalf("%s: user %s has turned into Casper", flag.CommandLine.Name(), currentUser.Username)
	}

	flag.BoolVar(&disableCache, "no-cache", false, "disable local cache")
	flag.BoolVar(&enableTime, "time", false, "enable time")
	flag.BoolVar(&enableInfoOutput, "info", false, "enable info output")
	flag.BoolVar(&enableTracing, "trace", false, "enable tracing")
	flag.BoolVar(&enableProfiling, "profile", false, "enable profiling")

	flag.Parse()

	ctx.CommandLine = strings.Join(os.Args, " ")
	fmt.Println(ctx.CommandLine)

	if len(flag.Args()) == 0 {
		log.Fatalf("%s: missing command", flag.CommandLine.Name())
	}

	//
	ctx.Username = currentUser.Username
	ctx.Hostname = currentHostname
	ctx.Workdir = fmt.Sprintf("%s/.plakar", currentUser.HomeDir)
	ctx.Repository = fmt.Sprintf("%s/store", ctx.Workdir)

	// start logger and defer done return function to end of execution

	if enableInfoOutput {
		logger.EnableInfo()
	}
	if enableTracing {
		logger.EnableTrace()
	}
	if enableProfiling {
		logger.EnableProfiling()
	}
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

	if !disableCache {
		ctx.localCache = cache.New(fmt.Sprintf("%s/cache", ctx.Workdir))
	}

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

	var store storage.Store
	if !strings.HasPrefix(ctx.Repository, "/") {
		if strings.HasPrefix(ctx.Repository, "plakar://") {
			network.ProtocolRegister()
			store = &client.ClientStore{}
		} else if strings.HasPrefix(ctx.Repository, "sqlite://") {
			store = &database.DatabaseStore{}
		} else {
			log.Fatalf("%s: unsupported plakar protocol", flag.CommandLine.Name())
		}
	} else {
		store = &fs.FSStore{}
	}
	ctx.store = store

	// create command needs to be handled early _after_ key is available
	if command == "create" {
		cmd_create(ctx, args)
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	err = store.Open(ctx.Repository)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "store does not seem to exist: run `plakar create`\n")
		} else {
			log.Fatalf("%s: could not open repository %s", flag.CommandLine.Name(), ctx.Repository)
		}
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

	ctx.store = store
	ctx.store.SetKeypair(ctx.keypair)
	ctx.store.SetCache(ctx.localCache)

	var exitCode int

	t0 := time.Now()
	switch command {
	case "cat":
		exitCode = cmd_cat(ctx, args)

	case "check":
		exitCode = cmd_check(ctx, args)

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

	case "rm":
		exitCode = cmd_rm(ctx, args)

	case "tarball":
		exitCode = cmd_tarball(ctx, args)

	case "ui":
		exitCode = cmd_ui(ctx, args)

	case "diff":
		exitCode = cmd_diff(ctx, args)

	case "pull":
		exitCode = cmd_pull(ctx, args)

	case "push":
		exitCode = cmd_push(ctx, args)

	case "server":
		exitCode = cmd_server(ctx, args)

	case "version":
		exitCode = cmd_version(ctx, args)

	default:
		log.Fatalf("%s: unsupported command: %s", flag.CommandLine.Name(), command)
	}

	if ctx.localCache != nil {
		ctx.localCache.Commit()
	}

	if enableTime {
		logger.Printf("time: %s", time.Since(t0))
	}

	os.Exit(exitCode)
}
