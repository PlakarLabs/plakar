package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"runtime"
	"strings"
	"time"

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/local"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"
	_ "github.com/poolpOrg/plakar/storage/client"
	_ "github.com/poolpOrg/plakar/storage/database"
	_ "github.com/poolpOrg/plakar/storage/fs"
)

type Plakar struct {
	Hostname    string
	Username    string
	Workdir     string
	Repository  string
	CommandLine string

	EncryptedKeypair []byte
	keypair          *encryption.Keypair

	store *storage.Store

	StdoutChannel  chan string
	StderrChannel  chan string
	VerboseChannel chan string
	TraceChannel   chan string

	localCache *cache.Cache
}

func (plakar *Plakar) Store() *storage.Store {
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
	var cpuCount int

	ctx := Plakar{}
	currentHostname, err := os.Hostname()
	if err != nil {
		currentHostname = "localhost"
	}

	currentUser, err := user.Current()
	if err != nil {
		log.Fatalf("%s: user %s has turned into Casper", flag.CommandLine.Name(), currentUser.Username)
	}

	cpuDefault := runtime.GOMAXPROCS(0)
	if cpuDefault != 1 {
		cpuDefault = cpuDefault - 1
	}

	flag.BoolVar(&disableCache, "no-cache", false, "disable local cache")
	flag.BoolVar(&enableTime, "time", false, "enable time")
	flag.BoolVar(&enableInfoOutput, "info", false, "enable info output")
	flag.BoolVar(&enableTracing, "trace", false, "enable tracing")
	flag.BoolVar(&enableProfiling, "profile", false, "enable profiling")
	flag.IntVar(&cpuCount, "cpu", cpuDefault, "limit the number of usable cores")
	flag.Parse()

	ctx.CommandLine = strings.Join(os.Args, " ")

	if len(flag.Args()) == 0 {
		log.Fatalf("%s: missing command", flag.CommandLine.Name())
	}

	//
	if cpuCount > runtime.NumCPU() {
		log.Fatalf("%s: can't use more cores than available: %d", flag.CommandLine.Name(), runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(cpuCount)
	}

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
	loggerWait := logger.Start()

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

	var store *storage.Store
	if !strings.HasPrefix(ctx.Repository, "/") {
		if strings.HasPrefix(ctx.Repository, "plakar://") {
			store, _ = storage.New("client")
		} else if strings.HasPrefix(ctx.Repository, "sqlite://") {
			store, _ = storage.New("database")
		} else {
			log.Fatalf("%s: unsupported plakar protocol", flag.CommandLine.Name())
		}
	} else {
		store, _ = storage.New("filesystem")
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
			os.Exit(1)
		} else {
			log.Fatalf("%s: could not open repository %s", flag.CommandLine.Name(), ctx.Repository)
		}
	}

	if store.Configuration().Encryption != "" {
		/* load keypair from plakar */
		encryptedKeypair, err := local.GetEncryptedKeypair(ctx.Workdir)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Fprintf(os.Stderr, "key %s not found, uh oh, emergency !...\n", store.Configuration().Encryption)
				os.Exit(1)
			} else {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				os.Exit(1)
			}
		}
		ctx.EncryptedKeypair = encryptedKeypair

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
	ctx.store.SetUsername(ctx.Username)
	ctx.store.SetHostname(ctx.Hostname)
	ctx.store.SetCommandLine(ctx.CommandLine)

	t0 := time.Now()
	exitCode, err := executeCommand(ctx, command, args)

	if err != nil {
		log.Fatal(err)
	}
	if exitCode == -1 {
		log.Fatalf("%s: unsupported command: %s", flag.CommandLine.Name(), command)
	}

	if ctx.localCache != nil {
		ctx.localCache.Commit()
	}

	if enableTime {
		logger.Printf("time: %s", time.Since(t0))
	}

	ctx.store.Close()

	loggerWait()
	os.Exit(exitCode)
}
