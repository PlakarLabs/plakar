package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/denisbrodbeck/machineid"
	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/helpers"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"

	_ "github.com/poolpOrg/plakar/storage/client"
	_ "github.com/poolpOrg/plakar/storage/database"
	_ "github.com/poolpOrg/plakar/storage/fs"
)

type Plakar struct {
	cachePath string

	NumCPU      int
	Hostname    string
	Username    string
	Repository  string
	CommandLine string
	MachineID   string

	Cache *cache.Cache
}

var commands map[string]func(Plakar, *storage.Store, []string) int = make(map[string]func(Plakar, *storage.Store, []string) int)

func registerCommand(command string, fn func(Plakar, *storage.Store, []string) int) {
	commands[command] = fn
}

func executeCommand(ctx Plakar, store *storage.Store, command string, args []string) (int, error) {
	fn, exists := commands[command]
	if !exists {
		return 1, fmt.Errorf("unknown command: %s", command)
	}
	return fn(ctx, store, args), nil
}

func main() {
	os.Exit(entryPoint())
}

func entryPoint() int {
	// default values
	opt_cpuDefault := runtime.GOMAXPROCS(0)
	if opt_cpuDefault != 1 {
		opt_cpuDefault = opt_cpuDefault - 1
	}

	opt_userDefault, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: go away casper !\n", flag.CommandLine.Name())
		return 1
	}

	opt_hostnameDefault, err := os.Hostname()
	if err != nil {
		opt_hostnameDefault = "localhost"
	}

	opt_machineIdDefault, err := machineid.ID()
	if err != nil {
		opt_machineIdDefault = uuid.NewSHA1(uuid.Nil, []byte(opt_hostnameDefault)).String()
	}
	opt_machineIdDefault = strings.ToLower(opt_machineIdDefault)

	opt_usernameDefault := opt_userDefault.Username
	opt_repositoryDefault := path.Join(opt_userDefault.HomeDir, ".plakar")
	opt_cacheDefault := path.Join(opt_userDefault.HomeDir, ".plakar-cache")

	// command line overrides
	var opt_cpuCount int
	var opt_cachedir string
	var opt_username string
	var opt_hostname string
	var opt_cpuProfile string
	var opt_memProfile string
	var opt_nocache bool
	var opt_time bool
	var opt_trace bool
	var opt_verbose bool
	var opt_profiling bool

	flag.StringVar(&opt_cachedir, "cache", opt_cacheDefault, "default cache directory")
	flag.IntVar(&opt_cpuCount, "cpu", opt_cpuDefault, "limit the number of usable cores")
	flag.StringVar(&opt_username, "username", opt_usernameDefault, "default username")
	flag.StringVar(&opt_hostname, "hostname", opt_hostnameDefault, "default hostname")
	flag.StringVar(&opt_cpuProfile, "profile-cpu", "", "profile CPU usage")
	flag.StringVar(&opt_memProfile, "profile-mem", "", "profile MEM usage")
	flag.BoolVar(&opt_nocache, "no-cache", false, "disable caching")
	flag.BoolVar(&opt_time, "time", false, "display command execution time")
	flag.BoolVar(&opt_trace, "trace", false, "display trace logs")
	flag.BoolVar(&opt_verbose, "verbose", false, "display verbose logs")
	flag.BoolVar(&opt_profiling, "profiling", false, "display profiling logs")
	flag.Parse()

	// setup from default + override
	if opt_cpuCount > runtime.NumCPU() {
		fmt.Fprintf(os.Stderr, "%s: can't use more cores than available: %d\n", flag.CommandLine.Name(), runtime.NumCPU())
		return 1
	}
	runtime.GOMAXPROCS(opt_cpuCount)

	if opt_cpuProfile != "" {
		f, err := os.Create(opt_cpuProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not create CPU profile: %d\n", flag.CommandLine.Name(), err)
			return 1
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not start CPU profile: %d\n", flag.CommandLine.Name(), err)
			return 1
		}
		defer pprof.StopCPUProfile()
	}

	ctx := Plakar{}
	ctx.cachePath = opt_cachedir
	ctx.NumCPU = opt_cpuCount
	ctx.Username = opt_username
	ctx.Hostname = opt_hostname
	ctx.Repository = opt_repositoryDefault
	ctx.CommandLine = strings.Join(os.Args, " ")
	ctx.MachineID = opt_machineIdDefault

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "%s: a command must be provided\n", flag.CommandLine.Name())
		return 1
	}

	// start logging
	if opt_verbose {
		logger.EnableInfo()
	}
	if opt_trace {
		logger.EnableTrace()
	}
	if opt_profiling {
		logger.EnableProfiling()
	}
	loggerWait := logger.Start()

	command, args := flag.Args()[0], flag.Args()[1:]
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

	// cmd_init must be ran before workdir.New()
	//if command == "init" {
	//	return cmd_init(ctx, args)
	//}

	// workdir.New() is supposed to work at this point,
	//ctx.Workdir, err = workdir.New(opt_workdir)
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "%s: run `plakar init` first\n", flag.CommandLine.Name())
	//	return 1
	//}
	if !opt_nocache {
		cache.Create(opt_cachedir)
		ctx.Cache = cache.New(opt_cachedir)
	}

	// cmd_create must be ran after workdir.New() but before other commands
	if command == "create" {
		return cmd_create(ctx, args)
	}

	store, err := storage.Open(ctx.Repository)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return 1
	}

	var secret []byte
	if store.Configuration().Encryption != "" {
		for {
			passphrase, err := helpers.GetPassphrase("repository")
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			secret, err = encryption.DeriveSecret(passphrase, store.Configuration().Encryption)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			break
		}
	}

	//
	store.SetSecret(secret)
	store.SetCache(ctx.Cache)
	store.SetUsername(ctx.Username)
	store.SetHostname(ctx.Hostname)
	store.SetCommandLine(ctx.CommandLine)
	store.SetMachineID(ctx.MachineID)

	// commands below all operate on an open store
	t0 := time.Now()
	status, err := executeCommand(ctx, store, command, args)
	t1 := time.Since(t0)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
	}

	if opt_time {
		logger.Printf("time: %s", t1)
	}

	store.Close()

	if !opt_nocache {
		ctx.Cache.Commit()
	}

	loggerWait()

	if opt_memProfile != "" {
		f, err := os.Create(opt_memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not write MEM profile: %d\n", flag.CommandLine.Name(), err)
			return 1
		}
	}

	return status
}
