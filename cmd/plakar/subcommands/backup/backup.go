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

package backup

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/identity"
	"github.com/PlakarKorp/plakar/logger"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot"
	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/dustin/go-humanize"
	"github.com/gobwas/glob"
	"github.com/google/uuid"
)

func init() {
	subcommands.Register("backup", cmd_backup)
}

type excludeFlags []string

func (e *excludeFlags) String() string {
	return strings.Join(*e, ",")
}

func (e *excludeFlags) Set(value string) error {
	*e = append(*e, value)
	return nil
}

func cmd_backup(ctx *context.Context, repo *repository.Repository, args []string) int {
	var opt_tags string
	var opt_excludes string
	var opt_exclude excludeFlags
	var opt_concurrency uint64
	var opt_quiet bool
	var opt_identity string

	excludes := []glob.Glob{}
	flags := flag.NewFlagSet("backup", flag.ExitOnError)
	flags.Uint64Var(&opt_concurrency, "concurrency", uint64(ctx.GetNumCPU())*8+1, "maximum number of parallel tasks")
	flags.StringVar(&opt_identity, "identity", "", "use identity from keyring")
	flags.StringVar(&opt_tags, "tag", "", "tag to assign to this snapshot")
	flags.StringVar(&opt_excludes, "excludes", "", "file containing a list of exclusions")
	flags.Var(&opt_exclude, "exclude", "file containing a list of exclusions")
	flags.BoolVar(&opt_quiet, "quiet", false, "suppress output")
	flags.Parse(args)

	go eventsProcessorStdio(ctx, opt_quiet)

	for _, item := range opt_exclude {
		excludes = append(excludes, glob.MustCompile(item))
	}

	if opt_excludes != "" {
		fp, err := os.Open(opt_excludes)
		if err != nil {
			logger.Error("%s", err)
			return 1
		}
		defer fp.Close()

		scanner := bufio.NewScanner(fp)
		for scanner.Scan() {
			pattern, err := glob.Compile(scanner.Text())
			if err != nil {
				logger.Error("%s", err)
				return 1
			}
			excludes = append(excludes, pattern)
		}
		if err := scanner.Err(); err != nil {
			logger.Error("%s", err)
			return 1
		}
	}
	_ = excludes

	snapshotUUID := uuid.Must(uuid.NewRandom())
	snapshotID, err := snapshotUUID.MarshalBinary()
	if err != nil {
		logger.Error("%s", err)
		return 1
	}

	snap, err := snapshot.New(repo, repo.Checksum(snapshotID))
	if err != nil {
		logger.Error("%s", err)
		return 1
	}

	identityID := os.Getenv("PLAKAR_IDENTITY")
	if opt_identity != "" {
		identityID = opt_identity
	}
	if identityID != "" {
		parsedID, err := uuid.Parse(identityID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: invalid identity: %s\n", flag.CommandLine.Name(), err)
			return 1
		}

		id, err := identity.UnsealIdentity(ctx.GetKeyringDir(), parsedID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: could not unseal identity: %s\n", flag.CommandLine.Name(), err)
			return 1
		}
		ctx.SetIdentity(id.Identifier)
		ctx.SetKeypair(&id.KeyPair)
	}

	if ctx.GetIdentity() != uuid.Nil {
		snap.Header.Identity.Identifier = ctx.GetIdentity()
		snap.Header.Identity.PublicKey = ctx.GetKeypair().PublicKey
	} else {
		logger.Warn("no identity set, snapshot will not be signed")
		logger.Warn("consider using 'plakar id' to create an identity")
	}

	snap.Header.SetContext("Hostname", ctx.GetHostname())
	snap.Header.SetContext("Username", ctx.GetUsername())
	snap.Header.SetContext("OperatingSystem", ctx.GetOperatingSystem())
	snap.Header.SetContext("MachineID", ctx.GetMachineID())
	snap.Header.SetContext("CommandLine", ctx.GetCommandLine())
	snap.Header.SetContext("ProcessID", fmt.Sprintf("%d", ctx.GetProcessID()))
	snap.Header.SetContext("Architecture", ctx.GetArchitecture())
	snap.Header.SetContext("NumCPU", fmt.Sprintf("%d", runtime.NumCPU()))
	snap.Header.SetContext("GOMAXPROCS", fmt.Sprintf("%d", runtime.GOMAXPROCS(0)))
	snap.Header.SetContext("Client", "plakar/"+utils.GetVersion())

	var tags []string
	if opt_tags == "" {
		tags = []string{}
	} else {
		tags = []string{opt_tags}
	}
	snap.Header.Tags = tags

	opts := &snapshot.PushOptions{
		MaxConcurrency: opt_concurrency,
		Excludes:       excludes,
	}

	if flags.NArg() == 0 {
		err = snap.Backup(ctx.GetCWD(), opts)
	} else if flags.NArg() == 1 {
		var cleanPath string

		if !strings.HasPrefix(flags.Arg(0), "/") {
			_, err := importer.NewImporter(flags.Arg(0))
			if err != nil {
				cleanPath = path.Clean(ctx.GetCWD() + "/" + flags.Arg(0))
			} else {
				cleanPath = flags.Arg(0)
			}
		} else {
			cleanPath = path.Clean(flags.Arg(0))
		}
		err = snap.Backup(cleanPath, opts)
	} else {
		log.Fatal("only one directory pushable")
	}

	if err != nil {
		logger.Error("failed to create snapshot: %s", err)
		return 1
	}

	signedStr := "unsigned"
	if ctx.GetIdentity() != uuid.Nil {
		signedStr = "signed"
	}
	logger.Info("created %s snapshot %x with root %s of size %s in %s",
		signedStr,
		snap.Header.GetIndexShortID(),
		base64.RawStdEncoding.EncodeToString(snap.Header.Root[:]),
		humanize.Bytes(snap.Header.Summary.Directory.Size+snap.Header.Summary.Below.Size),
		snap.Header.Duration)
	return 0
}
