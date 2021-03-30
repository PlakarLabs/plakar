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
	"syscall"

	"github.com/poolpOrg/plakar"
	"github.com/poolpOrg/plakar/repository"
	"github.com/poolpOrg/plakar/repository/client"
	"github.com/poolpOrg/plakar/repository/encryption"
	"github.com/poolpOrg/plakar/repository/fs"
	"github.com/poolpOrg/plakar/repository/local"
	"golang.org/x/crypto/ssh/terminal"
)

var localdir string
var hostname string
var storeloc string
var quiet bool
var skipKeygen bool
var cleartext bool

const VERSION = "0.0.1"

func keypairGenerate() ([]byte, error) {
	keypair, err := encryption.Keygen()
	if err != nil {
		return nil, err
	}

	passphrase := []byte("")
	for {
		fmt.Printf("passphrase: ")
		passphrase1, _ := terminal.ReadPassword(syscall.Stdin)
		fmt.Printf("\npassphrase (confirm): ")
		passphrase2, _ := terminal.ReadPassword(syscall.Stdin)
		if string(passphrase1) != string(passphrase2) {
			fmt.Printf("\npassphrases mismatch, try again.\n")
			continue
		}
		fmt.Printf("\n")
		passphrase = passphrase1
		break
	}
	pem, err := keypair.Encrypt(passphrase)
	if err != nil {
		return nil, err
	}

	return pem, err
}

func main() {
	ctx := plakar.Plakar{}

	hostbuf, err := os.Hostname()
	if err != nil {
		hostbuf = "localhost"
	}

	pwUser, err := user.Current()
	if err != nil {
		log.Fatalf("%s: user %s has turned into Casper", flag.CommandLine.Name(), pwUser.Username)
	}

	flag.StringVar(&localdir, "local", fmt.Sprintf("%s/.plakar", pwUser.HomeDir), "local store")
	flag.StringVar(&storeloc, "store", fmt.Sprintf("%s/.plakar", pwUser.HomeDir), "data store")
	flag.StringVar(&hostname, "hostname", strings.ToLower(hostbuf), "local hostname")
	flag.BoolVar(&quiet, "quiet", false, "quiet mode")
	flag.BoolVar(&skipKeygen, "skip-keygen", false, "skip keypair generation")
	flag.BoolVar(&cleartext, "cleartext", false, "disable encryption")

	flag.Parse()

	/* first thing first, initialize a plakar repository if none */
	local.Init(localdir)

	/* load keypair from plakar */
	data, err := local.GetEncryptedKeypair(localdir)
	if err != nil && !skipKeygen {
		if !os.IsNotExist(err) {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println("generating plakar keypair")
		data, err = keypairGenerate()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		err = local.SetEncryptedKeypair(localdir, data)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("keypair saved to local store")
	}

	var keypair *encryption.Keypair
	for {
		fmt.Printf("passphrase: ")
		passphrase, _ := terminal.ReadPassword(syscall.Stdin)
		keypair, err = encryption.Keyload(passphrase, data)
		if err != nil {
			fmt.Println()
			fmt.Println(err)
			continue
		}
		fmt.Println()
		break
	}

	/* PlakarCTX */
	ctx.Hostname = strings.ToLower(hostname)
	ctx.Username = pwUser.Username
	ctx.Keypair = keypair
	ctx.DisableEncryption = cleartext

	if len(flag.Args()) == 0 {
		log.Fatalf("%s: missing command", flag.CommandLine.Name())
	}

	command, args := flag.Arg(0), flag.Args()[1:]

	if len(args) > 1 {
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

	var store repository.Store
	if strings.HasPrefix(storeloc, "plakar://") {
		pstore := &client.ClientStore{}
		pstore.Ctx = &ctx
		pstore.Repository = storeloc
		store = pstore

	} else {
		pstore := &fs.FSStore{}
		pstore.Ctx = &ctx
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
