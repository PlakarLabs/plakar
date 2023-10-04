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
)

func cmd_config(ctx Plakar, args []string) int {
	flags := flag.NewFlagSet("config", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		log.Fatal("invalid number of arguments")
	}

	api := ctx.Config

	subcommand, parameters := flags.Arg(0), flags.Args()[1:]
	if subcommand == "global" {
		switch len(parameters) {
		case 0:
			api.ListGlobalParameters()
		case 1:
			api.GetGlobalParameter(parameters[0])
		case 2:
			api.SetGlobalParameter(parameters[0], parameters[1])
		default:
			log.Fatal("invalid number of arguments")
		}
		return 0
	}

	if subcommand == "repository" {
		switch len(parameters) {
		case 2:
			value, err := api.GetRepositoryParameter(parameters[0], parameters[1])
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(parameters[1], "=", value)

		case 3:
			err := api.SetRepositoryParameter(parameters[0], parameters[1], parameters[2])
			if err != nil {
				log.Fatal(err)
			}
		default:
			log.Fatal("invalid number of arguments")
		}
		return 0
	}

	log.Fatal("invalid subcommand")
	return 1
}
