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
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/poolpOrg/plakar/storage"
)

func init() {
	registerCommand("metadata", cmd_metadata)
}

func cmd_metadata(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("metadata", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		return info_plakar(repository)
	}

	metadatas, err := getMetadatas(repository, flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	for _, metadata := range metadatas {
		serialized, err := json.Marshal(metadata)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(serialized))
	}

	return 0
}
