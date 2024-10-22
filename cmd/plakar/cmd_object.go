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
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/PlakarLabs/plakar/objects"
	"github.com/PlakarLabs/plakar/repository"
)

func init() {
	registerCommand("object", cmd_object)
}

func cmd_object(ctx Plakar, repo *repository.Repository, args []string) int {
	flags := flag.NewFlagSet("packfile", flag.ExitOnError)
	flags.Parse(args)

	for _, arg := range flags.Args() {
		// convert arg to [32]byte
		if len(arg) != 64 {
			log.Fatalf("invalid packfile hash: %s", arg)
		}

		b, err := hex.DecodeString(arg)
		if err != nil {
			log.Fatalf("invalid packfile hash: %s", arg)
		}

		// Convert the byte slice to a [32]byte
		var byteArray [32]byte
		copy(byteArray[:], b)

		packfileID, offset, length, exists := repo.State().GetSubpartForObject(byteArray)
		if !exists {
			log.Fatal(err)
		}

		rd, _, err := repo.GetPackfileBlob(packfileID, offset, length)
		if err != nil {
			log.Fatal(err)
		}

		blob, err := io.ReadAll(rd)
		if err != nil {
			log.Fatal(err)
		}

		object, err := objects.NewObjectFromBytes(blob)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("object: %x\n", object.Checksum)
		fmt.Println("  type:", object.ContentType)
		if len(object.Tags) > 0 {
			fmt.Println("  tags:", strings.Join(object.Tags, ","))
		}

		fmt.Println("  chunks:")
		for _, chunk := range object.Chunks {
			fmt.Printf("    checksum: %x\n", chunk.Checksum)
		}
	}

	return 0
}
