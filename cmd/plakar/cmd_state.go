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
	"log"

	"github.com/PlakarLabs/plakar/repository"
	"github.com/PlakarLabs/plakar/repository/state"
)

func init() {
	registerCommand("state", cmd_state)
}

func cmd_state(ctx Plakar, repo *repository.Repository, args []string) int {
	flags := flag.NewFlagSet("packfile", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		states, err := repo.GetStates()
		if err != nil {
			log.Fatal(err)
		}

		for _, state := range states {
			fmt.Printf("%x\n", state)
		}
	} else {
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

			rawState, err := repo.GetState(byteArray)
			if err != nil {
				log.Fatal(err)
			}

			st, err := state.NewFromBytes(rawState)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Version: %d.%d.%d\n", st.Metadata.Version/100, (st.Metadata.Version/10)%10, st.Metadata.Version%10)
			fmt.Printf("Creation: %s\n", st.Metadata.CreationTime)
			if len(st.Metadata.Extends) > 0 {
				fmt.Printf("Extends:\n")
				for _, stateID := range st.Metadata.Extends {
					fmt.Printf("  %x\n", stateID)
				}
			}

			fmt.Println(st.Snapshots)

			for snapshotID, subpart := range st.Snapshots {
				fmt.Printf("snapshot %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[snapshotID],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for chunk, subpart := range st.Chunks {
				fmt.Printf("chunk %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[chunk],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for object, subpart := range st.Objects {
				fmt.Printf("object %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[object],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for file, subpart := range st.Files {
				fmt.Printf("file %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[file],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for directory, subpart := range st.Directories {
				fmt.Printf("directory %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[directory],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}

			for data, subpart := range st.Datas {
				fmt.Printf("data %x : packfile %x, offset %d, length %d\n",
					st.IdToChecksum[data],
					st.IdToChecksum[subpart.Packfile],
					subpart.Offset,
					subpart.Length)
			}
		}
	}

	return 0
}
