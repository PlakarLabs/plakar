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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/PlakarLabs/plakar/compression"
	"github.com/PlakarLabs/plakar/encryption"
	"github.com/PlakarLabs/plakar/packfile"
	"github.com/PlakarLabs/plakar/storage"
)

func init() {
	registerCommand("packfile", cmd_packfile)
}

func cmd_packfile(ctx Plakar, repository *storage.Repository, args []string) int {
	flags := flag.NewFlagSet("packfile", flag.ExitOnError)
	flags.Parse(args)

	if flags.NArg() == 0 {
		packfiles, err := repository.GetPackfiles()
		if err != nil {
			log.Fatal(err)
		}

		for _, packfile := range packfiles {
			fmt.Printf("%x\n", packfile)
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

			rawPackfile, err := repository.GetPackfile(byteArray)
			if err != nil {
				log.Fatal(err)
			}

			version := rawPackfile[len(rawPackfile)-2]
			footerOffset := rawPackfile[len(rawPackfile)-1]
			rawPackfile = rawPackfile[:len(rawPackfile)-2]

			_ = version

			footerbuf := rawPackfile[len(rawPackfile)-int(footerOffset):]
			rawPackfile = rawPackfile[:len(rawPackfile)-int(footerOffset)]

			secret := repository.GetSecret()

			decryptedFooter := footerbuf
			if secret != nil {
				// Decrypt the packfile
				decryptedFooter, err = encryption.Decrypt(secret, footerbuf)
				if err != nil {
					log.Fatal(err)
				}
			}
			if repository.Configuration().Compression != "" {
				// Decompress the packfile
				decryptedFooter, err = compression.Inflate(repository.Configuration().Compression, decryptedFooter)
				if err != nil {
					log.Fatal(err)
				}
			}
			footer, err := packfile.NewFooterFromBytes(decryptedFooter)
			if err != nil {
				log.Fatal(err)
			}

			indexbuf := rawPackfile[int(footer.IndexOffset):]
			rawPackfile = rawPackfile[:int(footer.IndexOffset)]

			decryptedIndex := indexbuf
			if secret != nil {
				// Decrypt the packfile
				decryptedIndex, err = encryption.Decrypt(secret, indexbuf)
				if err != nil {
					log.Fatal(err)
				}
			}
			if repository.Configuration().Compression != "" {
				// Decompress the packfile
				decryptedIndex, err = compression.Inflate(repository.Configuration().Compression, decryptedIndex)
				if err != nil {
					log.Fatal(err)
				}
			}

			hasher := sha256.New()
			hasher.Write(decryptedIndex)

			if !bytes.Equal(hasher.Sum(nil), footer.IndexChecksum[:]) {
				log.Fatal("index checksum mismatch")
			}

			index, err := packfile.NewIndexFromBytes(decryptedIndex)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(footer)
			fmt.Println(index)

			rawPackfile = append(rawPackfile, decryptedIndex...)
			rawPackfile = append(rawPackfile, decryptedFooter...)

			p, err := packfile.NewFromBytes(rawPackfile)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Version: %d.%d.%d\n", p.Footer.Version/100, p.Footer.Version%100/10, p.Footer.Version%10)
			fmt.Printf("Timestamp: %s\n", time.Unix(0, p.Footer.Timestamp))
			fmt.Printf("Index checksum: %x\n", p.Footer.IndexChecksum)
			fmt.Println()

			for i, entry := range p.Index {
				fmt.Printf("blob[%d]: %x %d %d %s\n", i, entry.Checksum, entry.Offset, entry.Length, entry.TypeName())
			}
		}
	}

	return 0
}
