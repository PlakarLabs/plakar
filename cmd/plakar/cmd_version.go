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
	"encoding/xml"
	"flag"
	"fmt"
	"net/http"
	"runtime"

	"golang.org/x/mod/semver"
	"golang.org/x/tools/blog/atom"
)

const VERSION = "v0.4.16-alpha"

type Release struct {
	Version      string `json:"version"`
	Type         string `json:"type"`
	DownloadURL  string `json:"download_url"`
	ChangelogURL string `json:"changelog_url"`
}

func cmd_version(ctx Plakar, args []string) int {
	var opt_check bool
	var opt_quiet bool
	flags := flag.NewFlagSet("version", flag.ExitOnError)
	flags.BoolVar(&opt_check, "check", false, "check for updates")
	flags.BoolVar(&opt_quiet, "quiet", false, "quiet mode")
	flags.Parse(args)

	if !semver.IsValid(VERSION) {
		panic("invalid version string: " + VERSION)
	}

	if opt_check {
		req, err := http.NewRequest("GET", "https://plakar.io/api/releases.atom", nil)
		if err != nil {
			fmt.Println("Error creating request:", err)
			return 1
		}

		req.Header.Set("User-Agent", fmt.Sprintf("plakar/%s (%s/%s)", VERSION, runtime.GOOS, runtime.GOARCH))

		client := http.Client{}
		res, err := client.Do(req)
		if err != nil {
			fmt.Println("Error playing request:", err)
			return 1
		}
		defer res.Body.Close()

		var feed []atom.Feed
		err = xml.NewDecoder(res.Body).Decode(&feed)
		if err != nil {
			fmt.Println("Error decoding JSON:", err)
			return 1
		}

		// Affichage des releases décodées
		found := false
		foundCount := 0
		foundLatest := ""
		for _, entry := range feed[0].Entry {
			if !semver.IsValid(entry.Title) {
				continue
			}
			if semver.Compare(VERSION, entry.Title) < 0 {
				found = true
				foundCount++
				if foundLatest == "" {
					foundLatest = entry.Title
				} else {
					if semver.Compare(foundLatest, entry.Title) < 0 {
						foundLatest = entry.Title
					}
				}
			}
		}
		if !found {
			if !opt_quiet {
				fmt.Println("You are running the latest version of plakar.")
			}
			return 1
		} else {
			if !opt_quiet {
				if foundCount == 1 {
					fmt.Printf("A new version of plakar is available: %s\n", foundLatest)
				} else {
					fmt.Printf("%d new versions of plakar are available, the latest is %s\n", foundCount, foundLatest)
				}
				fmt.Printf("To upgrade, run `plakar version upgrade`\n")
			}
			return 0
		}

	} else {
		if !opt_quiet {
			fmt.Println(VERSION)
		}
	}

	return 0
}
