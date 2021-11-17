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
	"log"

	"github.com/poolpOrg/plakar/logger"
)

func cmd_check(ctx Plakar, args []string) int {
	var enableFastCheck bool

	flags := flag.NewFlagSet("check", flag.ExitOnError)
	flags.BoolVar(&enableFastCheck, "fast", false, "enable fast checking (no checksum verification)")
	flags.Parse(args)

	if flags.NArg() == 0 {
		logger.Error("%s: at least one parameter is required", flags.Name())
		return 1
	}

	snapshots, err := getSnapshots(ctx.Store(), flags.Args())
	if err != nil {
		log.Fatal(err)
	}

	failures := false
	for offset, snapshot := range snapshots {
		_, pattern := parseSnapshotID(flags.Args()[offset])

		ok, err := snapshot.Check(pattern, enableFastCheck)
		if err != nil {
			logger.Warn("%s", err)
		}

		if ok {
			logger.Info("%s: OK", snapshot.Uuid)
		} else {
			logger.Info("%s: KO", snapshot.Uuid)
			failures = true
		}
	}

	if !failures {
		return 0
	}
	return 1
}
