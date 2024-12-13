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

package cat

import (
	"bufio"
	"compress/gzip"
	"flag"
	"io"
	"os"

	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/cmd/plakar/utils"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/snapshot/vfs"
	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
)

func init() {
	subcommands.Register("cat", cmd_cat)
}

func cmd_cat(ctx *context.Context, repo *repository.Repository, args []string) int {
	var opt_nodecompress bool
	var opt_highlight bool

	flags := flag.NewFlagSet("cat", flag.ExitOnError)
	flags.BoolVar(&opt_nodecompress, "no-decompress", false, "do not try to decompress output")
	flags.BoolVar(&opt_highlight, "highlight", false, "highlight output")
	flags.Parse(args)

	if flags.NArg() == 0 {
		ctx.GetLogger().Error("%s: at least one parameter is required", flags.Name())
		return 1
	}

	snapshots, err := utils.GetSnapshots(repo, flags.Args())
	if err != nil {
		ctx.GetLogger().Error("%s: could not obtain snapshots list: %s", flags.Name(), err)
		return 1
	}

	errors := 0
	for offset, snap := range snapshots {
		_, pathname := utils.ParseSnapshotID(flags.Args()[offset])

		if pathname == "" {
			ctx.GetLogger().Error("%s: missing filename for snapshot", flags.Name())
			errors++
			continue
		}

		fs, err := snap.Filesystem()
		if err != nil {
			ctx.GetLogger().Error("%s: %s: %s", flags.Name(), pathname, err)
			errors++
			continue
		}

		st, err := fs.Stat(pathname)
		if err != nil {
			ctx.GetLogger().Error("%s: %s: no such file", flags.Name(), pathname)
			errors++
			continue
		}

		if !st.Stat().Mode().IsRegular() {
			ctx.GetLogger().Error("%s: %s: not a regular file", flags.Name(), pathname)
			errors++
			continue
		}
		fileEntry := st.(*vfs.FileEntry)

		rd, err := snap.NewReader(pathname)
		if err != nil {
			ctx.GetLogger().Error("%s: %s: failed to open: %s", flags.Name(), pathname, err)
			errors++
			continue
		}

		var outRd io.ReadCloser = rd

		if !opt_nodecompress {
			if fileEntry.Object.ContentType == "application/gzip" && !opt_nodecompress {
				gzRd, err := gzip.NewReader(outRd)
				if err != nil {
					ctx.GetLogger().Error("%s: %s: %s", flags.Name(), pathname, err)
					errors++
					continue
				}
				outRd = gzRd
			}
		}

		if opt_highlight {
			lexer := lexers.Match(pathname)
			if lexer == nil {
				lexer = lexers.Get(fileEntry.Object.ContentType)
			}
			if lexer == nil {
				lexer = lexers.Fallback // Fallback if no lexer is found
			}
			formatter := formatters.Get("terminal")
			style := styles.Get("dracula")

			reader := bufio.NewReader(rd)
			buffer := make([]byte, 4096) // Fixed-size buffer for chunked reading
			for {
				n, err := reader.Read(buffer) // Read up to the size of the buffer
				if n > 0 {
					chunk := string(buffer[:n])

					// Tokenize the chunk and apply syntax highlighting
					iterator, errTokenize := lexer.Tokenise(nil, chunk)
					if errTokenize != nil {
						ctx.GetLogger().Error("%s: %s: %s", flags.Name(), pathname, errTokenize)
						errors++
						break
					}

					errFormat := formatter.Format(os.Stdout, style, iterator)
					if errFormat != nil {
						ctx.GetLogger().Error("%s: %s: %s", flags.Name(), pathname, errFormat)
						errors++
						break
					}
				}

				// Check for end of file (EOF)
				if err == io.EOF {
					break
				} else if err != nil {
					ctx.GetLogger().Error("%s: %s: %s", flags.Name(), pathname, err)
					errors++
					break
				}
			}

		} else {
			_, err = io.Copy(os.Stdout, outRd)
		}
		if err != nil {
			ctx.GetLogger().Error("%s: %s: %s", flags.Name(), pathname, err)
			errors++
			continue
		}
	}

	if errors != 0 {
		return 1
	}
	return 0
}
