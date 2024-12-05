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

package cmd_agent

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/PlakarKorp/plakar/agent"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands"
	"github.com/PlakarKorp/plakar/context"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	subcommands.Register("agent", cmd_agent)
}

type Message struct {
	Type string
	Body string
}

func cmd_agent(ctx *context.Context, _ *repository.Repository, args []string) int {
	logger := ctx.Logger
	flags := flag.NewFlagSet("agent", flag.ExitOnError)
	flags.Parse(args)

	srv, err := agent.NewAgent(ctx, "unix", filepath.Join(ctx.GetCacheDir(), "agent.sock"))
	if err != nil {
		logger.Error("failed to start agent: %v", err)
		return 1
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		srv.Close()
		os.Exit(0)
	}()

	srv.Run(handleConnection)
	return 0
}

type LineWriter struct {
	buf    bytes.Buffer
	mu     sync.Mutex        // Ensure thread-safe writes
	copyFn func(line string) // Function to call for each line
}

func NewLineWriter(copyFn func(line string)) *LineWriter {
	return &LineWriter{copyFn: copyFn}
}

func (lw *LineWriter) Write(p []byte) (n int, err error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	// Write data to buffer
	n, err = lw.buf.Write(p)
	if err != nil {
		return n, err
	}

	// Process complete lines
	scanner := bufio.NewScanner(&lw.buf)
	for scanner.Scan() {
		line := scanner.Text()
		lw.copyFn(line) // Call the provided function for each line
	}

	// Preserve the remaining data in the buffer (incomplete lines)
	if err := scanner.Err(); err == nil {
		lw.buf.Reset()
		lw.buf.Write(scanner.Bytes())
	}

	return n, nil
}

// handleConnection handles client requests
func handleConnection(ctx *context.Context, session *agent.Session) {
	logger := ctx.Logger
	logger.Info("client connected")
	defer logger.Info("client disconnected")

	command, err := session.Read()
	if err != nil {
		if err.Error() == "EOF" {
			return
		}
		fmt.Printf("error decoding message: %v\n", err)
		return
	}

	var commandCtx context.Context
	if err := msgpack.Unmarshal(command.Ctx, &commandCtx); err != nil {
		fmt.Printf("error decoding context: %v\n", err)
		return
	}

	commandCtx.Logger = logging.NewLogger(
		&LineWriter{copyFn: func(line string) {
			session.Stdout(line)
		}},
		&LineWriter{copyFn: func(line string) {
			session.Stderr(line)
		}},
	)

	go func() {
		for _ = range ctx.Events.Listen() {
			//logger.Info("event: %T", event)
		}
	}()

	var repo *repository.Repository
	if command.Repository != "" {
		store, err := storage.Open(&commandCtx, command.Repository)
		if err != nil {
			session.Result(err)
			return
		}

		tmp, err := repository.New(store, nil)
		if err != nil {
			session.Result(err)
			return
		}

		repo = tmp
	}

	ret, err := subcommands.Execute(false, &commandCtx, repo, command.Argv[0], command.Argv[1:])
	session.Result(err)
	_ = ret

}
