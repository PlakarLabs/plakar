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

package helpers

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/term"
)

func GetPassphrase(prefix string) ([]byte, error) {
	fmt.Fprintf(os.Stderr, "%s passphrase: ", prefix)
	passphrase, err := term.ReadPassword(syscall.Stdin)
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}
	return passphrase, nil
}

func GetPassphraseConfirm(prefix string) ([]byte, error) {
	fmt.Fprintf(os.Stderr, "%s passphrase: ", prefix)
	passphrase1, err := term.ReadPassword(syscall.Stdin)
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(os.Stderr, "%s passphrase (confirm): ", prefix)
	passphrase2, err := term.ReadPassword(syscall.Stdin)
	fmt.Fprintf(os.Stderr, "\n")
	if err != nil {
		return nil, err
	}

	if string(passphrase1) != string(passphrase2) {
		return nil, errors.New("passphrases mismatch")
	}

	return passphrase1, nil
}
