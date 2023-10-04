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

package encryption

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestEncryption(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	buffer := make([]byte, 65*1024)
	rand.Read(buffer)

	encrypted, err := Encrypt(key, buffer)
	if err != nil {
		t.Error(err)
	}

	decrypted, err := Decrypt(key, encrypted)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(decrypted, buffer) {
		t.Errorf("Decrypt(Encrypt(buffer)) != buffer")
	}
}
