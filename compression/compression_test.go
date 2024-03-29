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

package compression

import (
	"crypto/rand"
	"testing"
)

func TestCompressionGzip(t *testing.T) {
	token := make([]byte, 65*1024)
	rand.Read(token)
	deflated, _ := Deflate("gzip", token)
	inflated, err := Inflate("gzip", deflated)
	if err != nil {
		t.Errorf("Inflate(Deflate(%q)) != %q", inflated, token)
	}
}

func TestCompressionLZ4(t *testing.T) {
	token := make([]byte, 65*1024)
	rand.Read(token)
	deflated, _ := Deflate("lz4", token)
	inflated, err := Inflate("lz4", deflated)
	if err != nil {
		t.Errorf("Inflate(Deflate(%q)) != %q", inflated, token)
	}
}

func BenchmarkDeflateInflateGzip(b *testing.B) {
	token := make([]byte, 65*1024)
	_, _ = rand.Read(token)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deflated, _ := Deflate("gzip", token)
		_, _ = Inflate("gzip", deflated)
	}
}

func BenchmarkDeflateInflateLZ4(b *testing.B) {
	token := make([]byte, 65*1024)
	_, _ = rand.Read(token)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deflated, _ := Deflate("lz4", token)
		_, _ = Inflate("lz4", deflated)
	}
}
