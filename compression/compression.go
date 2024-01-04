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
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

func Deflate(name string, buf []byte) ([]byte, error) {
	if name == "gzip" {
		return DeflateGzip(buf)
	}
	if name == "lz4" {
		return DeflateLZ4(buf)
	}
	return nil, fmt.Errorf("unsupported compression method %q", name)
}

func DeflateLZ4(buf []byte) ([]byte, error) {
	b := bytes.NewBuffer(make([]byte, 0, len(buf)))
	w := lz4.NewWriter(b)
	defer func() {
		_ = w.Close()
	}()

	if _, err := w.Write(buf); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func DeflateGzip(buf []byte) ([]byte, error) {
	b := bytes.NewBuffer(make([]byte, 0, len(buf)))
	w := gzip.NewWriter(b)
	defer func() {
		_ = w.Close()
	}()

	if _, err := w.Write(buf); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func Inflate(name string, buf []byte) ([]byte, error) {
	if name == "gzip" {
		return InflateGzip(buf)
	}
	if name == "lz4" {
		return InflateLZ4(buf)
	}
	return nil, fmt.Errorf("unsupported compression method %q", name)
}

func InflateLZ4(buf []byte) ([]byte, error) {
	return io.ReadAll(lz4.NewReader(bytes.NewBuffer(buf)))
}

func InflateGzip(buf []byte) ([]byte, error) {
	w, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = w.Close()
	}()
	return io.ReadAll(w)
}
