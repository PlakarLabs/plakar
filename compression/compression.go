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
		return DeflateGzip(buf), nil
	}
	if name == "lz4" {
		return DeflateLZ4(buf), nil
	}
	return nil, fmt.Errorf("unsupported compression method %q", name)
}

func DeflateLZ4(buf []byte) []byte {
	var b bytes.Buffer
	w := lz4.NewWriter(&b)
	w.Write(buf)
	w.Close()
	return b.Bytes()
}

func DeflateGzip(buf []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write(buf)
	w.Close()
	return b.Bytes()
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
	w := lz4.NewReader(bytes.NewBuffer(buf))

	data, err := io.ReadAll(w)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func InflateGzip(buf []byte) ([]byte, error) {
	w, err := gzip.NewReader(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer w.Close()

	data, err := io.ReadAll(w)
	if err != nil {
		return nil, err
	}
	return data, nil
}
