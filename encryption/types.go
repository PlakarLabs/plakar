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
	"crypto/ecdsa"
	"time"
)

type Keypair struct {
	CreationTime time.Time
	Uuid         string
	PrivateKey   *ecdsa.PrivateKey
	PublicKey    *ecdsa.PublicKey
	MasterKey    []byte
}

type SerializedKeypair struct {
	CreationTime time.Time
	Uuid         string
	PrivateKey   string
	PublicKey    string
	MasterKey    string
}

type PublicKey struct {
	CreationTime time.Time
	Uuid         string
	PublicKey    *ecdsa.PublicKey
}

type SerializedPublicKey struct {
	CreationTime time.Time
	Uuid         string
	PublicKey    string
}

type MasterKey struct {
	CreationTime time.Time
	Uuid         string
	Key          []byte
}

type SerializedMasterKey struct {
	CreationTime time.Time
	Uuid         string
	Key          string
}
