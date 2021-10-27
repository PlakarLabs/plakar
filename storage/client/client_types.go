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

package client

import (
	"sync"

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/storage"
	"golang.org/x/crypto/ssh"
)

type ClientStore struct {
	config storage.StoreConfig

	Cache   *cache.Cache
	Keypair *encryption.Keypair

	mu         sync.Mutex
	conn       *ssh.Client
	sshChannel ssh.Channel
	sshReq     <-chan *ssh.Request

	Repository string

	storage.Store
}

type ClientTransaction struct {
	Uuid  string
	store ClientStore

	SkipDirs []string

	storage.Transaction
}
