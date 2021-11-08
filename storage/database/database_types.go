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

package database

import (
	"database/sql"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/poolpOrg/plakar/cache"
	"github.com/poolpOrg/plakar/encryption"
	"github.com/poolpOrg/plakar/network"
	"github.com/poolpOrg/plakar/storage"
)

type inflight struct {
	Add  bool
	Uuid string
	Chan chan network.Request
}

type DatabaseStore struct {
	config storage.StoreConfig

	Cache   *cache.Cache
	Keypair *encryption.Keypair

	backend string

	conn *sql.DB
	mu   sync.Mutex

	Repository string

	storage.Store
}

type DatabaseTransaction struct {
	Uuid  string
	store *DatabaseStore

	SkipDirs []string

	dbTx *sql.Tx
	storage.Transaction
}