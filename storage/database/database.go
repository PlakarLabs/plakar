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
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/PlakarLabs/plakar/cache"
	"github.com/PlakarLabs/plakar/network"
	"github.com/PlakarLabs/plakar/storage"
	"github.com/google/uuid"

	_ "github.com/mattn/go-sqlite3"
)

type inflight struct {
	Add  bool
	Uuid string
	Chan chan network.Request
}

type DatabaseRepository struct {
	config storage.RepositoryConfig

	Cache *cache.Cache

	backend string

	conn *sql.DB
	mu   sync.Mutex

	Repository string

	storage.RepositoryBackend
}

type DatabaseTransaction struct {
	Uuid       uuid.UUID
	repository *DatabaseRepository

	dbTx *sql.Tx
	storage.TransactionBackend
}

func init() {
	storage.Register("database", NewDatabaseRepository)
}

func NewDatabaseRepository() storage.RepositoryBackend {
	return &DatabaseRepository{}
}

func checksumToString(checksum [32]byte) string {
	return fmt.Sprintf("%064x", checksum)
}

func stringToChecksum(checksum string) ([32]byte, error) {
	var checksumBytes [32]byte

	b, err := hex.DecodeString(checksum)
	if err != nil {
		return checksumBytes, err
	}

	copy(checksumBytes[:], b)
	return checksumBytes, nil
}

func (repository *DatabaseRepository) connect(addr string) error {
	var connectionString string
	if strings.HasPrefix(addr, "sqlite://") {
		repository.backend = "sqlite3"
		connectionString = addr[9:]
	} else {
		return fmt.Errorf("unsupported database backend: %s", addr)
	}

	conn, err := sql.Open(repository.backend, connectionString)
	if err != nil {
		return err
	}
	repository.conn = conn

	if repository.backend == "sqlite3" {
		_, err = repository.conn.Exec("PRAGMA foreign_keys = ON")
		if err != nil {
			return nil
		}
	}

	return nil
}

func (repository *DatabaseRepository) Create(location string, config storage.RepositoryConfig) error {
	err := repository.connect(location)
	if err != nil {
		return err
	}

	statement, err := repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS configuration (
		configKey	VARCHAR(32) NOT NULL PRIMARY KEY,
		configValue	VARCHAR(64)
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS metadatas (
		metadataUuid	VARCHAR(36) NOT NULL PRIMARY KEY,
		metadataBlob	BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS indexes (
		indexUuid	VARCHAR(36) NOT NULL PRIMARY KEY,
		indexBlob	BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS filesystems (
		filesystemUuid	VARCHAR(36) NOT NULL PRIMARY KEY,
		filesystemBlob	BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS objects (
		objectChecksum	VARCHAR(64) NOT NULL PRIMARY KEY,
		objectBlob		BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS chunks (
		chunkChecksum	VARCHAR(64) NOT NULL PRIMARY KEY,
		chunkBlob		BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS chunksReferences (
		indexUuid		VARCHAR(36) NOT NULL,
		chunkChecksum	VARCHAR(64) NOT NULL,

		FOREIGN KEY (chunkChecksum)
			REFERENCES chunks (chunkChecksum)
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS objectsReferences (
		indexUuid		VARCHAR(36) NOT NULL,
		objectChecksum	VARCHAR(64) NOT NULL,

		FOREIGN KEY (objectChecksum)
			REFERENCES objects (objectChecksum)

	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`INSERT INTO configuration(configKey, configValue) VALUES(?, ?)`)
	defer statement.Close()
	if err != nil {
		return err
	}
	_, err = statement.Exec("RepositoryID", config.RepositoryID)
	if err != nil {
		return err
	}

	_, err = statement.Exec("Compression", config.Compression)
	if err != nil {
		return err
	}

	_, err = statement.Exec("Encryption", config.Encryption)
	if err != nil {
		return err
	}

	_, err = statement.Exec("Hashing", config.Hashing)
	if err != nil {
		return err
	}

	_, err = statement.Exec("CreationTime", config.CreationTime)
	if err != nil {
		return err
	}

	return nil
}

func (repository *DatabaseRepository) Open(location string) error {
	err := repository.connect(location)
	if err != nil {
		return err
	}

	repositoryConfig := storage.RepositoryConfig{}
	err = repository.conn.QueryRow(`SELECT configValue FROM configuration WHERE configKey='RepositoryID'`).Scan(&repositoryConfig.RepositoryID)
	if err != nil {
		return err
	}
	err = repository.conn.QueryRow(`SELECT configValue FROM configuration WHERE configKey='Compression'`).Scan(&repositoryConfig.Compression)
	if err != nil {
		return err
	}

	err = repository.conn.QueryRow(`SELECT configValue FROM configuration WHERE configKey='Hashing'`).Scan(&repositoryConfig.Hashing)
	if err != nil {
		return err
	}

	err = repository.conn.QueryRow(`SELECT configValue FROM configuration WHERE configKey='Encryption'`).Scan(&repositoryConfig.Encryption)
	if err != nil {
		return err
	}
	repository.config = repositoryConfig

	return nil

}

func (repository *DatabaseRepository) Configuration() storage.RepositoryConfig {
	return repository.config
}

func (repository *DatabaseRepository) Transaction(indexID uuid.UUID) (storage.TransactionBackend, error) {

	dbTx, err := repository.conn.BeginTx(context.TODO(), nil)
	if err != nil {
		return nil, err
	}

	tx := &DatabaseTransaction{}
	tx.dbTx = dbTx
	tx.Uuid = indexID
	tx.repository = repository

	return tx, nil
}

func (repository *DatabaseRepository) GetIndexes() ([]uuid.UUID, error) {
	rows, err := repository.conn.Query("SELECT indexUuid FROM indexes")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []uuid.UUID
	for rows.Next() {
		var indexUuid string
		err = rows.Scan(&indexUuid)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, uuid.Must(uuid.Parse(indexUuid)))
	}
	return indexes, nil
}

func (repository *DatabaseRepository) PutMetadata(indexID uuid.UUID, data []byte) error {
	statement, err := repository.conn.Prepare(`INSERT INTO metadatas (metadataUuid, metadataBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(indexID, data)
	if err != nil {
		return err
	}

	return nil
}

func (repository *DatabaseRepository) PutIndex(indexID uuid.UUID, data []byte) error {
	statement, err := repository.conn.Prepare(`INSERT INTO indexes (indexUuid, indexBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(indexID, data)
	if err != nil {
		return err
	}

	return nil
}

func (repository *DatabaseRepository) PutFilesystem(indexID uuid.UUID, data []byte) error {
	statement, err := repository.conn.Prepare(`INSERT INTO filesystems (filesystemUuid, filesystemBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(indexID, data)
	if err != nil {
		return err
	}

	return nil
}

func (repository *DatabaseRepository) PutChunk(checksum [32]byte, data []byte) error {
	statement, err := repository.conn.Prepare(`INSERT INTO chunks (chunkChecksum, chunkBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(checksumToString(checksum), data)
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}

	return nil
}

func (repository *DatabaseRepository) PutObject(checksum [32]byte, data []byte) error {
	statement, err := repository.conn.Prepare(`INSERT INTO objects (objectChecksum, objectBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(checksumToString(checksum), data)
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}

	return nil
}

func (repository *DatabaseRepository) GetChunks() ([][32]byte, error) {
	rows, err := repository.conn.Query("SELECT chunkChecksum FROM chunks")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var checksums [][32]byte
	for rows.Next() {
		var checksum string
		err = rows.Scan(&checksum)
		if err != nil {
			return nil, err
		}
		checksumDecoded, _ := stringToChecksum(checksum)
		checksums = append(checksums, checksumDecoded)
	}
	return checksums, nil
}

func (repository *DatabaseRepository) GetObjects() ([][32]byte, error) {
	rows, err := repository.conn.Query("SELECT objectChecksum FROM objects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var checksums [][32]byte
	for rows.Next() {
		var checksum string
		err = rows.Scan(&checksum)
		if err != nil {
			return nil, err
		}
		checksumDecoded, _ := stringToChecksum(checksum)
		checksums = append(checksums, checksumDecoded)
	}
	return checksums, nil
}

func (repository *DatabaseRepository) GetMetadata(indexID uuid.UUID) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT metadataBlob FROM metadatas WHERE metadataUuid=?`, indexID).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) GetIndex(indexID uuid.UUID) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT indexBlob FROM indexes WHERE indexUuid=?`, indexID).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) GetFilesystem(indexID uuid.UUID) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT filesystemBlob FROM filesystems WHERE filesystemUuid=?`, indexID).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) GetObject(checksum [32]byte) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT objectBlob FROM objects WHERE objectChecksum=?`, checksumToString(checksum)).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) GetChunk(checksum [32]byte) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT chunkBlob FROM chunks WHERE chunkChecksum=?`, checksumToString(checksum)).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) CheckObject(checksum [32]byte) (bool, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT objectChecksum FROM objects WHERE objectChecksum=?`, checksumToString(checksum)).Scan(&data)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (repository *DatabaseRepository) CheckChunk(checksum [32]byte) (bool, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT chunkChecksum FROM chunks WHERE chunkChecksum=?`, checksumToString(checksum)).Scan(&data)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (repository *DatabaseRepository) Purge(indexID uuid.UUID) error {
	return nil
}

func (repository *DatabaseRepository) Close() error {
	return nil
}

//////

func (transaction *DatabaseTransaction) GetUuid() uuid.UUID {
	return transaction.Uuid
}

func (transaction *DatabaseTransaction) PutObject(checksum [32]byte, data []byte) error {
	statement, err := transaction.dbTx.Prepare(`INSERT INTO objects (objectChecksum, objectBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(checksumToString(checksum), data)
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}

	return nil
}

func (transaction *DatabaseTransaction) PutChunk(checksum [32]byte, data []byte) error {
	statement, err := transaction.dbTx.Prepare(`INSERT INTO chunks (chunkChecksum, chunkBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(checksumToString(checksum), data)
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}

	return nil
}

func (transaction *DatabaseTransaction) PutMetadata(data []byte) error {
	statement, err := transaction.dbTx.Prepare(`INSERT INTO metadatas (metadataUuid, metadataBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(transaction.GetUuid(), data)
	if err != nil {
		return err
	}

	return nil
}

func (transaction *DatabaseTransaction) PutIndex(data []byte) error {
	statement, err := transaction.dbTx.Prepare(`INSERT INTO indexes (indexUuid, indexBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(transaction.GetUuid(), data)
	if err != nil {
		return err
	}

	return nil
}

func (transaction *DatabaseTransaction) PutFilesystem(data []byte) error {
	statement, err := transaction.dbTx.Prepare(`INSERT INTO filesystems (filesystemUuid, filesystemBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(transaction.GetUuid(), data)
	if err != nil {
		return err
	}

	return nil
}

func (transaction *DatabaseTransaction) Commit() error {
	return transaction.dbTx.Commit()
}
