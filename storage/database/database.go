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
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/storage"
)

func init() {
	storage.Register("database", NewDatabaseRepository)
}

func NewDatabaseRepository() storage.RepositoryBackend {
	return &DatabaseRepository{}
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
	t0 := time.Now()
	defer func() {
		logger.Profile("Create(%s): %s", repository, time.Since(t0))
	}()

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
	_, err = statement.Exec("Uuid", config.Uuid)
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

	return nil
}

func (repository *DatabaseRepository) Open(location string) error {
	err := repository.connect(location)
	if err != nil {
		return err
	}

	repositoryConfig := storage.RepositoryConfig{}
	err = repository.conn.QueryRow(`SELECT configValue FROM configuration WHERE configKey='Uuid'`).Scan(&repositoryConfig.Uuid)
	if err != nil {
		return err
	}
	err = repository.conn.QueryRow(`SELECT configValue FROM configuration WHERE configKey='Compression'`).Scan(&repositoryConfig.Compression)
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

func (repository *DatabaseRepository) Transaction() (storage.TransactionBackend, error) {
	Uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	dbTx, err := repository.conn.BeginTx(context.TODO(), nil)
	if err != nil {
		return nil, err
	}

	tx := &DatabaseTransaction{}
	tx.dbTx = dbTx
	tx.Uuid = Uuid.String()
	tx.repository = repository

	return tx, nil
}

func (repository *DatabaseRepository) GetIndexes() ([]string, error) {
	rows, err := repository.conn.Query("SELECT indexUuid FROM indexes")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var indexUuid string
		err = rows.Scan(&indexUuid)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, indexUuid)
	}
	return indexes, nil
}

func (repository *DatabaseRepository) GetChunks() ([]string, error) {
	rows, err := repository.conn.Query("SELECT chunkChecksum FROM chunks")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var checksums []string
	for rows.Next() {
		var checksum string
		err = rows.Scan(&checksum)
		if err != nil {
			return nil, err
		}
		checksums = append(checksums, checksum)
	}
	return checksums, nil
}

func (repository *DatabaseRepository) GetObjects() ([]string, error) {
	rows, err := repository.conn.Query("SELECT objectChecksum FROM objects")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var checksums []string
	for rows.Next() {
		var checksum string
		err = rows.Scan(&checksum)
		if err != nil {
			return nil, err
		}
		checksums = append(checksums, checksum)
	}
	return checksums, nil
}

func (repository *DatabaseRepository) GetMetadata(Uuid string) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT metadataBlob FROM metadatas WHERE metadataUuid=?`, Uuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) GetIndex(Uuid string) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT indexBlob FROM indexes WHERE indexUuid=?`, Uuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) GetObject(checksum string) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT objectBlob FROM objects WHERE objectChecksum=?`, checksum).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) GetChunk(checksum string) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT chunkBlob FROM chunks WHERE chunkChecksum=?`, checksum).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *DatabaseRepository) CheckObject(checksum string) (bool, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT objectChecksum FROM objects WHERE objectChecksum=?`, checksum).Scan(&data)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (repository *DatabaseRepository) CheckChunk(checksum string) (bool, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT chunkChecksum FROM chunks WHERE chunkChecksum=?`, checksum).Scan(&data)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (repository *DatabaseRepository) Purge(id string) error {
	return nil
}

func (repository *DatabaseRepository) Close() error {
	return nil
}

//////

func (transaction *DatabaseTransaction) GetUuid() string {
	return transaction.Uuid
}
func (transaction *DatabaseTransaction) ReferenceChunks(keys []string) ([]bool, error) {
	ret := make([]bool, 0)
	for _, key := range keys {
		res, err := transaction.dbTx.Exec("INSERT OR REPLACE INTO chunksReferences (indexUuid, chunkChecksum) VALUES(?, ?)", transaction.GetUuid(), key)
		if err != nil {
			// there has to be a better way ...
			if err.Error() == "FOREIGN KEY constraint failed" {
				ret = append(ret, false)
				continue
			}
			return nil, err
		}
		count, err := res.RowsAffected()
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		ret = append(ret, count != int64(0))
	}

	return ret, nil
}

func (transaction *DatabaseTransaction) ReferenceObjects(keys []string) ([]bool, error) {
	ret := make([]bool, 0)
	for _, key := range keys {
		res, err := transaction.dbTx.Exec("INSERT OR REPLACE INTO objectsReferences (indexUuid, objectChecksum) VALUES(?, ?)", transaction.GetUuid(), key)
		if err != nil {
			// there has to be a better way ...
			if err.Error() == "FOREIGN KEY constraint failed" {
				ret = append(ret, false)
				continue
			}
			return nil, err
		}
		count, err := res.RowsAffected()
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		ret = append(ret, count != int64(0))
	}

	return ret, nil
}

func (transaction *DatabaseTransaction) PutObject(checksum string, data []byte) error {
	statement, err := transaction.dbTx.Prepare(`INSERT INTO objects (objectChecksum, objectBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(checksum, data)
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}

	return nil
}

func (transaction *DatabaseTransaction) PutChunk(checksum string, data []byte) error {
	statement, err := transaction.dbTx.Prepare(`INSERT INTO chunks (chunkChecksum, chunkBlob) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(checksum, data)
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

func (transaction *DatabaseTransaction) Commit() error {
	return transaction.dbTx.Commit()
}
