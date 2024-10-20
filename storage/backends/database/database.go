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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/PlakarLabs/plakar/storage"

	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

type Repository struct {
	config storage.Configuration

	backend string

	conn    *sql.DB
	wrMutex sync.Mutex

	Repository string

	// storage.RepositoryBackend
}

func init() {
	storage.Register("database", NewRepository)
}

func NewRepository() storage.Backend {
	return &Repository{}
}

/*
func stringToChecksum(checksum string) ([32]byte, error) {
	var checksumBytes [32]byte

	b, err := hex.DecodeString(checksum)
	if err != nil {
		return checksumBytes, err
	}

	copy(checksumBytes[:], b)
	return checksumBytes, nil
}
*/

func (repository *Repository) connect(addr string) error {
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
		_, err = repository.conn.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			return nil
		}
		_, err = repository.conn.Exec("PRAGMA busy_timeout=2000;")
		if err != nil {
			return nil
		}

	}

	return nil
}

func (repository *Repository) Create(location string, config storage.Configuration) error {
	err := repository.connect(location)
	if err != nil {
		return err
	}

	statement, err := repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS configuration (
		value	BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS states (
		checksum	VARCHAR(64) NOT NULL PRIMARY KEY,
		data		BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repository.conn.Prepare(`CREATE TABLE IF NOT EXISTS packfiles (
		checksum	VARCHAR(64) NOT NULL PRIMARY KEY,
		data		BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	jsonConfig, err := json.Marshal(config)
	if err != nil {
		return err
	}

	statement, err = repository.conn.Prepare(`INSERT INTO configuration(value) VALUES(?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(jsonConfig)
	if err != nil {
		return err
	}

	return nil
}

func (repository *Repository) Open(location string) error {
	err := repository.connect(location)
	if err != nil {
		return err
	}

	var buffer []byte
	var repositoryConfig storage.Configuration

	err = repository.conn.QueryRow(`SELECT value FROM configuration`).Scan(&buffer)
	if err != nil {
		return err
	}

	err = json.Unmarshal(buffer, &repositoryConfig)
	if err != nil {
		return err
	}
	repository.config = repositoryConfig

	return nil

}

func (repository *Repository) Close() error {
	return nil
}

func (repository *Repository) Commit(snapshotID [32]byte, data []byte) error {
	statement, err := repository.conn.Prepare(`INSERT INTO snapshots (snapshotID, data) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repository.wrMutex.Lock()
	_, err = statement.Exec(snapshotID, data)
	repository.wrMutex.Unlock()
	if err != nil {
		return err
	}

	return nil
}

func (repository *Repository) Configuration() storage.Configuration {
	return repository.config
}

// states
func (repository *Repository) GetStates() ([][32]byte, error) {
	rows, err := repository.conn.Query("SELECT checksum FROM states")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	checksums := make([][32]byte, 0)
	for rows.Next() {
		var checksum []byte
		err = rows.Scan(&checksum)
		if err != nil {
			return nil, err
		}
		var checksum32 [32]byte
		copy(checksum32[:], checksum)
		checksums = append(checksums, checksum32)
	}
	return checksums, nil
}

func (repository *Repository) PutState(checksum [32]byte, data []byte) error {
	statement, err := repository.conn.Prepare(`INSERT INTO states (checksum, data) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repository.wrMutex.Lock()
	_, err = statement.Exec(checksum[:], data)
	repository.wrMutex.Unlock()
	if err != nil {
		if sqliteErr, ok := err.(sqlite3.Error); !ok {
			return err
		} else if !errors.As(err, &sqliteErr) {
			return err
		} else if !errors.Is(sqliteErr.Code, sqlite3.ErrConstraint) {
			return err
		}
	}

	return nil
}

func (repository *Repository) GetState(checksum [32]byte) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT data FROM states WHERE checksum=?`, checksum[:]).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *Repository) DeleteState(checksum [32]byte) error {
	statement, err := repository.conn.Prepare(`DELETE FROM states WHERE checksum=?`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repository.wrMutex.Lock()
	_, err = statement.Exec(checksum[:])
	repository.wrMutex.Unlock()
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([][32]byte, error) {
	rows, err := repository.conn.Query("SELECT checksum FROM packfiles")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	checksums := make([][32]byte, 0)
	for rows.Next() {
		var checksum []byte
		err = rows.Scan(&checksum)
		if err != nil {
			return nil, err
		}
		var checksum32 [32]byte
		copy(checksum32[:], checksum)
		checksums = append(checksums, checksum32)
	}
	return checksums, nil
}

func (repository *Repository) PutPackfile(checksum [32]byte, data []byte) error {
	statement, err := repository.conn.Prepare(`INSERT INTO packfiles (checksum, data) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repository.wrMutex.Lock()
	_, err = statement.Exec(checksum[:], data)
	repository.wrMutex.Unlock()
	if err != nil {
		if sqliteErr, ok := err.(sqlite3.Error); !ok {
			return err
		} else if !errors.As(err, &sqliteErr) {
			return err
		} else if !errors.Is(sqliteErr.Code, sqlite3.ErrConstraint) {
			return err
		}
	}

	return nil
}

func (repository *Repository) GetPackfile(checksum [32]byte) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT data FROM packfiles WHERE checksum=?`, checksum[:]).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *Repository) GetPackfileBlob(checksum [32]byte, offset uint32, length uint32) ([]byte, error) {
	var data []byte
	err := repository.conn.QueryRow(`SELECT substr(data, ?, ?) FROM packfiles WHERE checksum=?`, offset+1, length, checksum[:]).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (repository *Repository) DeletePackfile(checksum [32]byte) error {
	statement, err := repository.conn.Prepare(`DELETE FROM packfiles WHERE checksum=?`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repository.wrMutex.Lock()
	_, err = statement.Exec(checksum[:])
	repository.wrMutex.Unlock()
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}
	return nil
}
