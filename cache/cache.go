package cache

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

type Cache struct {
	directory string
	conn      *sql.DB
}

func New(cacheDir string) *Cache {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s/cache.db", cacheDir))
	if err != nil {
		log.Fatal(err)
	}

	cache := &Cache{}
	cache.conn = conn

	cache.directory = cacheDir

	statement, err := conn.Prepare(`CREATE TABLE IF NOT EXISTS snapshots (
		"uuid"	UUID NOT NULL PRIMARY KEY,
		"blob"	BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec()

	statement, err = conn.Prepare(`CREATE TABLE IF NOT EXISTS pathnames (
		"checksum"	VARCHAR NOT NULL PRIMARY KEY,
		"blob"		BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec()

	return cache
}

func (cache *Cache) PutPath(checksum string, data []byte) error {
	statement, err := cache.conn.Prepare(`INSERT INTO pathnames("checksum", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec(checksum, data)
	return nil
}

func (cache *Cache) GetPath(checksum string) ([]byte, error) {
	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM pathnames WHERE checksum=?`, checksum).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutSnapshot(checksum string, data []byte) error {
	statement, err := cache.conn.Prepare(`INSERT INTO snapshots("uuid", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec(checksum, data)
	return nil
}

func (cache *Cache) GetSnapshot(Uuid string) ([]byte, error) {
	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM snapshots WHERE uuid=?`, Uuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
