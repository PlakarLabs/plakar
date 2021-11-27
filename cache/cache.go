package cache

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type Cache struct {
	conn *sql.DB

	mu_snapshots sync.Mutex
	snapshots    map[string][]byte

	mu_pathnames sync.Mutex
	pathnames    map[string][]byte

	mu_objects sync.Mutex
	objects    map[string][]byte
}

func New(cacheDir string) *Cache {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s/cache.db", cacheDir))
	if err != nil {
		log.Fatal(err)
	}

	cache := &Cache{}
	cache.conn = conn
	cache.snapshots = make(map[string][]byte)
	cache.pathnames = make(map[string][]byte)
	cache.objects = make(map[string][]byte)

	statement, err := conn.Prepare(`CREATE TABLE IF NOT EXISTS snapshots (
		"uuid"	UUID NOT NULL PRIMARY KEY,
		"blob"	BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec()

	statement, err = conn.Prepare(`CREATE TABLE IF NOT EXISTS objects (
		"checksum"	VARCHAR NOT NULL PRIMARY KEY,
		"blob"		BLOB
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

func (cache *Cache) PutSnapshot(checksum string, data []byte) error {
	cache.mu_snapshots.Lock()
	cache.snapshots[checksum] = data
	cache.mu_snapshots.Unlock()

	statement, err := cache.conn.Prepare(`INSERT OR REPLACE INTO snapshots("uuid", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	statement.Exec(checksum, data)
	statement.Close()

	return nil
}

func (cache *Cache) GetSnapshot(Uuid string) ([]byte, error) {
	cache.mu_snapshots.Lock()
	ret, exists := cache.snapshots[Uuid]
	cache.mu_snapshots.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM snapshots WHERE uuid=?`, Uuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutPath(checksum string, data []byte) error {
	cache.mu_pathnames.Lock()
	cache.pathnames[checksum] = data
	cache.mu_pathnames.Unlock()

	statement, err := cache.conn.Prepare(`INSERT OR REPLACE INTO pathnames("checksum", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	statement.Exec(checksum, data)
	statement.Close()

	return nil
}

func (cache *Cache) GetPath(checksum string) ([]byte, error) {
	cache.mu_pathnames.Lock()
	ret, exists := cache.pathnames[checksum]
	cache.mu_pathnames.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM pathnames WHERE checksum=?`, checksum).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutObject(checksum string, data []byte) error {
	cache.mu_objects.Lock()
	cache.objects[checksum] = data
	cache.mu_objects.Unlock()
	return nil
}

func (cache *Cache) GetObject(checksum string) ([]byte, error) {
	cache.mu_objects.Lock()
	ret, exists := cache.objects[checksum]
	cache.mu_objects.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM objects WHERE checksum=?`, checksum).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) Commit() error {
	cache.conn.Close()
	return nil
}
