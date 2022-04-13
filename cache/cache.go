package cache

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type Cache struct {
	conn *sql.DB

	mu_metadatas sync.Mutex
	metadatas    map[string][]byte

	mu_indexes sync.Mutex
	indexes    map[string][]byte

	mu_pathnames sync.Mutex
	pathnames    map[string][]byte

	mu_objects sync.Mutex
	objects    map[string][]byte
}

func Create(localdir string) error {
	return os.MkdirAll(localdir, 0700)
}

func New(cacheDir string) *Cache {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s/cache.db", cacheDir))
	if err != nil {
		log.Fatal(err)
	}

	cache := &Cache{}
	cache.conn = conn
	cache.metadatas = make(map[string][]byte)
	cache.indexes = make(map[string][]byte)
	cache.pathnames = make(map[string][]byte)
	cache.objects = make(map[string][]byte)

	statement, err := conn.Prepare(`CREATE TABLE IF NOT EXISTS metadatas (
		"uuid"	UUID NOT NULL PRIMARY KEY,
		"blob"	BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec()

	statement, err = conn.Prepare(`CREATE TABLE IF NOT EXISTS indexes (
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

func (cache *Cache) PutMetadata(checksum string, data []byte) error {
	cache.mu_metadatas.Lock()
	cache.metadatas[checksum] = data
	cache.mu_metadatas.Unlock()
	return nil
}

func (cache *Cache) PutIndex(checksum string, data []byte) error {
	cache.mu_indexes.Lock()
	cache.indexes[checksum] = data
	cache.mu_indexes.Unlock()
	return nil
}

func (cache *Cache) GetMetadata(Uuid string) ([]byte, error) {
	cache.mu_metadatas.Lock()
	ret, exists := cache.metadatas[Uuid]
	cache.mu_metadatas.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM metadatas WHERE uuid=?`, Uuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) GetIndex(Uuid string) ([]byte, error) {
	cache.mu_indexes.Lock()
	ret, exists := cache.indexes[Uuid]
	cache.mu_indexes.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM indexes WHERE uuid=?`, Uuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutPath(checksum string, data []byte) error {
	cache.mu_pathnames.Lock()
	cache.pathnames[checksum] = data
	cache.mu_pathnames.Unlock()
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
	// XXX - to handle parallel use, New() needs to open a read-only version of the database
	// and Commit needs to re-open for writes so that cache.db is not locked for too long.
	//
	statement, err := cache.conn.Prepare(`INSERT OR REPLACE INTO pathnames("checksum", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for checksum, data := range cache.pathnames {
		statement.Exec(checksum, data)
	}
	statement.Close()

	statement, err = cache.conn.Prepare(`INSERT OR REPLACE INTO metadatas("uuid", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for checksum, data := range cache.metadatas {
		statement.Exec(checksum, data)
	}
	statement.Close()

	statement, err = cache.conn.Prepare(`INSERT OR REPLACE INTO indexes("uuid", "blob") VALUES(?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for checksum, data := range cache.indexes {
		statement.Exec(checksum, data)
	}
	statement.Close()

	cache.conn.Close()

	return nil
}
