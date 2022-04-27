package cache

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/poolpOrg/plakar/logger"
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
		"repository"	UUID,
		"uuid"	UUID NOT NULL PRIMARY KEY,
		"blob"	BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec()

	statement, err = conn.Prepare(`CREATE TABLE IF NOT EXISTS indexes (
		"repository"	UUID,
		"uuid"	UUID NOT NULL PRIMARY KEY,
		"blob"	BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec()

	statement, err = conn.Prepare(`CREATE TABLE IF NOT EXISTS objects (
		"repository"	UUID,
		"checksum"	VARCHAR NOT NULL PRIMARY KEY,
		"blob"		BLOB
	);`)
	if err != nil {
		log.Fatal(err)
	}
	defer statement.Close()
	statement.Exec()

	statement, err = conn.Prepare(`CREATE TABLE IF NOT EXISTS pathnames (
		"repository"	UUID,
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

func (cache *Cache) PutMetadata(RepositoryUuid string, Uuid string, data []byte) error {
	logger.Trace("cache: %s: PutMetadata()", Uuid)
	cache.mu_metadatas.Lock()
	cache.metadatas[fmt.Sprintf("%s:%s", RepositoryUuid, Uuid)] = data
	cache.mu_metadatas.Unlock()
	return nil
}

func (cache *Cache) PutIndex(RepositoryUuid string, Uuid string, data []byte) error {
	logger.Trace("cache: %s: PutIndex()", Uuid)
	cache.mu_indexes.Lock()
	cache.indexes[fmt.Sprintf("%s:%s", RepositoryUuid, Uuid)] = data
	cache.mu_indexes.Unlock()
	return nil
}

func (cache *Cache) GetMetadata(RepositoryUuid string, Uuid string) ([]byte, error) {
	logger.Trace("cache: %s: GetMetadata()", Uuid)
	cache.mu_metadatas.Lock()
	ret, exists := cache.metadatas[fmt.Sprintf("%s:%s", RepositoryUuid, Uuid)]
	cache.mu_metadatas.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM metadatas WHERE uuid=? AND repository=?`, Uuid, RepositoryUuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) GetIndex(RepositoryUuid string, Uuid string) ([]byte, error) {
	logger.Trace("cache: %s: GetIndex()", Uuid)
	cache.mu_indexes.Lock()
	ret, exists := cache.indexes[fmt.Sprintf("%s:%s", RepositoryUuid, Uuid)]
	cache.mu_indexes.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM indexes WHERE uuid=? AND repository=?`, Uuid, RepositoryUuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutPath(RepositoryUuid string, checksum string, data []byte) error {
	logger.Trace("cache: %s: PutPath()", RepositoryUuid)
	cache.mu_pathnames.Lock()
	cache.pathnames[fmt.Sprintf("%s:%s", RepositoryUuid, checksum)] = data
	cache.mu_pathnames.Unlock()
	return nil
}

func (cache *Cache) GetPath(RepositoryUuid string, checksum string) ([]byte, error) {
	logger.Trace("cache: %s: GetPath()", RepositoryUuid)
	cache.mu_pathnames.Lock()
	ret, exists := cache.pathnames[fmt.Sprintf("%s:%s", RepositoryUuid, checksum)]
	cache.mu_pathnames.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM pathnames WHERE checksum=? AND repository=?`, checksum, RepositoryUuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutObject(RepositoryUuid string, checksum string, data []byte) error {
	logger.Trace("cache: %s: PutObject()", RepositoryUuid)
	cache.mu_objects.Lock()
	cache.objects[fmt.Sprintf("%s:%s", RepositoryUuid, checksum)] = data
	cache.mu_objects.Unlock()
	return nil
}

func (cache *Cache) GetObject(RepositoryUuid string, checksum string) ([]byte, error) {
	logger.Trace("cache: %s: GetObject()", RepositoryUuid)
	cache.mu_objects.Lock()
	ret, exists := cache.objects[fmt.Sprintf("%s:%s", RepositoryUuid, checksum)]
	cache.mu_objects.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM objects WHERE checksum=? AND repository=?`, checksum, RepositoryUuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) Commit() error {
	logger.Trace("cache: Commit()")
	// XXX - to handle parallel use, New() needs to open a read-only version of the database
	// and Commit needs to re-open for writes so that cache.db is not locked for too long.
	//
	statement, err := cache.conn.Prepare(`INSERT OR REPLACE INTO pathnames("repository", "checksum", "blob") VALUES(?, ?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for key, data := range cache.pathnames {
		atoms := strings.Split(key, ":")
		repository, checksum := atoms[0], atoms[1]
		statement.Exec(repository, checksum, data)
	}
	statement.Close()

	statement, err = cache.conn.Prepare(`INSERT OR REPLACE INTO metadatas("repository", "uuid", "blob") VALUES(?, ?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for key, data := range cache.metadatas {
		atoms := strings.Split(key, ":")
		repository, id := atoms[0], atoms[1]
		statement.Exec(repository, id, data)
	}
	statement.Close()

	statement, err = cache.conn.Prepare(`INSERT OR REPLACE INTO indexes("repository", "uuid", "blob") VALUES(?, ?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	for key, data := range cache.indexes {
		atoms := strings.Split(key, ":")
		repository, id := atoms[0], atoms[1]
		statement.Exec(repository, id, data)
	}
	statement.Close()

	cache.conn.Close()

	return nil
}
