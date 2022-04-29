package cache

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/poolpOrg/plakar/logger"
	"github.com/poolpOrg/plakar/profiler"
)

type Cache struct {
	conn *sql.DB

	mu_metadatas    sync.Mutex
	metadatas       map[string][]byte
	putMetadataStmt *sql.Stmt

	mu_indexes   sync.Mutex
	indexes      map[string][]byte
	putIndexStmt *sql.Stmt

	mu_filesystems    sync.Mutex
	filesystems       map[string][]byte
	putFilesystemStmt *sql.Stmt

	mu_pathnames    sync.Mutex
	pathnames       map[string][]byte
	putPathnameStmt *sql.Stmt

	mu_objects    sync.Mutex
	objects       map[string][]byte
	putObjectStmt *sql.Stmt
}

func Create(localdir string) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.Create", time.Since(t0))
	}()
	return os.MkdirAll(localdir, 0700)
}

func New(cacheDir string) *Cache {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.New", time.Since(t0))
	}()

	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s/cache.db", cacheDir))
	if err != nil {
		log.Fatal(err)
	}

	cache := &Cache{}
	cache.conn = conn
	cache.metadatas = make(map[string][]byte)
	cache.indexes = make(map[string][]byte)
	cache.filesystems = make(map[string][]byte)
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

	statement, err = conn.Prepare(`CREATE TABLE IF NOT EXISTS filesystems (
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
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutMetadata", time.Since(t0))
	}()

	logger.Trace("cache", "%s: PutMetadata()", Uuid)
	cache.mu_metadatas.Lock()
	defer cache.mu_metadatas.Unlock()
	cache.metadatas[fmt.Sprintf("%s:%s", RepositoryUuid, Uuid)] = data
	if cache.putMetadataStmt == nil {
		stmt, err := cache.conn.Prepare(`INSERT OR REPLACE INTO metadatas("repository", "uuid", "blob") VALUES(?, ?, ?)`)
		if err != nil {
			log.Fatal(err)
		}
		cache.putMetadataStmt = stmt
	}
	cache.putMetadataStmt.Exec(RepositoryUuid, Uuid, data)
	return nil
}

func (cache *Cache) PutIndex(RepositoryUuid string, Uuid string, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutIndex", time.Since(t0))
	}()

	logger.Trace("cache", "%s: PutIndex()", Uuid)
	cache.mu_indexes.Lock()
	defer cache.mu_indexes.Unlock()

	cache.indexes[fmt.Sprintf("%s:%s", RepositoryUuid, Uuid)] = data

	if cache.putIndexStmt == nil {
		stmt, err := cache.conn.Prepare(`INSERT OR REPLACE INTO indexes("repository", "uuid", "blob") VALUES(?, ?, ?)`)
		if err != nil {
			log.Fatal(err)
		}
		cache.putIndexStmt = stmt
	}
	cache.putIndexStmt.Exec(RepositoryUuid, Uuid, data)
	return nil
}

func (cache *Cache) PutFilesystem(RepositoryUuid string, Uuid string, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutFilesystem", time.Since(t0))
	}()

	logger.Trace("cache", "%s: PutFilesystem()", Uuid)
	cache.mu_filesystems.Lock()
	defer cache.mu_filesystems.Unlock()
	cache.filesystems[fmt.Sprintf("%s:%s", RepositoryUuid, Uuid)] = data

	if cache.putFilesystemStmt == nil {
		stmt, err := cache.conn.Prepare(`INSERT OR REPLACE INTO filesystems("repository", "uuid", "blob") VALUES(?, ?, ?)`)
		if err != nil {
			log.Fatal(err)
		}
		cache.putFilesystemStmt = stmt
	}
	cache.putFilesystemStmt.Exec(RepositoryUuid, Uuid, data)
	return nil
}

func (cache *Cache) GetMetadata(RepositoryUuid string, Uuid string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetMetadata", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetMetadata()", Uuid)
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
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetIndex", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetIndex()", Uuid)
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

func (cache *Cache) GetFilesystem(RepositoryUuid string, Uuid string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetFilesystem", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetFilesystem()", Uuid)
	cache.mu_filesystems.Lock()
	ret, exists := cache.filesystems[fmt.Sprintf("%s:%s", RepositoryUuid, Uuid)]
	cache.mu_filesystems.Unlock()
	if exists {
		return ret, nil
	}

	var data []byte
	err := cache.conn.QueryRow(`SELECT blob FROM filesystems WHERE uuid=? AND repository=?`, Uuid, RepositoryUuid).Scan(&data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cache *Cache) PutPath(RepositoryUuid string, checksum string, data []byte) error {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutPath", time.Since(t0))
	}()
	logger.Trace("cache", "%s: PutPath()", RepositoryUuid)
	cache.mu_pathnames.Lock()
	defer cache.mu_pathnames.Unlock()

	cache.pathnames[fmt.Sprintf("%s:%s", RepositoryUuid, checksum)] = data

	if cache.putPathnameStmt == nil {
		stmt, err := cache.conn.Prepare(`INSERT OR REPLACE INTO pathnames("repository", "checksum", "blob") VALUES(?, ?, ?)`)
		if err != nil {
			log.Fatal(err)
		}
		cache.putPathnameStmt = stmt
	}
	cache.putPathnameStmt.Exec(RepositoryUuid, checksum, data)
	return nil
}

func (cache *Cache) GetPath(RepositoryUuid string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetPath", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetPath()", RepositoryUuid)
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
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.PutObject", time.Since(t0))
	}()
	logger.Trace("cache", "%s: PutObject()", RepositoryUuid)
	cache.mu_objects.Lock()
	defer cache.mu_objects.Unlock()
	cache.objects[fmt.Sprintf("%s:%s", RepositoryUuid, checksum)] = data

	if cache.putObjectStmt == nil {
		stmt, err := cache.conn.Prepare(`INSERT OR REPLACE INTO objects("repository", "checksum", "blob") VALUES(?, ?, ?)`)
		if err != nil {
			log.Fatal(err)
		}
		cache.putObjectStmt = stmt
	}
	cache.putObjectStmt.Exec(RepositoryUuid, checksum, data)

	return nil
}

func (cache *Cache) GetObject(RepositoryUuid string, checksum string) ([]byte, error) {
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.GetObject", time.Since(t0))
	}()
	logger.Trace("cache", "%s: GetObject()", RepositoryUuid)
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
	t0 := time.Now()
	defer func() {
		profiler.RecordEvent("cache.Commit", time.Since(t0))
	}()

	logger.Trace("cache", "Commit()")
	// XXX - to handle parallel use, New() needs to open a read-only version of the database
	// and Commit needs to re-open for writes so that cache.db is not locked for too long.
	//

	if cache.putMetadataStmt != nil {
		cache.putMetadataStmt.Close()
	}

	if cache.putIndexStmt != nil {
		cache.putIndexStmt.Close()
	}

	if cache.putFilesystemStmt != nil {
		cache.putFilesystemStmt.Close()
	}

	if cache.putFilesystemStmt != nil {
		cache.putFilesystemStmt.Close()
	}

	if cache.putPathnameStmt != nil {
		cache.putPathnameStmt.Close()
	}

	if cache.putObjectStmt != nil {
		cache.putObjectStmt.Close()
	}

	cache.conn.Close()

	return nil
}
