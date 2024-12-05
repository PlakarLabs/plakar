package caching

import (
	"fmt"
	"sync"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/google/uuid"
)

type Manager struct {
	cacheDir string

	repositoryCache      map[uuid.UUID]*_RepositoryCache
	repositoryCacheMutex sync.Mutex

	scanCache      map[objects.Checksum]*_ScanCache
	scanCacheMutex sync.Mutex

	vfsCache      map[string]*_VFSCache
	vfsCacheMutex sync.Mutex
}

func NewManager(cacheDir string) *Manager {
	return &Manager{
		cacheDir: cacheDir,

		repositoryCache: make(map[uuid.UUID]*_RepositoryCache),
		scanCache:       make(map[objects.Checksum]*_ScanCache),
		vfsCache:        make(map[string]*_VFSCache),
	}
}

func (m *Manager) Close() error {
	m.vfsCacheMutex.Lock()
	defer m.vfsCacheMutex.Unlock()

	for _, cache := range m.vfsCache {
		cache.Close()
	}

	// we may rework the interface later to allow for error handling
	// at this point closing is best effort
	return nil
}

func (m *Manager) VFS(scheme string, origin string) (*_VFSCache, error) {
	m.vfsCacheMutex.Lock()
	defer m.vfsCacheMutex.Unlock()

	key := fmt.Sprintf("%s://%s", scheme, origin)

	if cache, ok := m.vfsCache[key]; ok {
		return cache, nil
	}

	if cache, err := newVFSCache(m, scheme, origin); err != nil {
		return nil, err
	} else {
		m.vfsCache[key] = cache
		return cache, nil
	}
}

func (m *Manager) Repository(repositoryID uuid.UUID) (*_RepositoryCache, error) {
	m.repositoryCacheMutex.Lock()
	defer m.repositoryCacheMutex.Unlock()

	if cache, ok := m.repositoryCache[repositoryID]; ok {
		return cache, nil
	}

	if cache, err := newRepositoryCache(m, repositoryID); err != nil {
		return nil, err
	} else {
		m.repositoryCache[repositoryID] = cache
		return cache, nil
	}
}

func (m *Manager) Scan(snapshotID objects.Checksum) (*_ScanCache, error) {
	m.scanCacheMutex.Lock()
	defer m.scanCacheMutex.Unlock()

	if cache, ok := m.scanCache[snapshotID]; ok {
		return cache, nil
	}

	if cache, err := newScanCache(m, snapshotID); err != nil {
		return nil, err
	} else {
		m.scanCache[snapshotID] = cache
		return cache, nil
	}
}
