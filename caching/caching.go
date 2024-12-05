package caching

import (
	"fmt"
	"sync"
)

type Manager struct {
	cacheDir string

	vfsCache      map[string]*_VFSCache
	vfsCacheMutex sync.Mutex
}

func NewManager(cacheDir string) *Manager {
	return &Manager{
		cacheDir: cacheDir,
		vfsCache: make(map[string]*_VFSCache),
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
