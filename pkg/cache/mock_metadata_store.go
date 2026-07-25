package cache

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MockCacheMetadataStore is a simple in-memory metadataStore for testing
// Does not require Redis or any external dependencies
type MockCacheMetadataStore struct {
	hosts      map[string]map[string]*Host // locality -> hostId -> host
	fsNodes    map[string]*FSMetadata      // id -> metadata
	fsChildren map[string][]string         // parent id -> child ids
	locks      map[string]bool             // lock keys
	mu         sync.RWMutex
}

func NewMockCacheMetadataStore() *MockCacheMetadataStore {
	return &MockCacheMetadataStore{
		hosts:      make(map[string]map[string]*Host),
		fsNodes:    make(map[string]*FSMetadata),
		fsChildren: make(map[string][]string),
		locks:      make(map[string]bool),
	}
}

func (m *MockCacheMetadataStore) AddHostToIndex(ctx context.Context, locality string, host *Host) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.hosts[locality]; !exists {
		m.hosts[locality] = make(map[string]*Host)
	}
	m.hosts[locality][host.HostId] = host
	return nil
}

func (m *MockCacheMetadataStore) SetHostKeepAlive(ctx context.Context, locality string, host *Host) error {
	return m.AddHostToIndex(ctx, locality, host)
}

func (m *MockCacheMetadataStore) RemoveHost(ctx context.Context, locality string, host *Host) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if localityHosts, exists := m.hosts[locality]; exists {
		delete(localityHosts, host.HostId)
	}
	return nil
}

func (m *MockCacheMetadataStore) GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	hosts := []*Host{}
	if localityHosts, exists := m.hosts[locality]; exists {
		for _, host := range localityHosts {
			hosts = append(hosts, host)
		}
	}
	return hosts, nil
}

func (m *MockCacheMetadataStore) SetClientLock(ctx context.Context, hash string, host string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "client-lock:" + hash
	if m.locks[key] {
		return errors.New("lock already held")
	}
	m.locks[key] = true
	return nil
}

func (m *MockCacheMetadataStore) RemoveClientLock(ctx context.Context, hash string, host string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "client-lock:" + hash
	delete(m.locks, key)
	return nil
}

func (m *MockCacheMetadataStore) SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "store-lock:" + locality + ":" + sourcePath
	if m.locks[key] {
		return errors.New("lock already held")
	}
	m.locks[key] = true
	return nil
}

func (m *MockCacheMetadataStore) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "store-lock:" + locality + ":" + sourcePath
	delete(m.locks, key)
	return nil
}

func (m *MockCacheMetadataStore) RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	// No-op for mock - locks don't expire
	return nil
}

func (m *MockCacheMetadataStore) SetFsNode(ctx context.Context, id string, metadata *FSMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.fsNodes[id] = metadata
	return nil
}

func (m *MockCacheMetadataStore) GetFsNode(ctx context.Context, id string) (*FSMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metadata, exists := m.fsNodes[id]
	if !exists {
		return nil, errors.New("fs node not found")
	}
	return metadata, nil
}

func (m *MockCacheMetadataStore) RemoveFsNode(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.fsNodes, id)
	return nil
}

func (m *MockCacheMetadataStore) RemoveFsNodeChild(ctx context.Context, pid, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if children, exists := m.fsChildren[pid]; exists {
		newChildren := []string{}
		for _, childId := range children {
			if childId != id {
				newChildren = append(newChildren, childId)
			}
		}
		m.fsChildren[pid] = newChildren
	}
	return nil
}

func (m *MockCacheMetadataStore) GetFsNodeChildren(ctx context.Context, id string) ([]*FSMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	childIds, exists := m.fsChildren[id]
	if !exists {
		return []*FSMetadata{}, nil
	}

	children := []*FSMetadata{}
	for _, childId := range childIds {
		if metadata, exists := m.fsNodes[childId]; exists {
			children = append(children, metadata)
		}
	}
	return children, nil
}

func (m *MockCacheMetadataStore) AddFsNodeChild(ctx context.Context, pid, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.fsChildren[pid]; !exists {
		m.fsChildren[pid] = []string{}
	}

	// Don't add duplicates
	for _, existingId := range m.fsChildren[pid] {
		if existingId == id {
			return nil
		}
	}

	m.fsChildren[pid] = append(m.fsChildren[pid], id)
	return nil
}

func (m *MockCacheMetadataStore) AddRecentStub(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	return nil
}

func (m *MockCacheMetadataStore) ListRecentStubs(ctx context.Context, locality string, ttl time.Duration, limit int) ([]RecentStub, error) {
	return nil, nil
}

func (m *MockCacheMetadataStore) MarkStubReported(ctx context.Context, locality, stubID string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "reported:" + locality + ":" + stubID
	if m.locks[key] {
		return false, nil
	}
	m.locks[key] = true
	return true, nil
}

func (m *MockCacheMetadataStore) AcquireReconcileLock(ctx context.Context, locality, logicalHost, hash string, ttlSeconds int) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "reconcile-lock:" + locality + ":" + logicalHost + ":" + hash
	if m.locks[key] {
		return false, nil
	}
	m.locks[key] = true
	return true, nil
}

func (m *MockCacheMetadataStore) ReleaseReconcileLock(ctx context.Context, locality, logicalHost, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.locks, "reconcile-lock:"+locality+":"+logicalHost+":"+hash)
	return nil
}
