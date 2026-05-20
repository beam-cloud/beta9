package cache

import (
	"context"
	"errors"
	"sync"
)

// MockRegistry is a simple in-memory coordinator for testing
// Does not require Redis or any external dependencies
type MockRegistry struct {
	hosts      map[string]map[string]*Host // locality -> hostId -> host
	fsNodes    map[string]*FSMetadata      // id -> metadata
	fsChildren map[string][]string         // parent id -> child ids
	locks      map[string]bool             // lock keys
	mu         sync.RWMutex
}

func NewMockRegistry() *MockRegistry {
	return &MockRegistry{
		hosts:      make(map[string]map[string]*Host),
		fsNodes:    make(map[string]*FSMetadata),
		fsChildren: make(map[string][]string),
		locks:      make(map[string]bool),
	}
}

func (m *MockRegistry) AddHostToIndex(ctx context.Context, locality string, host *Host) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.hosts[locality]; !exists {
		m.hosts[locality] = make(map[string]*Host)
	}
	m.hosts[locality][host.HostId] = host
	return nil
}

func (m *MockRegistry) SetHostKeepAlive(ctx context.Context, locality string, host *Host) error {
	return m.AddHostToIndex(ctx, locality, host)
}

func (m *MockRegistry) RemoveHost(ctx context.Context, locality string, host *Host) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if localityHosts, exists := m.hosts[locality]; exists {
		delete(localityHosts, host.HostId)
	}
	return nil
}

func (m *MockRegistry) GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error) {
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

func (m *MockRegistry) GetRegionConfig(ctx context.Context, locality string) (ServerConfig, error) {
	return ServerConfig{}, errors.New("region config not found")
}

func (m *MockRegistry) SetClientLock(ctx context.Context, hash string, host string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "client-lock:" + hash
	if m.locks[key] {
		return errors.New("lock already held")
	}
	m.locks[key] = true
	return nil
}

func (m *MockRegistry) RemoveClientLock(ctx context.Context, hash string, host string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "client-lock:" + hash
	delete(m.locks, key)
	return nil
}

func (m *MockRegistry) SetStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "store-lock:" + locality + ":" + sourcePath
	if m.locks[key] {
		return errors.New("lock already held")
	}
	m.locks[key] = true
	return nil
}

func (m *MockRegistry) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := "store-lock:" + locality + ":" + sourcePath
	delete(m.locks, key)
	return nil
}

func (m *MockRegistry) RefreshStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	// No-op for mock - locks don't expire
	return nil
}

func (m *MockRegistry) SetFsNode(ctx context.Context, id string, metadata *FSMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.fsNodes[id] = metadata
	return nil
}

func (m *MockRegistry) GetFsNode(ctx context.Context, id string) (*FSMetadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metadata, exists := m.fsNodes[id]
	if !exists {
		return nil, errors.New("fs node not found")
	}
	return metadata, nil
}

func (m *MockRegistry) RemoveFsNode(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.fsNodes, id)
	return nil
}

func (m *MockRegistry) RemoveFsNodeChild(ctx context.Context, pid, id string) error {
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

func (m *MockRegistry) GetFsNodeChildren(ctx context.Context, id string) ([]*FSMetadata, error) {
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

func (m *MockRegistry) AddFsNodeChild(ctx context.Context, pid, id string) error {
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
