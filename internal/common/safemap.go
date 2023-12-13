package common

import "sync"

type SafeMap[V any] struct {
	mu         sync.RWMutex
	containers map[string]V
}

// NewSafeMap initializes a new SafeMap with the specified value type.
func NewSafeMap[V any]() *SafeMap[V] {
	return &SafeMap[V]{
		containers: make(map[string]V),
	}
}

func (m *SafeMap[V]) Set(key string, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.containers[key] = value
}

func (m *SafeMap[V]) Get(key string) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists := m.containers[key]
	return value, exists
}

func (m *SafeMap[V]) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.containers, key)
}

func (m *SafeMap[V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.containers)
}
