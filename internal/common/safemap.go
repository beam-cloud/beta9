package common

import "sync"

type SafeMap[V any] struct {
	mu   sync.RWMutex
	_map map[string]V
}

// NewSafeMap initializes a new SafeMap with the specified value type.
func NewSafeMap[V any]() *SafeMap[V] {
	return &SafeMap[V]{
		_map: make(map[string]V),
	}
}

func (m *SafeMap[V]) Set(key string, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m._map[key] = value
}

func (m *SafeMap[V]) Get(key string) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, exists := m._map[key]
	return value, exists
}

func (m *SafeMap[V]) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m._map, key)
}

func (m *SafeMap[V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m._map)
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
func (m *SafeMap[V]) Range(f func(key string, value V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m._map {
		if !f(k, v) {
			break
		}
	}
}
