package cache

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func addEvictionTestContent(t *testing.T, store *Store, content string, lastAccess time.Time) string {
	t.Helper()
	hash, _, err := store.AddReader(context.Background(), bytes.NewReader([]byte(content)))
	require.NoError(t, err)
	require.True(t, store.Exists(hash))
	require.NoError(t, os.Chtimes(store.completeMarkerPath(hash), lastAccess, lastAccess))
	return hash
}

func TestEvictLRURemovesOldestContentFirst(t *testing.T) {
	store := newTestStore(t, 5)

	now := time.Now()
	oldest := addEvictionTestContent(t, store, "oldest-content", now.Add(-3*time.Hour))
	older := addEvictionTestContent(t, store, "older-content!", now.Add(-2*time.Hour))
	newest := addEvictionTestContent(t, store, "newest-content", now.Add(-time.Hour))

	// Freeing one object's worth of bytes must evict only the oldest
	evicted, freed := store.evictLRU(int64(len("oldest-content")))
	require.Equal(t, 1, evicted)
	require.GreaterOrEqual(t, freed, int64(len("oldest-content")))
	require.False(t, store.Exists(oldest))
	require.True(t, store.Exists(older))
	require.True(t, store.Exists(newest))

	// A larger target evicts in LRU order
	evicted, _ = store.evictLRU(1 << 30)
	require.Equal(t, 2, evicted)
	require.False(t, store.Exists(older))
	require.False(t, store.Exists(newest))
}

func TestEvictLRUNeverRemovesRecentlyReadContent(t *testing.T) {
	store := newTestStore(t, 5)

	hash := addEvictionTestContent(t, store, "hot-content", time.Now())

	evicted, freed := store.evictLRU(1 << 30)
	require.Zero(t, evicted)
	require.Zero(t, freed)
	require.True(t, store.Exists(hash))
}

func TestTouchContentAccessRefreshesMarkerAndThrottles(t *testing.T) {
	store := newTestStore(t, 5)

	stale := time.Now().Add(-time.Hour)
	hash := addEvictionTestContent(t, store, "touched-content", stale)

	store.touchContentAccess(hash)
	info, err := os.Stat(store.completeMarkerPath(hash))
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), info.ModTime(), time.Minute)

	// A second touch within the throttle window must not hit the filesystem
	require.NoError(t, os.Chtimes(store.completeMarkerPath(hash), stale, stale))
	store.touchContentAccess(hash)
	info, err = os.Stat(store.completeMarkerPath(hash))
	require.NoError(t, err)
	require.WithinDuration(t, stale, info.ModTime(), time.Minute)
}

func TestEvictionCandidateUsesInMemoryTouchWhenFresher(t *testing.T) {
	store := newTestStore(t, 5)

	stale := time.Now().Add(-2 * time.Hour)
	hash := addEvictionTestContent(t, store, "in-memory-touch", stale)

	// Simulate a throttled touch that never reached the filesystem
	store.accessTouchMu.Lock()
	store.accessTouches[hash] = time.Now()
	store.accessTouchMu.Unlock()

	evicted, _ := store.evictLRU(1 << 30)
	require.Zero(t, evicted)
	require.True(t, store.Exists(hash))
}

func TestPruneContentNotProtectedKeepsRecentStubContent(t *testing.T) {
	store := newTestStore(t, 5)

	old := time.Now().Add(-8 * 24 * time.Hour)
	protected := addEvictionTestContent(t, store, "protected-content", old)
	stale := addEvictionTestContent(t, store, "stale-content", old)

	evicted, freed := store.PruneContentNotProtected(map[string]struct{}{protected: struct{}{}}, 7*24*time.Hour)
	require.Equal(t, 1, evicted)
	require.Positive(t, freed)
	require.True(t, store.Exists(protected))
	require.False(t, store.Exists(stale))
}
