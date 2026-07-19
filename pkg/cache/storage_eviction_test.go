package cache

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
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

func TestEvictWatermarkPctAcceptsWholePercent(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheEvictWatermarkPct = 80

	require.Equal(t, 0.80, store.evictWatermarkPct())
}

func TestMaybeEvictDiskCacheEvictsRecentUnprotectedBeforeProtectedContent(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheEvictWatermarkPct = 0.80
	var events []CacheChurnEvent
	store.SetChurnSink(func(event CacheChurnEvent) {
		events = append(events, event)
	})

	now := time.Now()
	protected := addEvictionTestContent(t, store, "protected-hot-content", now.Add(-2*time.Minute))
	unprotected := addEvictionTestContent(t, store, "unprotected-hot-content", now.Add(-time.Minute))
	store.SetProtectedContent(map[string]struct{}{protected: struct{}{}})

	evicted := store.maybeEvictDiskCache(diskUsageSnapshot{
		totalBytes:     1000,
		usedBytes:      850,
		availableBytes: 150,
		usagePct:       0.85,
	})

	require.True(t, evicted)
	require.True(t, store.Exists(protected))
	require.False(t, store.Exists(unprotected))
	require.Len(t, events, 1)
	require.Equal(t, CacheChurnStatusEvicted, events[0].Status)
	require.Equal(t, CacheChurnOperationDiskEviction, events[0].Operation)
	require.Equal(t, 1, events[0].EvictedObjects)
	require.Zero(t, events[0].ProtectedObjects)
	require.False(t, events[0].Timestamp.IsZero())
}

func TestMaybeEvictDiskCachePreservesProtectedContentAboveSoftWatermark(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheEvictWatermarkPct = 0.80
	store.serverConfig.DiskCacheMaxUsagePct = 0.95
	store.diskConfig.MinFreeBytes = 100
	var events []CacheChurnEvent
	store.SetChurnSink(func(event CacheChurnEvent) {
		events = append(events, event)
	})

	protected := addEvictionTestContent(t, store, "protected-hot-content", time.Now().Add(-time.Minute))
	store.SetProtectedContent(map[string]struct{}{protected: struct{}{}})

	evicted := store.maybeEvictDiskCache(diskUsageSnapshot{
		totalBytes:     1000,
		usedBytes:      850,
		availableBytes: 150,
		usagePct:       0.85,
	})

	require.False(t, evicted)
	require.True(t, store.Exists(protected))
	require.Len(t, events, 1)
	require.Equal(t, CacheChurnStatusNothingEvictable, events[0].Status)
	require.Equal(t, 1, events[0].ProtectedCandidates)
}

func TestMaybeEvictDiskCacheEvictsProtectedContentToClearHardReserve(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheEvictWatermarkPct = 0.80
	store.serverConfig.DiskCacheMaxUsagePct = 0.95
	store.diskConfig.MinFreeBytes = 180
	var events []CacheChurnEvent
	store.SetChurnSink(func(event CacheChurnEvent) {
		events = append(events, event)
	})

	protected := addEvictionTestContent(t, store, "protected-hot-content", time.Now().Add(-time.Minute))
	store.SetProtectedContent(map[string]struct{}{protected: struct{}{}})

	evicted := store.maybeEvictDiskCache(diskUsageSnapshot{
		totalBytes:     1000,
		usedBytes:      850,
		availableBytes: 150,
		usagePct:       0.85,
	})

	require.True(t, evicted)
	require.False(t, store.Exists(protected))
	require.Len(t, events, 1)
	require.Equal(t, CacheChurnStatusProtectedEvicted, events[0].Status)
	require.Equal(t, 1, events[0].ProtectedObjects)
	require.Positive(t, events[0].ProtectedFreedBytes)
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

func TestEvictionCandidateUsesCompleteMarkerSize(t *testing.T) {
	store := newTestStore(t, 5)
	content := "content-spanning-pages"
	hash := addEvictionTestContent(t, store, content, time.Now().Add(-time.Hour))

	var candidate *evictionCandidate
	for _, item := range store.evictionCandidates() {
		if item.hash == hash {
			item := item
			candidate = &item
			break
		}
	}

	require.NotNil(t, candidate)
	require.Equal(t, int64(len(content)), candidate.sizeBytes)
}

func TestEvictionSkipsTemporaryAndIncompleteContentDirs(t *testing.T) {
	store := newTestStore(t, 5)

	stale := time.Now().Add(-time.Hour)
	complete := addEvictionTestContent(t, store, "complete-content", stale)
	tempHash := strings.Repeat("a", 64)
	tempDir := filepath.Join(filepath.Dir(store.pageDir(tempHash)), "."+tempHash+".123.tmp")
	require.NoError(t, os.MkdirAll(tempDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, store.pageKey(tempHash, 0)), []byte("temp"), 0644))

	recentIncompleteHash := strings.Repeat("b", 64)
	recentIncompleteDir := store.pageDir(recentIncompleteHash)
	require.NoError(t, os.MkdirAll(recentIncompleteDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(recentIncompleteDir, store.pageKey(recentIncompleteHash, 0)), []byte("partial"), 0644))

	staleIncompleteHash := strings.Repeat("c", 64)
	staleIncompleteDir := store.pageDir(staleIncompleteHash)
	require.NoError(t, os.MkdirAll(staleIncompleteDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(staleIncompleteDir, store.pageKey(staleIncompleteHash, 0)), []byte("partial"), 0644))
	require.NoError(t, os.Chtimes(staleIncompleteDir, stale.Add(-evictionIncompleteContentGrace), stale.Add(-evictionIncompleteContentGrace)))

	evicted, _ := store.evictLRU(1 << 30)

	require.Equal(t, 2, evicted)
	require.False(t, store.Exists(complete))
	require.DirExists(t, tempDir)
	require.DirExists(t, recentIncompleteDir)
	require.NoDirExists(t, staleIncompleteDir)
}

func TestPruneContentNotProtectedKeepsExplicitlyProtectedAndRecentContent(t *testing.T) {
	store := newTestStore(t, 5)

	old := time.Now().Add(-8 * 24 * time.Hour)
	recentAccess := time.Now().Add(-time.Hour)
	protected := addEvictionTestContent(t, store, "protected-content", old)
	stale := addEvictionTestContent(t, store, "stale-content", old)
	recent := addEvictionTestContent(t, store, "recent-content", recentAccess)

	evicted, freed := store.PruneContentNotProtected(map[string]struct{}{protected: struct{}{}}, 7*24*time.Hour)
	require.Equal(t, 1, evicted)
	require.Positive(t, freed)
	require.True(t, store.Exists(protected))
	require.False(t, store.Exists(stale))
	require.True(t, store.Exists(recent))
}
