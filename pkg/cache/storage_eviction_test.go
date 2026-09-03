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
	// The index remembers the write as the last access; backdate the entry
	// to match the marker so it looks the way a restart would rebuild it.
	entry, ok := store.index.get(hash)
	require.True(t, ok)
	entry.lastAccess = lastAccess
	store.index.put(hash, entry)
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
	known, _ := store.index.touch(hash, time.Now(), evictionAccessTouchInterval)
	require.True(t, known)

	evicted, _ := store.evictLRU(1 << 30)
	require.Zero(t, evicted)
	require.True(t, store.Exists(hash))

	// A rescan must not lose the in-memory access to the older on-disk mtime
	store.rebuildContentIndex()
	evicted, _ = store.evictLRU(1 << 30)
	require.Zero(t, evicted)
	require.True(t, store.Exists(hash))
}

func TestContentIndexRebuildKeepsCompletionsRacingTheWalk(t *testing.T) {
	store := newTestStore(t, 5)

	// A walk began, then two writes completed before it was swapped in: one
	// the walk never saw, one it saw before the marker landed.
	walkStarted := time.Now()
	scanned := store.scanContent()
	unseen := addEvictionTestContent(t, store, "completed-after-walk", time.Now())
	halfSeen := addEvictionTestContent(t, store, "completed-mid-walk", time.Now())
	scanned[halfSeen] = contentEntry{dir: store.pageDir(halfSeen), size: 3, lastAccess: walkStarted}

	store.index.replace(scanned, walkStarted)

	require.True(t, store.Exists(unseen, int64(len("completed-after-walk"))))
	require.True(t, store.Exists(halfSeen, int64(len("completed-mid-walk"))))
}

func TestRemoveContentFailureLeavesTheLeftoverIndexed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	store := newTestStore(t, 5)

	stale := time.Now().Add(-2 * time.Hour)
	hash := addEvictionTestContent(t, store, "stuck-content", stale)
	// A directory the process cannot empty: a subdirectory it may not list.
	locked := filepath.Join(store.pageDir(hash), "locked")
	require.NoError(t, os.MkdirAll(filepath.Join(locked, "inner"), 0755))
	require.NoError(t, os.Chmod(locked, 0))
	t.Cleanup(func() { _ = os.Chmod(locked, 0755) })

	evicted, _ := store.evictLRU(1 << 30)
	require.Zero(t, evicted)
	require.False(t, store.Exists(hash))

	entry, ok := store.index.get(hash)
	require.True(t, ok, "the leftover must stay visible to the next pass")
	require.False(t, entry.complete)
	require.WithinDuration(t, stale, entry.lastAccess, time.Second)

	// Once the obstacle is gone the next pass reclaims it.
	require.NoError(t, os.Chmod(locked, 0755))
	evicted, _ = store.evictLRU(1 << 30)
	require.Equal(t, 1, evicted)
	require.NoDirExists(t, store.pageDir(hash))
}

func TestContentIndexRebuildFindsExistingContent(t *testing.T) {
	store := newTestStore(t, 5)
	hash := addEvictionTestContent(t, store, "survives-restart", time.Now())

	// A fresh store over the same directory learns the content from disk
	restarted, err := NewStore(context.Background(), store.currentHost, store.locality, store.metadataStore, Config{
		Server: store.serverConfig, Disk: store.diskConfig, Global: store.globalConfig,
	})
	require.NoError(t, err)
	require.True(t, restarted.Exists(hash, int64(len("survives-restart"))))

	// Content removed behind the index's back stops being advertised on the
	// first read that misses it
	require.NoError(t, os.RemoveAll(store.pageDir(hash)))
	require.True(t, restarted.Exists(hash))
	_, err = restarted.ReadAt(hash, 0, make([]byte, 4))
	require.ErrorIs(t, err, ErrContentNotFound)
	require.False(t, restarted.Exists(hash))
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

	// Abandoned dirs were not written through the store; the rescan finds them
	store.rebuildContentIndex()
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
