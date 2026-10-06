package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func stubDiskUsage(t *testing.T, stat func(string) (diskUsageSnapshot, error)) {
	t.Helper()
	previous := statDiskUsage
	statDiskUsage = stat
	t.Cleanup(func() { statDiskUsage = previous })
}

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
	entry.completedAt = lastAccess
	store.index.put(hash, entry)
	return hash
}

func evictionCandidateFor(t *testing.T, store *Store, hash string) evictionCandidate {
	t.Helper()
	for _, candidate := range store.evictionCandidates() {
		if candidate.hash == hash {
			return candidate
		}
	}
	t.Fatalf("%s is not an eviction candidate", hash)
	return evictionCandidate{}
}

func TestRemoveContentSkipsContentTouchedSinceItWasChosen(t *testing.T) {
	store := newTestStore(t, 5)
	hash := addEvictionTestContent(t, store, "read-after-chosen", time.Now().Add(-2*time.Hour))
	candidate := evictionCandidateFor(t, store, hash)

	// A read between the pass snapshot and the removal keeps the content.
	store.touchContentAccess(hash)
	require.ErrorIs(t, store.removeContent(candidate), errContentTouched)
	require.True(t, store.Exists(hash))

	require.NoError(t, store.removeContent(evictionCandidateFor(t, store, hash)))
	require.False(t, store.Exists(hash))
}

func TestReadProbeKeepsOldContentUntilProtectionArrives(t *testing.T) {
	store := newTestStore(t, 5)
	hash := addEvictionTestContent(t, store, "active-image", time.Now().Add(-2*time.Hour))
	cold := addEvictionTestContent(t, store, "unused-image", time.Now().Add(-time.Hour))
	candidate := evictionCandidateFor(t, store, hash)
	server := &Server{cas: store}

	response, err := server.HasContent(context.Background(), &proto.CacheHasContentRequest{Hash: hash, ExpectedSize: 999})
	require.NoError(t, err)
	require.False(t, response.Exists)
	require.Equal(t, candidate.lastAccess, evictionCandidateFor(t, store, hash).lastAccess)
	response, err = server.HasContent(context.Background(), &proto.CacheHasContentRequest{Hash: hash, ExpectedSize: int64(len("active-image"))})
	require.NoError(t, err)
	require.True(t, response.Exists)
	require.ErrorIs(t, store.removeContent(candidate), errContentTouched)
	store.SetProtectedContent(map[string]struct{}{}) // Report has not arrived yet.
	require.True(t, store.maybeEvictDiskCache(diskUsageSnapshot{totalBytes: 1000, usedBytes: 850, availableBytes: 150, usagePct: .85}))
	require.True(t, store.Exists(hash))
	require.False(t, store.Exists(cold))
}

func TestMemoryHitRenewsCompleteDiskContent(t *testing.T) {
	owner := newTestStore(t, 5)
	cfg := owner.serverConfig
	cfg.MaxCachePct = 1
	store, err := NewStore(context.Background(), owner.currentHost, owner.locality, owner.metadataStore, Config{Server: cfg})
	require.NoError(t, err)
	t.Cleanup(store.Cleanup)
	hash := addEvictionTestContent(t, store, "memory-and-disk", time.Now().Add(-2*time.Hour))
	store.cache.Wait()
	_, exists := store.cache.GetTTL(hash)
	require.True(t, exists)
	candidate := evictionCandidateFor(t, store, hash)
	response, err := (&Server{cas: store}).HasContent(context.Background(), &proto.CacheHasContentRequest{Hash: hash})
	require.NoError(t, err)
	require.True(t, response.Exists)
	require.ErrorIs(t, store.removeContent(candidate), errContentTouched)
}

func TestLocalViewReadKeepsOwnersEvictionCandidate(t *testing.T) {
	for _, access := range []string{"complete", "page", "read"} {
		t.Run(access, func(t *testing.T) {
			owner := newTestStore(t, 5)
			hash := addEvictionTestContent(t, owner, "shared-image", time.Now().Add(-2*time.Hour))
			candidate := evictionCandidateFor(t, owner, hash)
			view, err := NewStore(context.Background(), &Host{HostId: "view"}, "test", NewMockCacheMetadataStore(), Config{Server: owner.serverConfig})
			require.NoError(t, err)
			t.Cleanup(view.Cleanup)
			switch access {
			case "complete":
				require.True(t, (&Client{localDiskStore: view}).LocalContentComplete(hash))
			case "page":
				_, _, n, ok, err := view.PageRegion(hash, 0, 1)
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, 1, n)
			case "read":
				_, err := view.ReadAt(hash, 0, make([]byte, 1))
				require.NoError(t, err)
			}
			require.ErrorIs(t, owner.removeContent(candidate), errContentTouched)
			require.True(t, owner.Exists(hash))
			evicted, _ := owner.evictLRU(1 << 30)
			require.Zero(t, evicted)
		})
	}
}

func TestSustainedLocalViewReadsPersistOwnersRecency(t *testing.T) {
	owner := newTestStore(t, 5)
	start := time.Now().Truncate(evictionAccessTouchInterval).Add(-4 * evictionAccessTouchInterval)
	hash := addEvictionTestContent(t, owner, "continuously-read-image", start)
	candidate := evictionCandidateFor(t, owner, hash)
	view, err := NewStore(context.Background(), &Host{HostId: "view"}, "test", NewMockCacheMetadataStore(), Config{Server: owner.serverConfig})
	require.NoError(t, err)
	t.Cleanup(view.Cleanup)
	for minute := 1; minute <= 20; minute++ {
		now := start.Add(time.Duration(minute) * time.Minute)
		status, persist := view.index.touchComplete(hash, view.serverConfig.PageSizeBytes, now)
		require.Equal(t, contentStatusComplete, status)
		if persist {
			require.NoError(t, os.Chtimes(view.completeMarkerPath(hash), now, now))
		}
	}
	require.ErrorIs(t, owner.removeContent(candidate), errContentTouched)
	require.True(t, owner.Exists(hash))
}

func TestPageRegionPreservesIncompletePromotedPages(t *testing.T) {
	store := newTestStore(t, 5)
	hash := strings.Repeat("c", 64)
	store.PutFullPages(hash, 0, []byte("firstsecond"))
	require.False(t, store.Exists(hash))
	path, offset, n, ok, err := store.PageRegion(hash, 0, 5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Zero(t, offset)
	require.Equal(t, 5, n)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first", string(data))
	require.False(t, store.Exists(hash))
}

func TestReadProbesDoNotWaitForWriter(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%t", complete), func(t *testing.T) {
			store := newTestStore(t, 5)
			hash := "still-writing"
			if complete {
				hash = addEvictionTestContent(t, store, "cached-image", time.Now().Add(-2*time.Hour))
			}
			defer store.lockObject(hash).Unlock() // Also held by writers sharing this stripe.
			completed := make(chan bool, 1)
			go func() {
				response, err := (&Server{cas: store}).HasContent(context.Background(), &proto.CacheHasContentRequest{Hash: hash})
				ok := err == nil && response.Exists == complete
				if complete {
					_, _, n, present, err := store.PageRegion(hash, 0, 1)
					ok = ok && err == nil && present && n == 1
				}
				completed <- ok
			}()
			select {
			case ok := <-completed:
				require.True(t, ok)
			case <-time.After(time.Second):
				t.Fatal("read probe blocked behind a writer")
			}
		})
	}
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

func TestEvictLRUEvictsFreshlyStoredContentLast(t *testing.T) {
	store := newTestStore(t, 5)

	now := time.Now()
	cold := addEvictionTestContent(t, store, "cold-content", now.Add(-2*time.Hour))
	// Stored long ago, read a minute ago.
	hot := addEvictionTestContent(t, store, "hot-content!", now.Add(-2*time.Hour))
	entry, ok := store.index.get(hot)
	require.True(t, ok)
	entry.lastAccess = now.Add(-time.Minute)
	store.index.put(hot, entry)
	// Written twenty minutes ago and not read yet: colder than hot by access
	// time, but the write is what a container is about to read.
	fresh := addEvictionTestContent(t, store, "fresh-content", now.Add(-20*time.Minute))
	store.rebuildContentIndex()

	// Normal pass: fresh content is guarded like recently-read content.
	evicted, _ := store.evictLRUWithProtected(1<<30, nil)
	require.Equal(t, 1, evicted)
	require.False(t, store.Exists(cold))
	require.True(t, store.Exists(fresh))

	// Pressure also keeps the read guard, even before stub protection arrives.
	evicted, _ = store.evictLRUWithProtected(int64(len("hot-content!")), nil)
	require.Zero(t, evicted)
	require.True(t, store.Exists(hot))
	require.True(t, store.Exists(fresh))
	evicted, _ = store.evictLRUWithProtected(1<<30, map[string]struct{}{})
	require.Zero(t, evicted, "fresh writes survive normal pressure without any stub protection")
}

func TestEvictWatermarkPctAcceptsWholePercent(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheEvictWatermarkPct = 80

	require.Equal(t, 0.80, store.evictWatermarkPct())
}

func TestMaybeEvictDiskCachePreservesRecentUnprotectedBeforeReport(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheEvictWatermarkPct = 0.80
	var events []CacheChurnEvent
	store.SetChurnSink(func(event CacheChurnEvent) {
		events = append(events, event)
	})

	now := time.Now()
	protected := addEvictionTestContent(t, store, "protected-hot-content", now.Add(-2*time.Minute))
	unprotected := addEvictionTestContent(t, store, "unprotected-hot-content", now.Add(-time.Hour))
	store.touchContentAccess(unprotected)
	store.SetProtectedContent(map[string]struct{}{protected: struct{}{}})

	evicted := store.maybeEvictDiskCache(diskUsageSnapshot{
		totalBytes:     1000,
		usedBytes:      850,
		availableBytes: 150,
		usagePct:       0.85,
	})

	require.False(t, evicted)
	require.True(t, store.Exists(protected))
	require.True(t, store.Exists(unprotected))
	require.Len(t, events, 1)
	require.Equal(t, CacheChurnStatusNothingEvictable, events[0].Status)
	require.Equal(t, CacheChurnOperationDiskEviction, events[0].Operation)
	require.Zero(t, events[0].EvictedObjects)
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

	protected := addEvictionTestContent(t, store, "protected-hot-content", time.Now().Add(-time.Hour))
	fresh := addEvictionTestContent(t, store, "fresh-volume-content", time.Now().Add(-20*time.Minute))
	store.SetProtectedContent(map[string]struct{}{protected: struct{}{}})

	evicted := store.maybeEvictDiskCache(diskUsageSnapshot{
		totalBytes:     1000,
		usedBytes:      850,
		availableBytes: 150,
		usagePct:       0.85,
	})

	require.False(t, evicted)
	require.True(t, store.Exists(protected))
	require.True(t, store.Exists(fresh))
	require.Len(t, events, 1)
	require.Equal(t, CacheChurnStatusNothingEvictable, events[0].Status)
	require.Equal(t, 1, events[0].ProtectedCandidates)
	require.Equal(t, 1, events[0].RecentCandidates)
}

func TestMaybeEvictDiskCachePreservesProtectedContentBelowHardReserve(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheEvictWatermarkPct = 0.80
	store.serverConfig.DiskCacheMaxUsagePct = 0.95
	store.diskConfig.MinFreeBytes = 180
	var events []CacheChurnEvent
	store.SetChurnSink(func(event CacheChurnEvent) {
		events = append(events, event)
	})

	protected := addEvictionTestContent(t, store, "protected-hot-content", time.Now().Add(-time.Hour))
	fresh := addEvictionTestContent(t, store, "fresh-volume-content", time.Now().Add(-20*time.Minute))
	store.SetProtectedContent(map[string]struct{}{protected: struct{}{}})

	evicted := store.maybeEvictDiskCache(diskUsageSnapshot{
		totalBytes:     1000,
		usedBytes:      850,
		availableBytes: 150,
		usagePct:       0.85,
	})

	require.False(t, evicted)
	require.True(t, store.Exists(protected))
	require.True(t, store.Exists(fresh))
	require.Len(t, events, 1)
	require.Equal(t, CacheChurnStatusNothingEvictable, events[0].Status)
	require.Zero(t, events[0].ProtectedObjects)
	require.Zero(t, events[0].ProtectedFreedBytes)
}

func TestIndexReadRenewalThrottlesMarkerPersistence(t *testing.T) {
	store := newTestStore(t, 5)
	start := time.Now().Truncate(evictionAccessTouchInterval)
	hash := addEvictionTestContent(t, store, "touched-content", start.Add(-time.Hour))
	for _, at := range []time.Time{start.Add(time.Minute), start.Add(2 * time.Minute)} {
		known, persist := store.index.touch(hash, at, evictionAccessTouchInterval)
		require.True(t, known)
		if persist {
			require.NoError(t, os.Chtimes(store.completeMarkerPath(hash), at, at))
		}
	}
	info, err := os.Stat(store.completeMarkerPath(hash))
	require.NoError(t, err)
	require.Equal(t, start.Add(time.Minute), info.ModTime())
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

	// Content completed long before the walk, whose marker then vanished on
	// disk: that is drift, and the walk's view of it must win.
	drifted := addEvictionTestContent(t, store, "marker-lost-on-disk", time.Now())
	driftedEntry, _ := store.index.get(drifted)
	driftedEntry.completedAt = time.Now().Add(-time.Hour)
	store.index.put(drifted, driftedEntry)
	require.NoError(t, os.Remove(store.completeMarkerPath(drifted)))

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
	require.False(t, store.Exists(drifted))
}

// TestDiskUsageReportsIndexedBytesAsEvictable: RefreshDiskUsage tells the
// reconciler how much of the disk is content it can evict, so the protected
// budget is net of everything else on the volume.
func TestDiskUsageReportsIndexedBytesAsEvictable(t *testing.T) {
	store := newTestStore(t, 5)
	require.Equal(t, int64(0), store.index.bytes())

	a := addEvictionTestContent(t, store, "first-object", time.Now())
	addEvictionTestContent(t, store, "second-object-longer", time.Now())
	sizeA, _ := store.index.get(a)
	require.Greater(t, sizeA.size, int64(0))
	require.Equal(t, int64(len("first-object")+len("second-object-longer")), store.index.bytes())

	server := &Server{cas: store}
	usage, err := server.RefreshDiskUsage()
	require.NoError(t, err)
	require.Equal(t, uint64(store.index.bytes()), usage.EvictableBytes)
	require.Greater(t, usage.UsedBytes, usage.EvictableBytes, "the temp dir's filesystem holds more than the two objects")

	store.index.forget(a)
	require.Equal(t, int64(len("second-object-longer")), store.index.bytes())
	store.index.forget(a)
	require.Equal(t, int64(len("second-object-longer")), store.index.bytes(), "forgetting twice must not double-subtract")

	// Overwriting an entry replaces its contribution rather than adding to it.
	b := addEvictionTestContent(t, store, "third", time.Now())
	entryB, _ := store.index.get(b)
	entryB.size = 100
	store.index.put(b, entryB)
	require.Equal(t, int64(len("second-object-longer")+100), store.index.bytes())

	// A rebuild from disk resets the total to what the walk found, which still
	// includes the object whose index entry was forgotten above.
	store.index.replace(store.scanContent(), time.Now())
	require.Equal(t, int64(len("first-object")+len("second-object-longer")+len("third")), store.index.bytes())
	var walked int64
	for _, entry := range store.scanContent() {
		walked += entry.size
	}
	require.Equal(t, walked, store.index.bytes())
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

func TestDiskWriteGuardEvictsBeforeRefusingAStore(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheMaxUsagePct = 0.95
	store.serverConfig.DiskCacheEvictWatermarkPct = 0.80

	old := addEvictionTestContent(t, store, "stale content nobody has read in a while", time.Now().Add(-time.Hour))

	// The filesystem reports itself over the hard limit until something is
	// evicted, then comfortably under it.
	var stats, evictedAt int
	stubDiskUsage(t, func(string) (diskUsageSnapshot, error) {
		stats++
		if !store.Exists(old) {
			if evictedAt == 0 {
				evictedAt = stats
			}
			return diskUsageSnapshot{totalBytes: 1000, usedBytes: 700, availableBytes: 300, usagePct: 0.70}, nil
		}
		return diskUsageSnapshot{totalBytes: 1000, usedBytes: 960, availableBytes: 40, usagePct: 0.96}, nil
	})

	// A plain (non-evicting) refresh, as the periodic check would leave it.
	_, err := store.refreshDiskCacheUsage(false)
	require.NoError(t, err)
	require.True(t, store.diskCachedUsageExceeded)

	// A store arriving now must evict and go through instead of failing.
	require.True(t, store.diskWriteAllowed())
	require.False(t, store.Exists(old), "stale content should have been evicted to admit the write")
	require.NotZero(t, evictedAt)

	hash, _, err := store.AddReader(context.Background(), bytes.NewReader([]byte("fresh content that needed the room")))
	require.NoError(t, err)
	require.True(t, store.Exists(hash))
}

// Pressure found by the startup accounting pass, before any write has run the
// guard, must be evicted by the first write like any other pressure.
func TestDiskWriteGuardEvictsStartupPressure(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheMaxUsagePct = 0.95
	store.serverConfig.DiskCacheEvictWatermarkPct = 0.80

	old := addEvictionTestContent(t, store, "stale content nobody has read in a while", time.Now().Add(-time.Hour))

	stubDiskUsage(t, func(string) (diskUsageSnapshot, error) {
		if !store.Exists(old) {
			return diskUsageSnapshot{totalBytes: 1000, usedBytes: 700, availableBytes: 300, usagePct: 0.70}, nil
		}
		return diskUsageSnapshot{totalBytes: 1000, usedBytes: 960, availableBytes: 40, usagePct: 0.96}, nil
	})

	// What StartDiskMonitor leaves behind on a host that restarts over the
	// limit: the gate set, no guard check on record yet.
	store.lastDiskGuardCheckNanos.Store(0)
	_, err := store.refreshDiskCacheUsage(false)
	require.NoError(t, err)
	require.True(t, store.diskCachedUsageExceeded)

	require.True(t, store.diskWriteAllowed())
	require.False(t, store.Exists(old))
}

// A write that finds an eviction pass already running waits for it and goes
// through on the room it made, rather than failing on the stale gate.
func TestDiskWriteGuardConcurrentWritersWaitForInflightEviction(t *testing.T) {
	store := newTestStore(t, 5)
	store.serverConfig.DiskCacheMaxUsagePct = 0.95
	store.serverConfig.DiskCacheEvictWatermarkPct = 0.80

	old := addEvictionTestContent(t, store, "stale content nobody has read in a while", time.Now().Add(-time.Hour))

	// The first stat taken by an evicting pass blocks until released, holding
	// the pass (and the guard) open.
	evictingStat := make(chan struct{})
	release := make(chan struct{})
	var evictPasses int
	stubDiskUsage(t, func(string) (diskUsageSnapshot, error) {
		if store.evictMu.TryLock() {
			store.evictMu.Unlock()
		} else if store.Exists(old) {
			evictPasses++
			if evictPasses == 1 {
				close(evictingStat)
				<-release
			}
		}
		if !store.Exists(old) {
			return diskUsageSnapshot{totalBytes: 1000, usedBytes: 700, availableBytes: 300, usagePct: 0.70}, nil
		}
		return diskUsageSnapshot{totalBytes: 1000, usedBytes: 960, availableBytes: 40, usagePct: 0.96}, nil
	})

	_, err := store.refreshDiskCacheUsage(false)
	require.NoError(t, err)
	require.True(t, store.diskCachedUsageExceeded)

	first := make(chan bool, 1)
	go func() { first <- store.diskWriteAllowed() }()
	<-evictingStat

	second := make(chan bool, 1)
	go func() { second <- store.diskWriteAllowed() }()
	select {
	case allowed := <-second:
		t.Fatalf("second writer answered %v while the eviction was still running", allowed)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	require.True(t, <-first)
	require.True(t, <-second)
	require.False(t, store.Exists(old))
	require.Equal(t, 1, evictPasses, "the waiting writer must not start a second eviction pass")
}

func TestDiskAdmissionAccountsForConcurrentWrites(t *testing.T) {
	store := newTestStore(t, 5)
	stubDiskUsage(t, func(string) (diskUsageSnapshot, error) {
		return diskUsageSnapshot{totalBytes: 1000, availableBytes: 1000}, nil
	})
	first, err := store.reserveDiskWrite(600)
	require.NoError(t, err)
	_, err = store.reserveDiskWrite(600)
	require.ErrorIs(t, err, errDiskCacheCapacity)
	first.release()
	second, err := store.reserveDiskWrite(600)
	require.NoError(t, err)
	second.release()
	require.Zero(t, store.diskWrites.pending)
}

func TestDiskAdmissionStopsStreamsWithoutEvictingProtectedContent(t *testing.T) {
	for _, method := range []string{"reader", "expected-hash", "parallel-pages", "add"} {
		t.Run(method, func(t *testing.T) {
			store := newTestStore(t, 5)
			store.serverConfig.DiskCacheMaxUsagePct = 0.95
			protected := addEvictionTestContent(t, store, "keep", time.Now().Add(-time.Hour))
			store.SetProtectedContent(map[string]struct{}{protected: {}})
			store.lastDiskGuardCheckNanos.Store(time.Now().UnixNano())
			stubDiskUsage(t, func(string) (diskUsageSnapshot, error) {
				var written uint64
				err := filepath.WalkDir(store.diskCacheDir, func(path string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if path == store.pageDir(protected) {
						return filepath.SkipDir
					}
					if !entry.IsDir() {
						info, err := entry.Info()
						if err != nil {
							return err
						}
						written += uint64(info.Size())
					}
					return nil
				})
				return diskUsageSnapshot{totalBytes: 1000, usedBytes: 940 + written, availableBytes: 60 - written, usagePct: float64(940+written) / 1000}, err
			})
			content := []byte("more-than-one-page")
			hash := fmt.Sprintf("%x", sha256.Sum256(content))
			var err error
			switch method {
			case "add":
				err = store.Add(context.Background(), hash, content)
			case "reader":
				_, _, err = store.AddReader(context.Background(), bytes.NewReader(content))
			case "expected-hash":
				_, _, err = store.AddReaderWithExpectedHash(context.Background(), bytes.NewReader(content), hash)
			case "parallel-pages":
				_, _, err = store.AddPageSourceWithExpectedHash(context.Background(), hash, int64(len(content)), 3,
					func(_ context.Context, _ int64, offset int64, page []byte) (int, error) {
						return copy(page, content[offset:]), nil
					})
			}
			require.ErrorContains(t, err, "disk cache capacity exceeded")
			require.False(t, store.Exists(hash), "refused content must not be advertised complete")
			if method == "add" {
				_, statErr := os.Stat(store.pageDir(hash))
				require.True(t, os.IsNotExist(statErr), "mid-stream refusal must remove published partial pages")
			}
			require.True(t, store.Exists(protected))
			require.Zero(t, store.diskWrites.pending)
		})
	}
}

func TestReconciledCacheWaitsForProtectionBeforeEviction(t *testing.T) {
	InitLogger(false, false)
	store, err := NewStore(context.Background(), &Host{HostId: "test"}, "test", NewMockCacheMetadataStore(), Config{
		Server: ServerConfig{PageSizeBytes: 5, DiskCacheDir: t.TempDir(), DiskCacheMaxUsagePct: 100},
		Disk:   DiskConfig{Enabled: true}, Reconciliation: ReconciliationConfig{Enabled: true},
	})
	require.NoError(t, err)
	t.Cleanup(store.Cleanup)
	old := time.Now().Add(-24 * time.Hour)
	required := addEvictionTestContent(t, store, "required", old)
	unneeded := addEvictionTestContent(t, store, "unneeded", old)
	snapshot := diskUsageSnapshot{totalBytes: 1000, usedBytes: 900, availableBytes: 100, usagePct: .9}
	store.serverConfig.DiskCacheEvictWatermarkPct = .8
	require.False(t, store.maybeEvictDiskCache(snapshot))
	evicted, _ := store.PruneContentNotProtected(nil, time.Hour)
	require.Zero(t, evicted)
	require.True(t, store.Exists(required))
	require.True(t, store.Exists(unneeded))

	candidate := evictionCandidateFor(t, store, required)
	store.SetProtectedContent(map[string]struct{}{required: {}})
	require.ErrorIs(t, store.removeUnprotectedContent(candidate), errContentTouched)
	require.True(t, store.maybeEvictDiskCache(snapshot))
	require.True(t, store.Exists(required))
	require.False(t, store.Exists(unneeded))
}
