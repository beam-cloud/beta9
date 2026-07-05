package cache

// LRU disk eviction. Content recency is the complete marker file's
// mtime, refreshed on read (throttled), so it survives restarts with no extra
// index. When filesystem usage crosses the eviction watermark, the store first
// deletes unprotected stale content. If the node remains under pressure, it can
// evict newer unprotected content, and only uses protected content to clear the
// hard write gate.

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	// defaultDiskCacheEvictWatermarkPct keeps eviction comfortably below
	// kubelet DiskPressure thresholds (typically ~0.85-0.90).
	defaultDiskCacheEvictWatermarkPct = 0.80
	// evictionAccessTouchInterval throttles per-hash marker mtime updates on
	// the read path.
	evictionAccessTouchInterval = 5 * time.Minute
	// evictionRecentAccessGuard preserves hot content during normal eviction.
	// Hard disk pressure may still evict it to keep the node healthy.
	evictionRecentAccessGuard = 10 * time.Minute
	// evictionIncompleteContentGrace keeps in-flight writes out of eviction,
	// while allowing abandoned marker-less v2 dirs to be reclaimed.
	evictionIncompleteContentGrace = 30 * time.Minute
	// accessTouchMapSweepSize bounds the in-memory touch throttle map.
	accessTouchMapSweepSize = 65536
)

type evictionCandidate struct {
	hash       string
	dir        string
	lastAccess time.Time
	sizeBytes  int64
}

func (cas *Store) evictWatermarkPct() float64 {
	pct := normalizedPct(cas.serverConfig.DiskCacheEvictWatermarkPct)
	if pct <= 0 || pct > 1 {
		pct = defaultDiskCacheEvictWatermarkPct
	}
	return pct
}

// touchContentAccess records a successful read of hash so eviction prefers
// newer content. Throttled per hash; persisted via the complete marker mtime.
func (cas *Store) touchContentAccess(hash string) {
	if hash == "" {
		return
	}

	now := time.Now()
	cas.accessTouchMu.Lock()
	if last, ok := cas.accessTouches[hash]; ok && now.Sub(last) < evictionAccessTouchInterval {
		cas.accessTouchMu.Unlock()
		return
	}
	cas.accessTouches[hash] = now
	if len(cas.accessTouches) > accessTouchMapSweepSize {
		// Entries past the throttle window are already persisted as marker
		// mtimes and carry no extra information.
		for h, t := range cas.accessTouches {
			if now.Sub(t) >= evictionAccessTouchInterval {
				delete(cas.accessTouches, h)
			}
		}
	}
	cas.accessTouchMu.Unlock()

	// Best-effort; legacy-layout content without a v2 marker falls back to
	// directory mtime during eviction.
	_ = os.Chtimes(cas.completeMarkerPath(hash), now, now)
}

// maybeEvictDiskCache evicts least-recently-read content when filesystem usage
// is above the eviction watermark or below the configured free-byte reserve. It
// reports whether anything was evicted.
func (cas *Store) maybeEvictDiskCache(snapshot diskUsageSnapshot) bool {
	watermark := cas.evictWatermarkPct()
	if snapshot.totalBytes <= 0 {
		return false
	}

	bytesToFree := int64(0)
	if snapshot.usagePct > watermark {
		targetUsedBytes := int64(watermark * float64(snapshot.totalBytes))
		bytesToFree = int64(snapshot.usedBytes) - targetUsedBytes
	}
	if cas.diskConfig.MinFreeBytes > 0 {
		reserveDeficit := cas.diskConfig.MinFreeBytes - int64(snapshot.availableBytes)
		if reserveDeficit > bytesToFree {
			bytesToFree = reserveDeficit
		}
	}
	if bytesToFree <= 0 {
		return false
	}

	started := time.Now()
	protected := cas.protectedContentSnapshot()
	evicted, freed := cas.evictLRUWithProtected(bytesToFree, protected, false)
	if freed < bytesToFree {
		moreEvicted, moreFreed := cas.evictLRUWithProtected(bytesToFree-freed, protected, true)
		evicted += moreEvicted
		freed += moreFreed
	}
	protectedEvicted := 0
	var protectedFreed int64
	if criticalBytes := cas.criticalDiskPressureBytesToFree(snapshot, freed); criticalBytes > 0 {
		protectedEvicted, protectedFreed = cas.evictLRUWithProtected(criticalBytes, nil, true)
		if protectedEvicted > 0 {
			Logger.Warnf("disk cache evicted protected content under critical pressure: freed %d bytes across %d objects (usage=%.2f watermark=%.2f available=%d reserve=%d)", protectedFreed, protectedEvicted, snapshot.usagePct, watermark, snapshot.availableBytes, cas.diskConfig.MinFreeBytes)
			evicted += protectedEvicted
			freed += protectedFreed
		}
	}

	if evicted == 0 {
		total, protectedCount, recentCount, evictableCount := cas.evictionCandidateStats(protected)
		Logger.Warnf("disk cache eviction found nothing evictable: usage=%.2f watermark=%.2f available=%d reserve=%d want_free_bytes=%d candidates=%d protected=%d recent=%d eligible=%d", snapshot.usagePct, watermark, snapshot.availableBytes, cas.diskConfig.MinFreeBytes, bytesToFree, total, protectedCount, recentCount, evictableCount)
		cas.emitDiskEvictionChurn(CacheChurnStatusNothingEvictable, snapshot, watermark, bytesToFree, 0, 0, 0, 0, total, protectedCount, recentCount, evictableCount)
		return false
	}

	Logger.Infof("disk cache eviction freed %d bytes across %d objects in %s (usage=%.2f watermark=%.2f available=%d reserve=%d)", freed, evicted, time.Since(started).Truncate(time.Millisecond), snapshot.usagePct, watermark, snapshot.availableBytes, cas.diskConfig.MinFreeBytes)
	if evicted > 0 {
		status := CacheChurnStatusEvicted
		if protectedEvicted > 0 {
			status = CacheChurnStatusProtectedEvicted
		}
		cas.emitDiskEvictionChurn(status, snapshot, watermark, bytesToFree, evicted, freed, protectedEvicted, protectedFreed, 0, 0, 0, 0)
	}
	return true
}

func (cas *Store) emitDiskEvictionChurn(status string, snapshot diskUsageSnapshot, watermark float64, targetFreeBytes int64, evictedObjects int, freedBytes int64, protectedObjects int, protectedFreedBytes int64, totalCandidates int, protectedCandidates int, recentCandidates int, eligibleCandidates int) {
	cas.emitChurnEvent(CacheChurnEvent{
		Operation:           CacheChurnOperationDiskEviction,
		Status:              status,
		Path:                snapshot.path,
		EvictedObjects:      evictedObjects,
		ProtectedObjects:    protectedObjects,
		FreedBytes:          freedBytes,
		ProtectedFreedBytes: protectedFreedBytes,
		UsagePct:            snapshot.usagePct,
		WatermarkPct:        watermark,
		AvailableBytes:      snapshot.availableBytes,
		ReserveBytes:        cas.diskConfig.MinFreeBytes,
		TargetFreeBytes:     targetFreeBytes,
		TotalCandidates:     totalCandidates,
		ProtectedCandidates: protectedCandidates,
		RecentCandidates:    recentCandidates,
		EligibleCandidates:  eligibleCandidates,
		Timestamp:           time.Now().UTC(),
	})
}

func (cas *Store) criticalDiskPressureBytesToFree(snapshot diskUsageSnapshot, alreadyFreed int64) int64 {
	if snapshot.totalBytes <= 0 {
		return 0
	}

	projectedUsed := int64(snapshot.usedBytes) - alreadyFreed
	if projectedUsed < 0 {
		projectedUsed = 0
	}
	projectedAvailable := int64(snapshot.availableBytes) + alreadyFreed

	bytesToFree := int64(0)
	maxUsagePct := normalizedPct(cas.serverConfig.DiskCacheMaxUsagePct)
	if maxUsagePct <= 0 {
		maxUsagePct = defaultHostStorageCapacityThresholdPct
	}
	if maxUsagePct > 0 && maxUsagePct < 1 {
		targetUsedBytes := int64(maxUsagePct * float64(snapshot.totalBytes))
		if deficit := projectedUsed - targetUsedBytes; deficit > bytesToFree {
			bytesToFree = deficit
		}
	}
	if cas.diskConfig.MinFreeBytes > 0 {
		if deficit := cas.diskConfig.MinFreeBytes - projectedAvailable; deficit > bytesToFree {
			bytesToFree = deficit
		}
	}
	return bytesToFree
}

// evictLRU deletes least-recently-read content until roughly bytesToFree bytes
// have been reclaimed. Returns the number of objects evicted and the bytes
// freed.
func (cas *Store) evictLRU(bytesToFree int64) (int, int64) {
	return cas.evictLRUWithProtected(bytesToFree, nil, false)
}

func (cas *Store) evictLRUWithProtected(bytesToFree int64, protected map[string]struct{}, allowRecent bool) (int, int64) {
	if bytesToFree <= 0 {
		return 0, 0
	}

	candidates := cas.evictionCandidates()
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastAccess.Before(candidates[j].lastAccess)
	})

	evicted := 0
	var freed int64
	cutoff := time.Now().Add(-evictionRecentAccessGuard)
	for _, candidate := range candidates {
		if freed >= bytesToFree {
			break
		}
		if _, ok := protected[candidate.hash]; ok {
			continue
		}
		// Oldest-first order: once we reach recently-read content, nothing
		// after it is evictable either.
		if !allowRecent && candidate.lastAccess.After(cutoff) {
			break
		}
		if err := cas.removeContent(candidate); err != nil {
			Logger.Warnf("disk cache eviction failed to remove %s: %v", candidate.hash, err)
			continue
		}
		evicted++
		freed += candidate.sizeBytes
	}
	return evicted, freed
}

func (cas *Store) evictionCandidateStats(protected map[string]struct{}) (int, int, int, int) {
	candidates := cas.evictionCandidates()
	cutoff := time.Now().Add(-evictionRecentAccessGuard)
	protectedCount := 0
	recentCount := 0
	evictableCount := 0
	for _, candidate := range candidates {
		if _, ok := protected[candidate.hash]; ok {
			protectedCount++
			continue
		}
		if candidate.lastAccess.After(cutoff) {
			recentCount++
			continue
		}
		evictableCount++
	}
	return len(candidates), protectedCount, recentCount, evictableCount
}

// PruneContentNotProtected removes content that has not been used inside ttl
// and is not required by any recent stub. It is intentionally driven by the
// embedded cache owner so non-owner workers never run prune loops.
func (cas *Store) PruneContentNotProtected(protected map[string]struct{}, ttl time.Duration) (int, int64) {
	if cas == nil || ttl <= 0 {
		return 0, 0
	}
	candidates := cas.evictionCandidates()
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].lastAccess.Before(candidates[j].lastAccess)
	})

	cutoff := time.Now().Add(-ttl)
	evicted := 0
	var freed int64
	for _, candidate := range candidates {
		if _, ok := protected[candidate.hash]; ok {
			continue
		}
		if candidate.lastAccess.After(cutoff) {
			break
		}
		if err := cas.removeContent(candidate); err != nil {
			Logger.Warnf("disk cache stale prune failed to remove %s: %v", candidate.hash, err)
			continue
		}
		evicted++
		freed += candidate.sizeBytes
	}
	return evicted, freed
}

func (cas *Store) PressureEvictContent(protected map[string]struct{}, bytesToFree int64) (int, int64) {
	return cas.evictLRUWithProtected(bytesToFree, protected, true)
}

func (cas *Store) SetProtectedContent(protected map[string]struct{}) {
	if cas == nil {
		return
	}
	next := make(map[string]struct{}, len(protected))
	for hash := range protected {
		if hash != "" {
			next[hash] = struct{}{}
		}
	}
	cas.protectedMu.Lock()
	cas.protectedContent = next
	cas.protectedMu.Unlock()
}

func (cas *Store) protectedContentSnapshot() map[string]struct{} {
	if cas == nil {
		return nil
	}
	cas.protectedMu.RLock()
	defer cas.protectedMu.RUnlock()
	if len(cas.protectedContent) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(cas.protectedContent))
	for hash := range cas.protectedContent {
		out[hash] = struct{}{}
	}
	return out
}

// evictionCandidates lists all on-disk content with its last access time and
// size, covering both the v2 (pages/<bucket>/<hash>) and legacy (<hash>)
// layouts.
func (cas *Store) evictionCandidates() []evictionCandidate {
	candidates := []evictionCandidate{}
	now := time.Now()

	buckets, _ := os.ReadDir(filepath.Join(cas.diskCacheDir, "pages"))
	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		bucketDir := filepath.Join(cas.diskCacheDir, "pages", bucket.Name())
		entries, _ := os.ReadDir(bucketDir)
		for _, entry := range entries {
			if entry.IsDir() && isCacheContentHash(entry.Name()) {
				dir := filepath.Join(bucketDir, entry.Name())
				if _, err := os.Stat(filepath.Join(dir, cacheCompleteMarkerName)); err == nil {
					candidates = cas.appendCandidate(candidates, entry.Name(), dir)
				} else if os.IsNotExist(err) && incompleteContentDirEvictable(dir, now) {
					candidates = cas.appendCandidate(candidates, entry.Name(), dir)
				}
			}
		}
	}

	entries, _ := os.ReadDir(cas.diskCacheDir)
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != "pages" && isCacheContentHash(entry.Name()) {
			candidates = cas.appendCandidate(candidates, entry.Name(), filepath.Join(cas.diskCacheDir, entry.Name()))
		}
	}

	return candidates
}

func incompleteContentDirEvictable(dir string, now time.Time) bool {
	info, err := os.Stat(dir)
	if err != nil {
		return false
	}
	return now.Sub(info.ModTime()) >= evictionIncompleteContentGrace
}

func isCacheContentHash(name string) bool {
	if len(name) != 64 {
		return false
	}
	return strings.IndexFunc(name, func(r rune) bool {
		return (r < '0' || r > '9') && (r < 'a' || r > 'f')
	}) == -1
}

func (cas *Store) appendCandidate(candidates []evictionCandidate, hash, dir string) []evictionCandidate {
	info, err := os.Stat(dir)
	if err != nil {
		return candidates
	}

	// Prefer the complete marker mtime: it is refreshed on read and reflects
	// content recency rather than page churn.
	lastAccess := info.ModTime()
	if markerInfo, err := os.Stat(filepath.Join(dir, cacheCompleteMarkerName)); err == nil {
		lastAccess = markerInfo.ModTime()
	}

	// An in-memory touch may be fresher than the on-disk mtime since Chtimes
	// is throttled
	cas.accessTouchMu.Lock()
	if touched, ok := cas.accessTouches[hash]; ok && touched.After(lastAccess) {
		lastAccess = touched
	}
	cas.accessTouchMu.Unlock()

	return append(candidates, evictionCandidate{
		hash:       hash,
		dir:        dir,
		lastAccess: lastAccess,
		sizeBytes:  dirSizeBytes(dir),
	})
}

// removeContent deletes one object's pages. The complete marker is removed
// first so concurrent completeness checks stop treating the content as present
// before its pages disappear; in-flight readers degrade to a normal cache
// miss.
func (cas *Store) removeContent(candidate evictionCandidate) error {
	if err := os.Remove(filepath.Join(candidate.dir, cacheCompleteMarkerName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if cas.memoryCacheEnabled && cas.cache != nil {
		cas.cache.Del(candidate.hash)
	}
	if err := os.RemoveAll(candidate.dir); err != nil {
		return err
	}

	cas.accessTouchMu.Lock()
	delete(cas.accessTouches, candidate.hash)
	cas.accessTouchMu.Unlock()
	return nil
}

func dirSizeBytes(dir string) int64 {
	var total int64
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if info, err := entry.Info(); err == nil && !info.IsDir() {
			total += info.Size()
		}
	}
	return total
}
