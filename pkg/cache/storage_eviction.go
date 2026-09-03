package cache

// LRU disk eviction over an in-memory content index.
//
// The index maps every content hash on disk to its size, completeness and last
// access. It is built by one directory walk at startup, maintained on every
// completion, read and removal, and re-walked on a slow timer to absorb drift
// (a page deleted by hand, an abandoned temp dir). Every completeness check and
// every eviction pass then answers from memory: before the index, each check
// read a marker file and each pass stat'd every object on disk several times,
// which on a node with tens of thousands of objects was the reconciler's
// dominant CPU cost.
//
// Content recency also persists as the complete marker's mtime, refreshed on
// read (throttled), so it survives a restart. When filesystem usage crosses the
// eviction watermark, the store first deletes unprotected stale content. If the
// node remains under pressure, it can evict newer unprotected content, and only
// uses protected content to clear the hard write gate.

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
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
	// contentIndexRescanInterval bounds how long the index can disagree with
	// the disk about content this process did not write or delete itself.
	contentIndexRescanInterval = 30 * time.Minute
)

type evictionCandidate struct {
	hash       string
	dir        string
	lastAccess time.Time
	sizeBytes  int64
}

// contentEntry is one object as the index knows it. complete mirrors the
// on-disk marker; an incomplete entry is an in-flight or abandoned write.
type contentEntry struct {
	dir        string
	size       int64
	pageSize   int64
	pageCount  int64
	complete   bool
	lastAccess time.Time
	// completedAt is when this process wrote the complete marker; zero for
	// entries learned from a disk walk.
	completedAt time.Time
}

type contentIndex struct {
	mu      sync.RWMutex
	entries map[string]contentEntry
}

func (idx *contentIndex) get(hash string) (contentEntry, bool) {
	idx.mu.RLock()
	entry, ok := idx.entries[hash]
	idx.mu.RUnlock()
	return entry, ok
}

func (idx *contentIndex) put(hash string, entry contentEntry) {
	idx.mu.Lock()
	idx.entries[hash] = entry
	idx.mu.Unlock()
}

func (idx *contentIndex) forget(hash string) {
	idx.mu.Lock()
	delete(idx.entries, hash)
	idx.mu.Unlock()
}

// putIfAbsent records entry unless the hash is already indexed, so a retry
// record never displaces a writer that completed the same hash meanwhile.
func (idx *contentIndex) putIfAbsent(hash string, entry contentEntry) {
	idx.mu.Lock()
	if _, ok := idx.entries[hash]; !ok {
		idx.entries[hash] = entry
	}
	idx.mu.Unlock()
}

// touch advances an entry's access time; it reports whether the entry exists
// and whether the previous touch is older than interval, so the caller can
// throttle the on-disk mtime refresh that persists recency across restarts.
func (idx *contentIndex) touch(hash string, now time.Time, interval time.Duration) (known bool, persist bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	entry, ok := idx.entries[hash]
	if !ok {
		return false, false
	}
	persist = now.Sub(entry.lastAccess) >= interval
	entry.lastAccess = now
	idx.entries[hash] = entry
	return true, persist
}

// candidates snapshots the index as eviction candidates, skipping in-flight
// writes that are still inside their grace period.
func (idx *contentIndex) candidates(now time.Time) []evictionCandidate {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	out := make([]evictionCandidate, 0, len(idx.entries))
	for hash, entry := range idx.entries {
		if !entry.complete && now.Sub(entry.lastAccess) < evictionIncompleteContentGrace {
			continue
		}
		out = append(out, evictionCandidate{hash: hash, dir: entry.dir, lastAccess: entry.lastAccess, sizeBytes: entry.size})
	}
	return out
}

// replace swaps in a walk of the disk that began at since. The walk is not
// atomic, so the index it replaces wins where it knows more: a read since the
// walk started is more recent than any mtime the walk observed, and a
// completion written since then is authoritative over a directory the walk
// saw half-written or not at all. Anything else the walk disagrees with is
// drift on disk, and the walk wins.
func (idx *contentIndex) replace(scanned map[string]contentEntry, since time.Time) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for hash, current := range idx.entries {
		entry, ok := scanned[hash]
		completedDuringWalk := current.complete && !current.completedAt.Before(since)
		switch {
		case completedDuringWalk && (!ok || !entry.complete):
			scanned[hash] = current
		case ok && current.lastAccess.After(entry.lastAccess):
			entry.lastAccess = current.lastAccess
			scanned[hash] = entry
		}
	}
	idx.entries = scanned
}

// rebuildContentIndex walks the cache directory once and replaces the index
// with what it finds. It runs at startup and on the rescan timer.
func (cas *Store) rebuildContentIndex() {
	started := time.Now()
	scanned := cas.scanContent()
	cas.index.replace(scanned, started)
	Logger.Infof("disk cache content index rebuilt: %d objects in %s", len(scanned), time.Since(started).Truncate(time.Millisecond))
}

// scanContent lists all on-disk content, covering both the v2
// (pages/<bucket>/<hash>) and legacy (<hash>) layouts.
func (cas *Store) scanContent() map[string]contentEntry {
	scanned := map[string]contentEntry{}
	buckets, _ := os.ReadDir(filepath.Join(cas.diskCacheDir, "pages"))
	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		bucketDir := filepath.Join(cas.diskCacheDir, "pages", bucket.Name())
		entries, _ := os.ReadDir(bucketDir)
		for _, entry := range entries {
			if entry.IsDir() && isContentHash(entry.Name()) {
				cas.scanContentDir(scanned, entry.Name(), filepath.Join(bucketDir, entry.Name()))
			}
		}
	}

	entries, _ := os.ReadDir(cas.diskCacheDir)
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != "pages" && isContentHash(entry.Name()) {
			cas.scanContentDir(scanned, entry.Name(), filepath.Join(cas.diskCacheDir, entry.Name()))
		}
	}
	return scanned
}

func (cas *Store) scanContentDir(scanned map[string]contentEntry, hash, dir string) {
	info, err := os.Stat(dir)
	if err != nil {
		return
	}
	entry := contentEntry{dir: dir, lastAccess: info.ModTime()}
	if filepath.Clean(dir) == filepath.Clean(cas.pageDir(hash)) {
		if size, pageSize, pageCount, ok := cas.completeMarker(hash); ok {
			entry.size, entry.pageSize, entry.pageCount, entry.complete = size, pageSize, pageCount, true
			// The marker mtime is refreshed on read and reflects content
			// recency rather than page churn.
			if markerInfo, err := os.Stat(filepath.Join(dir, cacheCompleteMarkerName)); err == nil {
				entry.lastAccess = markerInfo.ModTime()
			}
		}
	}
	if !entry.complete {
		entry.size = dirSizeBytes(dir)
	}
	scanned[hash] = entry
}

// indexCompleteContent records a freshly written complete marker.
func (cas *Store) indexCompleteContent(hash string, size, pageCount int64) {
	now := time.Now()
	cas.index.put(hash, contentEntry{
		dir:         cas.pageDir(hash),
		size:        size,
		pageSize:    cas.serverConfig.PageSizeBytes,
		pageCount:   pageCount,
		complete:    true,
		lastAccess:  now,
		completedAt: now,
	})
}

func (cas *Store) evictWatermarkPct() float64 {
	pct := normalizedPct(cas.serverConfig.DiskCacheEvictWatermarkPct)
	if pct <= 0 || pct > 1 {
		pct = defaultDiskCacheEvictWatermarkPct
	}
	return pct
}

// touchContentAccess records a successful read of hash so eviction prefers
// newer content. The index is updated on every read; the marker mtime that
// carries recency across restarts is refreshed at most once per interval.
func (cas *Store) touchContentAccess(hash string) {
	if hash == "" {
		return
	}
	now := time.Now()
	if known, persist := cas.index.touch(hash, now, evictionAccessTouchInterval); known && persist {
		_ = os.Chtimes(cas.completeMarkerPath(hash), now, now)
	}
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

// evictionCandidates lists all indexed content with its last access time and
// size.
func (cas *Store) evictionCandidates() []evictionCandidate {
	return cas.index.candidates(time.Now())
}

// removeContent deletes one object's pages. The index entry and the complete
// marker go first so concurrent completeness checks stop treating the content
// as present before its pages disappear; in-flight readers degrade to a normal
// cache miss. Whatever a failed removal leaves behind is re-indexed as an
// abandoned write so the next pass retries it rather than leaking it until
// the rescan.
func (cas *Store) removeContent(candidate evictionCandidate) error {
	cas.index.forget(candidate.hash)
	if err := os.Remove(filepath.Join(candidate.dir, cacheCompleteMarkerName)); err != nil && !os.IsNotExist(err) {
		cas.retainForRetry(candidate, candidate.sizeBytes)
		return err
	}
	if cas.memoryCacheEnabled && cas.cache != nil {
		cas.cache.Del(candidate.hash)
	}
	if err := os.RemoveAll(candidate.dir); err != nil {
		cas.retainForRetry(candidate, dirSizeBytes(candidate.dir))
		return err
	}
	return nil
}

// retainForRetry puts a failed removal back in the index as an abandoned
// write of the given size, keeping its old access time so it is eligible
// again immediately. A writer that completed the same hash meanwhile owns
// the entry and is left alone.
func (cas *Store) retainForRetry(candidate evictionCandidate, size int64) {
	cas.index.putIfAbsent(candidate.hash, contentEntry{dir: candidate.dir, size: size, lastAccess: candidate.lastAccess})
}

func dirSizeBytes(dir string) int64 {
	directory, err := os.Open(dir)
	if err != nil {
		return 0
	}
	defer directory.Close()

	var total int64
	for {
		entries, readErr := directory.ReadDir(256)
		for _, entry := range entries {
			if info, err := entry.Info(); err == nil && !info.IsDir() {
				total += info.Size()
			}
		}
		if readErr != nil {
			if readErr != io.EOF {
				return total
			}
			break
		}
	}
	return total
}
