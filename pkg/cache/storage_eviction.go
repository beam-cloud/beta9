package cache

// LRU disk eviction. Content recency is the complete marker file's
// mtime, refreshed on read (throttled), so it survives restarts with no extra
// index. When filesystem usage crosses the eviction watermark, the
// least-recently-read content is deleted until enough space is reclaimed.
// Content read very recently is never evicted.

import (
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	// defaultDiskCacheEvictWatermarkPct keeps eviction comfortably below
	// kubelet DiskPressure thresholds (typically ~0.85-0.90).
	defaultDiskCacheEvictWatermarkPct = 0.85
	// evictionAccessTouchInterval throttles per-hash marker mtime updates on
	// the read path.
	evictionAccessTouchInterval = 5 * time.Minute
	// evictionRecentAccessGuard prevents evicting content that was read this
	// recently, regardless of space pressure.
	evictionRecentAccessGuard = 10 * time.Minute
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
	pct := cas.serverConfig.DiskCacheEvictWatermarkPct
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
// is above the eviction watermark. It reports whether anything was evicted.
func (cas *Store) maybeEvictDiskCache(usagePct float64, totalDiskMb int64) bool {
	watermark := cas.evictWatermarkPct()
	if usagePct <= watermark || totalDiskMb <= 0 {
		return false
	}

	bytesToFree := int64((usagePct - watermark) * float64(totalDiskMb) * 1024 * 1024)
	started := time.Now()
	evicted, freed := cas.evictLRU(bytesToFree)
	if evicted == 0 {
		Logger.Warnf("disk cache eviction found nothing evictable: usage=%.2f watermark=%.2f want_free=%d bytes", usagePct, watermark, bytesToFree)
		return false
	}

	Logger.Infof("disk cache eviction freed %d bytes across %d objects in %s (usage=%.2f watermark=%.2f)", freed, evicted, time.Since(started).Truncate(time.Millisecond), usagePct, watermark)
	return true
}

// evictLRU deletes least-recently-read content until roughly bytesToFree bytes
// have been reclaimed. Returns the number of objects evicted and the bytes
// freed.
func (cas *Store) evictLRU(bytesToFree int64) (int, int64) {
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
		// Oldest-first order: once we reach recently-read content, nothing
		// after it is evictable either.
		if candidate.lastAccess.After(cutoff) {
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

// evictionCandidates lists all on-disk content with its last access time and
// size, covering both the v2 (pages/<bucket>/<hash>) and legacy (<hash>)
// layouts.
func (cas *Store) evictionCandidates() []evictionCandidate {
	candidates := []evictionCandidate{}

	buckets, _ := os.ReadDir(filepath.Join(cas.diskCacheDir, "pages"))
	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		bucketDir := filepath.Join(cas.diskCacheDir, "pages", bucket.Name())
		entries, _ := os.ReadDir(bucketDir)
		for _, entry := range entries {
			if entry.IsDir() {
				candidates = cas.appendCandidate(candidates, entry.Name(), filepath.Join(bucketDir, entry.Name()))
			}
		}
	}

	entries, _ := os.ReadDir(cas.diskCacheDir)
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != "pages" && len(entry.Name()) == 64 {
			candidates = cas.appendCandidate(candidates, entry.Name(), filepath.Join(cas.diskCacheDir, entry.Name()))
		}
	}

	return candidates
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
