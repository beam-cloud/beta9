package worker

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/rs/zerolog/log"
)

const stubCodeReadyMarker = ".beta9-cache-ready"

type stubCodeEntry struct {
	path      string
	readyPath string
	lastUsed  time.Time
	sizeBytes int64
	temporary bool
}

func (m *WorkerCacheManager) pruneOwnerStubCodeCache(server *cache.Server) {
	if m == nil || server == nil {
		return
	}
	root := stubCodeCacheRoot(m.config, m.poolConfig)
	if root == "" {
		return
	}
	pruned, freed := pruneStubCodeCache(root, m.recentStubTTL())
	if pruned > 0 {
		log.Info().Int("pruned", pruned).Int64("freed_bytes", freed).Str("root", root).Msg("pruned stale stub-code cache entries")
	}

	usage, err := fastDiskUsage(root)
	if err != nil {
		return
	}
	watermark := cacheDefaultDiskMaxUsage
	targetFree := int64(0)
	if usage.usagePct > 0.85 {
		targetUsed := int64(0.85 * float64(usage.totalBytes))
		targetFree = int64(usage.usedBytes) - targetUsed
	}
	if m.config.Cache.Disk.MinFreeBytes > 0 {
		if deficit := m.config.Cache.Disk.MinFreeBytes - int64(usage.availableBytes); deficit > targetFree {
			targetFree = deficit
		}
	}
	if usage.usagePct < watermark && targetFree <= 0 {
		return
	}
	evicted, pressureFreed := pressureEvictStubCodeCache(root, targetFree)
	if evicted > 0 {
		log.Warn().
			Int("evicted", evicted).
			Int64("freed_bytes", pressureFreed).
			Float64("disk_usage_pct", usage.usagePct).
			Str("root", root).
			Msg("pressure-evicted stub-code cache entries")
	}
}

func pruneStubCodeCache(root string, ttl time.Duration) (int, int64) {
	if ttl <= 0 {
		return 0, 0
	}
	entries := listStubCodeEntries(root)
	cutoff := time.Now().Add(-ttl)
	pruned := 0
	var freed int64
	for _, entry := range entries {
		if !entry.temporary && entry.lastUsed.After(cutoff) {
			continue
		}
		if err := os.RemoveAll(entry.path); err != nil {
			log.Debug().Err(err).Str("path", entry.path).Msg("failed to prune stub-code cache entry")
			continue
		}
		pruned++
		freed += entry.sizeBytes
	}
	return pruned, freed
}

func pressureEvictStubCodeCache(root string, bytesToFree int64) (int, int64) {
	entries := listStubCodeEntries(root)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastUsed.Before(entries[j].lastUsed)
	})
	evicted := 0
	var freed int64
	for _, entry := range entries {
		if bytesToFree > 0 && freed >= bytesToFree {
			break
		}
		if err := os.RemoveAll(entry.path); err != nil {
			log.Debug().Err(err).Str("path", entry.path).Msg("failed to pressure-evict stub-code cache entry")
			continue
		}
		evicted++
		freed += entry.sizeBytes
	}
	return evicted, freed
}

func listStubCodeEntries(root string) []stubCodeEntry {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	out := make([]stubCodeEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}
		item := stubCodeEntry{
			path:      path,
			lastUsed:  info.ModTime(),
			sizeBytes: dirSizeBytesRecursive(path),
		}
		if strings.Contains(entry.Name(), ".tmp.") {
			item.temporary = true
			out = append(out, item)
			continue
		}
		item.readyPath = filepath.Join(path, stubCodeReadyMarker)
		if readyInfo, err := os.Stat(item.readyPath); err == nil {
			item.lastUsed = readyInfo.ModTime()
			out = append(out, item)
		}
	}
	return out
}

func dirSizeBytesRecursive(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		if info, err := entry.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

type diskUsage struct {
	totalBytes     uint64
	availableBytes uint64
	usedBytes      uint64
	usagePct       float64
}

func fastDiskUsage(path string) (diskUsage, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return diskUsage{}, err
	}
	totalBytes := uint64(stat.Blocks) * uint64(stat.Bsize)
	availableBytes := uint64(stat.Bavail) * uint64(stat.Bsize)
	usedBytes := totalBytes - availableBytes
	usagePct := 0.0
	if totalBytes > 0 {
		usagePct = float64(usedBytes) / float64(totalBytes)
	}
	return diskUsage{
		totalBytes:     totalBytes,
		availableBytes: availableBytes,
		usedBytes:      usedBytes,
		usagePct:       usagePct,
	}, nil
}
