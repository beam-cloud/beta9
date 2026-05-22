package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/beam-cloud/ristretto"
	"github.com/shirou/gopsutil/v4/mem"
)

const (
	diskCacheUsageCheckInterval = 1 * time.Minute
)

type Store struct {
	ctx                     context.Context
	currentHost             *Host
	locality                string
	cache                   *ristretto.Cache[string, interface{}]
	serverConfig            ServerConfig
	globalConfig            GlobalConfig
	coordinator             Registry
	maxCacheSizeMb          int64
	diskCacheDir            string
	diskCachedUsageExceeded bool
	memoryCacheEnabled      bool
	mu                      sync.Mutex
	metrics                 CacheMetrics
	bufferPool              *BufferPool
	prefetcher              *Prefetcher
	closing                 atomic.Bool
}

func NewStore(ctx context.Context, currentHost *Host, locality string, coordinator Registry, config Config) (*Store, error) {
	if config.Server.PageSizeBytes <= 0 {
		return nil, errors.New("invalid cache configuration")
	}

	cas := &Store{
		ctx:                ctx,
		serverConfig:       config.Server,
		globalConfig:       config.Global,
		coordinator:        coordinator,
		currentHost:        currentHost,
		locality:           locality,
		diskCacheDir:       config.Server.DiskCacheDir,
		memoryCacheEnabled: config.Server.MaxCachePct > 0,
		mu:                 sync.Mutex{},
		metrics:            initMetrics(ctx, config.Metrics, currentHost, locality),
	}

	Logger.Infof("Disk cache directory located at: '%s'", cas.diskCacheDir)

	if cas.memoryCacheEnabled {
		_, totalMemoryMb := getMemoryMb()
		maxCacheSizeMb := (totalMemoryMb * cas.serverConfig.MaxCachePct) / 100
		maxCost := maxCacheSizeMb * 1e6

		Logger.Infof("Memory cache ENABLED")
		Logger.Infof("Total available memory: %dMB", totalMemoryMb)
		Logger.Infof("Max cache size: %dMB", maxCacheSizeMb)
		Logger.Infof("Max cost: %d", maxCost)

		if maxCacheSizeMb <= 0 {
			return nil, errors.New("invalid memory limit")
		}

		cache, err := ristretto.NewCache(&ristretto.Config[string, interface{}]{
			NumCounters: 1e7,
			MaxCost:     maxCost,
			BufferItems: 64,
			OnEvict:     cas.onEvict,
			Metrics:     cas.globalConfig.DebugMode,
		})
		if err != nil {
			return nil, err
		}

		cas.cache = cache
		cas.maxCacheSizeMb = maxCacheSizeMb
	} else {
		Logger.Infof("Memory cache DISABLED (disk-only mode)")
		cache, _ := ristretto.NewCache(&ristretto.Config[string, interface{}]{
			NumCounters: 1,
			MaxCost:     1,
			BufferItems: 64,
			Metrics:     false,
		})
		cas.cache = cache
	}

	// Only start disk monitor if we have a metrics URL (not in benchmarks/tests)
	if config.Metrics.URL != "" {
		go cas.monitorDiskCacheUsage()
	}

	// Initialize buffer pool for reduced allocations
	cas.bufferPool = NewBufferPool()

	// Initialize prefetcher for sequential read optimization
	cas.prefetcher = NewPrefetcher(ctx, cas, cas.bufferPool)

	return cas, nil
}

type cacheValue struct {
	Hash    string
	Content []byte
}

func (cas *Store) Add(ctx context.Context, hash string, content []byte) error {
	size := int64(len(content))
	chunkKeys := []string{}

	if cas.globalConfig.DebugMode {
		Logger.Debugf("Cost added before Add: %+v", cas.cache.Metrics.CostAdded())
	}

	dirPath := filepath.Join(cas.diskCacheDir, hash)
	if !cas.diskCachedUsageExceeded {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return fmt.Errorf("failed to create cache directory: %w", err)
		}
	}

	// Break content into chunks and store
	for offset := int64(0); offset < size; offset += cas.serverConfig.PageSizeBytes {
		chunkIdx := offset / cas.serverConfig.PageSizeBytes
		end := offset + cas.serverConfig.PageSizeBytes
		if end > size {
			end = size
		}

		// Copy the chunk into a new buffer
		chunk := make([]byte, end-offset)
		copy(chunk, content[offset:end])
		chunkKey := fmt.Sprintf("%s-%d", hash, chunkIdx)

		// Write through to disk cache if we still have storage available
		if !cas.diskCachedUsageExceeded {
			filePath := filepath.Join(dirPath, chunkKey)
			if err := writeCacheChunkAtomic(filePath, chunk); err != nil {
				return fmt.Errorf("failed to write to disk cache: %w", err)
			}
		}

		chunkKeys = append(chunkKeys, chunkKey)

		if cas.memoryCacheEnabled {
			_, exists := cas.cache.GetTTL(chunkKey)
			if exists {
				continue
			}

			added := cas.cache.Set(chunkKey, cacheValue{Hash: hash, Content: chunk}, int64(len(chunk)))
			if !added {
				return errors.New("unable to cache: set dropped")
			}
		}
	}

	// Release the large initial buffer
	content = nil

	if cas.memoryCacheEnabled {
		chunks := strings.Join(chunkKeys, ",")
		added := cas.cache.SetWithTTL(hash, chunks, int64(len(chunks)), time.Duration(cas.serverConfig.ObjectTtlS)*time.Second)
		if !added {
			return errors.New("unable to cache: set dropped")
		}
	}

	Logger.Debugf("Added object: %s, size: %d bytes", hash, size)
	return nil
}

func (cas *Store) AddReader(ctx context.Context, reader io.Reader) (string, int64, error) {
	if reader == nil {
		return "", 0, errors.New("nil content reader")
	}
	if cas.serverConfig.PageSizeBytes <= 0 {
		return "", 0, errors.New("invalid page size")
	}
	if cas.diskCachedUsageExceeded {
		if !cas.memoryCacheEnabled {
			return "", 0, errors.New("disk cache capacity exceeded")
		}
		return cas.addReaderToMemory(ctx, reader)
	}

	if err := os.MkdirAll(cas.diskCacheDir, 0755); err != nil {
		return "", 0, fmt.Errorf("failed to create cache directory: %w", err)
	}

	tempDir, err := os.MkdirTemp(cas.diskCacheDir, ".store-*")
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temp cache directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	hasher := sha256.New()
	pageSize := int(cas.serverConfig.PageSizeBytes)
	buf := make([]byte, pageSize)
	var size int64
	var chunkCount int64

	for {
		if err := ctx.Err(); err != nil {
			return "", size, err
		}

		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if _, err := hasher.Write(chunk); err != nil {
				return "", size, err
			}

			tempChunkPath := filepath.Join(tempDir, fmt.Sprintf("chunk-%d", chunkCount))
			if err := writeCacheChunkAtomic(tempChunkPath, chunk); err != nil {
				return "", size, fmt.Errorf("failed to write temp cache chunk: %w", err)
			}

			size += int64(n)
			chunkCount++
		}

		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		return "", size, readErr
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	dirPath := filepath.Join(cas.diskCacheDir, hash)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return "", size, fmt.Errorf("failed to create cache directory: %w", err)
	}

	chunkKeys := make([]string, 0, chunkCount)
	for chunkIdx := int64(0); chunkIdx < chunkCount; chunkIdx++ {
		chunkKey := fmt.Sprintf("%s-%d", hash, chunkIdx)
		chunkKeys = append(chunkKeys, chunkKey)

		tempChunkPath := filepath.Join(tempDir, fmt.Sprintf("chunk-%d", chunkIdx))
		filePath := filepath.Join(dirPath, chunkKey)
		if err := linkCacheChunkAtomic(tempChunkPath, filePath); err != nil {
			return "", size, fmt.Errorf("failed to install cache chunk: %w", err)
		}
	}

	if cas.memoryCacheEnabled {
		for _, chunkKey := range chunkKeys {
			filePath := filepath.Join(dirPath, chunkKey)
			chunk, err := os.ReadFile(filePath)
			if err != nil {
				return "", size, fmt.Errorf("failed to read cache chunk for memory cache: %w", err)
			}

			added := cas.cache.Set(chunkKey, cacheValue{Hash: hash, Content: chunk}, int64(len(chunk)))
			if !added {
				return "", size, errors.New("unable to cache: set dropped")
			}
		}

		chunks := strings.Join(chunkKeys, ",")
		added := cas.cache.SetWithTTL(hash, chunks, int64(len(chunks)), time.Duration(cas.serverConfig.ObjectTtlS)*time.Second)
		if !added {
			return "", size, errors.New("unable to cache: set dropped")
		}
	}

	Logger.Debugf("Added object: %s, size: %d bytes", hash, size)
	return hash, size, nil
}

func (cas *Store) addReaderToMemory(ctx context.Context, reader io.Reader) (string, int64, error) {
	hasher := sha256.New()
	pageSize := int(cas.serverConfig.PageSizeBytes)
	buf := make([]byte, pageSize)
	chunks := make([][]byte, 0)
	var size int64

	for {
		if err := ctx.Err(); err != nil {
			return "", size, err
		}

		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if _, err := hasher.Write(chunk); err != nil {
				return "", size, err
			}

			chunks = append(chunks, chunk)
			size += int64(n)
		}

		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		return "", size, readErr
	}

	hash := hex.EncodeToString(hasher.Sum(nil))
	chunkKeys := make([]string, 0, len(chunks))
	for chunkIdx, chunk := range chunks {
		chunkKey := fmt.Sprintf("%s-%d", hash, chunkIdx)
		chunkKeys = append(chunkKeys, chunkKey)

		added := cas.cache.Set(chunkKey, cacheValue{Hash: hash, Content: chunk}, int64(len(chunk)))
		if !added {
			return "", size, errors.New("unable to cache: set dropped")
		}
	}

	chunksValue := strings.Join(chunkKeys, ",")
	added := cas.cache.SetWithTTL(hash, chunksValue, int64(len(chunksValue)), time.Duration(cas.serverConfig.ObjectTtlS)*time.Second)
	if !added {
		return "", size, errors.New("unable to cache: set dropped")
	}

	Logger.Debugf("Added object to memory cache: %s, size: %d bytes", hash, size)
	return hash, size, nil
}

func writeCacheChunkAtomic(filePath string, chunk []byte) error {
	if info, err := os.Stat(filePath); err == nil {
		if info.Size() == int64(len(chunk)) {
			return nil
		}
		if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(filePath), "."+filepath.Base(filePath)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if _, err := tmpFile.Write(chunk); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	if err := os.Link(tmpPath, filePath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return err
	}

	return nil
}

func linkCacheChunkAtomic(tmpPath, filePath string) error {
	if info, err := os.Stat(filePath); err == nil {
		tmpInfo, tmpErr := os.Stat(tmpPath)
		if tmpErr != nil {
			return tmpErr
		}
		if info.Size() == tmpInfo.Size() {
			return nil
		}
		if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err := os.Link(tmpPath, filePath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return err
	}

	return nil
}

func (cas *Store) Exists(hash string) bool {
	var exists bool = false

	if cas.memoryCacheEnabled {
		_, exists = cas.cache.GetTTL(hash)
		if exists {
			return true
		}
	}

	info, err := os.Stat(filepath.Join(cas.diskCacheDir, hash))
	if err != nil {
		return false
	}

	return info.IsDir()
}

func (cas *Store) Get(hash string, offset, length int64, dst []byte) (int64, error) {
	remainingLength := length
	o := offset
	dstOffset := int64(0)

	// Track metrics
	cas.metrics.TotalReads.Inc()
	start := time.Now()
	defer func() {
		if dstOffset > 0 {
			throughputMBps := (float64(dstOffset) / (1024 * 1024)) / (float64(time.Since(start).Microseconds()) / 1e6)
			cas.metrics.ReadThroughputMBps.Update(throughputMBps)
		}
		// Update hit ratios
		cas.updateHitRatios()
	}()

	// Notify prefetcher about this read
	cas.prefetcher.OnRead(hash, offset, length)

	if cas.memoryCacheEnabled {
		cas.cache.ResetTTL(hash, time.Duration(cas.serverConfig.ObjectTtlS)*time.Second)
	}

	for remainingLength > 0 {
		chunkIdx := o / cas.serverConfig.PageSizeBytes
		chunkKey := fmt.Sprintf("%s-%d", hash, chunkIdx)

		var value interface{}
		var found bool = false

		// Check in-memory cache for chunk (L0)
		fromMemory := false
		if cas.memoryCacheEnabled {
			value, found = cas.cache.Get(chunkKey)
			fromMemory = found
			if found {
				cas.metrics.L0Hits.Inc()
			}
		}

		// Not found in memory, check disk cache (L1) before giving up
		if !found {
			var err error
			value, found, err = cas.getFromDiskCache(hash, chunkKey)
			if err != nil || !found {
				cas.metrics.L2Misses.Inc()
				return 0, ErrContentNotFound
			}
			cas.metrics.L1Hits.Inc()
		}

		v, ok := value.(cacheValue)
		if !ok {
			return 0, fmt.Errorf("unexpected cache value type")
		}

		chunkBytes := v.Content
		start := o % cas.serverConfig.PageSizeBytes
		chunkRemaining := int64(len(chunkBytes)) - start
		if chunkRemaining <= 0 {
			return dstOffset, ErrContentNotFound
		}

		readLength := min(remainingLength, chunkRemaining)
		end := start + readLength

		if start < 0 || end <= start || end > int64(len(chunkBytes)) {
			return 0, fmt.Errorf("invalid chunk boundaries: start %d, end %d, chunk size %d", start, end, len(chunkBytes))
		}

		copy(dst[dstOffset:dstOffset+readLength], chunkBytes[start:end])

		// Track bytes served from appropriate tier
		if fromMemory {
			cas.metrics.L0BytesServed.Add(int(readLength))
		} else {
			cas.metrics.L1BytesServed.Add(int(readLength))
		}

		remainingLength -= readLength
		o += readLength
		dstOffset += readLength
	}

	return dstOffset, nil
}

func (cas *Store) getFromDiskCache(hash, chunkKey string) (value cacheValue, found bool, err error) {
	cas.mu.Lock()
	defer cas.mu.Unlock()

	if cas.memoryCacheEnabled {
		rawValue, found := cas.cache.Get(chunkKey)
		if found {
			return rawValue.(cacheValue), true, nil
		}
	}

	chunkPath := filepath.Join(cas.diskCacheDir, hash, chunkKey)

	// Use fadvise to hint sequential/random access patterns
	file, err := os.Open(chunkPath)
	if err != nil {
		return cacheValue{}, false, ErrContentNotFound
	}
	defer file.Close()

	// Hint sequential access for better readahead
	if err := fadviseSequential(file.Fd()); err == nil {
		Logger.Debugf("Set FADV_SEQUENTIAL for %s", chunkPath)
	}

	chunkBytes, err := os.ReadFile(chunkPath)
	if err != nil {
		return cacheValue{}, false, ErrContentNotFound
	}

	// Hint willneed for prefetch
	if err := fadviseWillneed(file.Fd(), 0, int64(len(chunkBytes))); err == nil {
		Logger.Debugf("Set FADV_WILLNEED for %s", chunkPath)
	}

	value = cacheValue{Hash: hash, Content: chunkBytes}
	if cas.memoryCacheEnabled {
		cas.cache.Set(chunkKey, value, int64(len(chunkBytes)))
	}

	return value, true, nil
}

func (cas *Store) onEvict(item *ristretto.Item[interface{}]) {
	if cas.closing.Load() {
		return
	}

	hash := ""
	var chunkKeys []string = []string{}

	// We've evicted a chunk of a cached object - extract the hash and evict all the other chunks
	switch v := item.Value.(type) {
	case cacheValue:
		hash = v.Hash
		chunks, found := cas.cache.Get(hash)
		if found {
			chunkKeys = strings.Split(chunks.(string), ",")
		}
	case string:
		// In this case, we evicted the key that stores which chunks are currently present in the cache
		// the value of which is formatted like this: "<hash>-0,<hash>-1,<hash>-2"
		// so here we can extract the hash by splitting on '-' and taking the first item
		hash = strings.SplitN(v, "-", 2)[0]
		chunkKeys = strings.Split(v, ",")
	default:
	}

	Logger.Infof("Evicted object from memory cache: %s", hash)
	Logger.Debugf("Object chunks: %+v", chunkKeys)

	for _, k := range chunkKeys {
		cas.cache.Del(k)
	}
}

func (cas *Store) Cleanup() {
	if cas.cache != nil {
		cas.closing.Store(true)
		cas.cache.Close()
	}
}

func (cas *Store) GetDiskCacheMetrics() (int64, int64, float64, error) {
	var (
		diskUsageMb      int64
		totalDiskSpaceMb int64
		usagePercentage  float64
		err              error
	)

	// Get current disk usage
	diskUsageMb, err = getDiskUsageMb(cas.diskCacheDir)
	if err != nil {
		return 0, 0, 0, err
	}

	// Get total disk capacity
	totalDiskSpaceMb, err = getTotalDiskSpaceMb(cas.diskCacheDir)
	if err != nil {
		return 0, 0, 0, err
	}

	// Calculate usage ratio.
	if totalDiskSpaceMb > 0 {
		usagePercentage = float64(diskUsageMb) / float64(totalDiskSpaceMb)
	} else {
		usagePercentage = 0
	}

	return diskUsageMb, totalDiskSpaceMb, usagePercentage, nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func getDiskUsageMb(path string) (int64, error) {
	var totalUsage int64 = 0
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalUsage += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return totalUsage / (1024 * 1024), nil
}

func getTotalDiskSpaceMb(path string) (int64, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		return 0, err
	}
	return int64(stat.Blocks) * int64(stat.Bsize) / (1024 * 1024), nil
}

func getMemoryMb() (int64, int64) {
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Fatalf("Unable to retrieve host memory info: %v", err)
	}
	return int64(v.Available / (1024 * 1024)), int64(v.Total / (1024 * 1024))
}

func (cas *Store) monitorDiskCacheUsage() {
	ticker := time.NewTicker(diskCacheUsageCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cas.ctx.Done():
			return
		case <-ticker.C:
			currentUsage, totalDiskSpace, usagePercentage, err := cas.GetDiskCacheMetrics()
			if err != nil {
				// Silently skip if directory doesn't exist (common in tests/benchmarks)
				if !os.IsNotExist(err) {
					Logger.Errorf("Failed to fetch disk cache metrics: %v", err)
				}
				continue
			}

			availableMemoryMb, totalMemoryMb := getMemoryMb()
			usedMemoryMb := totalMemoryMb - availableMemoryMb
			cas.metrics.MemCacheUsageMB.Update(float64(usedMemoryMb))
			cas.metrics.MemCacheUsagePct.Update(float64(usedMemoryMb) / float64(totalMemoryMb) * 100)
			cas.metrics.DiskCacheUsageMB.Update(float64(currentUsage))
			cas.metrics.DiskCacheUsagePct.Update(float64(usagePercentage))

			Logger.Infof("Memory Cache Usage: %dMB / %dMB (%.2f%%)", availableMemoryMb, totalMemoryMb, float64(availableMemoryMb)/float64(totalMemoryMb)*100)
			Logger.Infof("Disk Cache Usage: %dMB / %dMB (%.2f%%)", currentUsage, totalDiskSpace, usagePercentage*100)

			// Update internal state for disk usage exceeded
			cas.mu.Lock()
			cas.diskCachedUsageExceeded = usagePercentage > cas.serverConfig.DiskCacheMaxUsagePct
			cas.mu.Unlock()
		}
	}
}

// updateHitRatios calculates and updates cache hit ratio metrics
func (cas *Store) updateHitRatios() {
	l0Hits := cas.metrics.L0Hits.Get()
	l1Hits := cas.metrics.L1Hits.Get()
	l2Misses := cas.metrics.L2Misses.Get()
	total := l0Hits + l1Hits + l2Misses

	if total > 0 {
		l0Ratio := float64(l0Hits) / float64(total) * 100
		l1Ratio := float64(l1Hits) / float64(total) * 100
		l2Ratio := float64(l2Misses) / float64(total) * 100

		cas.metrics.L0HitRatio.Update(l0Ratio)
		cas.metrics.L1HitRatio.Update(l1Ratio)
		cas.metrics.L2MissRatio.Update(l2Ratio)
	}
}
