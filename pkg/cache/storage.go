package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
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
	pageLockStripeCount         = 4096
)

type Store struct {
	ctx                     context.Context
	currentHost             *Host
	locality                string
	cache                   *ristretto.Cache[string, interface{}]
	serverConfig            ServerConfig
	globalConfig            GlobalConfig
	prefetchConfig          ReadPrefetchConfig
	metadataStore           CacheMetadataStore
	maxCacheSizeMb          int64
	diskCacheDir            string
	diskCachedUsageExceeded bool
	memoryCacheEnabled      bool
	mu                      sync.Mutex
	metrics                 CacheMetrics
	bufferPool              *BufferPool
	prefetcher              *Prefetcher
	pageLocks               [pageLockStripeCount]sync.RWMutex
	closing                 atomic.Bool
}

func NewStore(ctx context.Context, currentHost *Host, locality string, metadataStore CacheMetadataStore, config Config) (*Store, error) {
	if config.Server.PageSizeBytes <= 0 {
		return nil, errors.New("invalid cache configuration")
	}
	cas := &Store{
		ctx:                ctx,
		serverConfig:       config.Server,
		globalConfig:       config.Global,
		prefetchConfig:     config.Client.Prefetch,
		metadataStore:      metadataStore,
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
	if config.Client.Prefetch.Enabled {
		cas.prefetcher = NewPrefetcher(ctx, cas, cas.bufferPool)
	}

	return cas, nil
}

type cacheValue struct {
	Hash    string
	Content []byte
}

func (cas *Store) pageFileBuckets() int {
	if cas.serverConfig.PageFileBuckets <= 0 {
		return 1024
	}
	return cas.serverConfig.PageFileBuckets
}

func (cas *Store) pageLock(hash string, pageIdx int64) *sync.RWMutex {
	h := fnv.New64a()
	_, _ = h.Write([]byte(hash))
	var b [8]byte
	for i := uint(0); i < 8; i++ {
		b[i] = byte(uint64(pageIdx) >> (i * 8))
	}
	_, _ = h.Write(b[:])
	return &cas.pageLocks[h.Sum64()%pageLockStripeCount]
}

func (cas *Store) pageDir(hash string) string {
	bucket := "00"
	if len(hash) >= 2 {
		bucket = hash[:2]
	}
	return filepath.Join(cas.diskCacheDir, "pages", bucket, hash)
}

func (cas *Store) legacyPageDir(hash string) string {
	return filepath.Join(cas.diskCacheDir, hash)
}

func (cas *Store) pageKey(hash string, pageIdx int64) string {
	return fmt.Sprintf("%s-%d", hash, pageIdx)
}

func (cas *Store) pagePath(hash string, pageIdx int64) string {
	return filepath.Join(cas.pageDir(hash), cas.pageKey(hash, pageIdx))
}

func (cas *Store) legacyPagePath(hash string, pageIdx int64) string {
	return filepath.Join(cas.legacyPageDir(hash), cas.pageKey(hash, pageIdx))
}

func (cas *Store) existingPagePath(hash string, pageIdx int64) (string, os.FileInfo, error) {
	v2 := cas.pagePath(hash, pageIdx)
	if info, err := os.Stat(v2); err == nil {
		return v2, info, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", nil, err
	}

	legacy := cas.legacyPagePath(hash, pageIdx)
	if info, err := os.Stat(legacy); err == nil {
		return legacy, info, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", nil, err
	}

	return "", nil, ErrContentNotFound
}

func (cas *Store) Add(ctx context.Context, hash string, content []byte) error {
	size := int64(len(content))
	chunkKeys := []string{}

	if cas.globalConfig.DebugMode {
		Logger.Debugf("Cost added before Add: %+v", cas.cache.Metrics.CostAdded())
	}

	dirPath := cas.pageDir(hash)
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
		chunkKey := cas.pageKey(hash, chunkIdx)

		// Write through to disk cache if we still have storage available
		if !cas.diskCachedUsageExceeded {
			filePath := filepath.Join(dirPath, chunkKey)
			pageLock := cas.pageLock(hash, chunkIdx)
			pageLock.Lock()
			if err := writeCacheChunkAtomic(filePath, chunk); err != nil {
				pageLock.Unlock()
				return fmt.Errorf("failed to write to disk cache: %w", err)
			}
			pageLock.Unlock()
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
	dirPath := cas.pageDir(hash)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return "", size, fmt.Errorf("failed to create cache directory: %w", err)
	}

	chunkKeys := make([]string, 0, chunkCount)
	for chunkIdx := int64(0); chunkIdx < chunkCount; chunkIdx++ {
		chunkKey := cas.pageKey(hash, chunkIdx)
		chunkKeys = append(chunkKeys, chunkKey)

		tempChunkPath := filepath.Join(tempDir, fmt.Sprintf("chunk-%d", chunkIdx))
		filePath := filepath.Join(dirPath, chunkKey)
		pageLock := cas.pageLock(hash, chunkIdx)
		pageLock.Lock()
		if err := linkCacheChunkAtomic(tempChunkPath, filePath); err != nil {
			pageLock.Unlock()
			return "", size, fmt.Errorf("failed to install cache chunk: %w", err)
		}
		pageLock.Unlock()
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

func (cas *Store) PutFullPages(hash string, offset int64, data []byte) {
	if hash == "" || offset < 0 || len(data) == 0 || cas.serverConfig.PageSizeBytes <= 0 || cas.diskCachedUsageExceeded {
		return
	}

	pageSize := cas.serverConfig.PageSizeBytes
	if offset%pageSize != 0 {
		return
	}
	fullPages := int64(len(data)) / pageSize
	if fullPages <= 0 {
		return
	}
	if err := os.MkdirAll(cas.pageDir(hash), 0755); err != nil {
		Logger.Warnf("cache local promotion mkdir failed: hash=%s err=%v", hash, err)
		return
	}

	for page := int64(0); page < fullPages; page++ {
		start := page * pageSize
		end := start + pageSize
		pageIdx := (offset / pageSize) + page
		pagePath := cas.pagePath(hash, pageIdx)
		pageLock := cas.pageLock(hash, pageIdx)
		pageLock.Lock()
		err := writeCacheChunkAtomic(pagePath, data[start:end])
		pageLock.Unlock()
		if err != nil {
			Logger.Warnf("cache local promotion write failed: hash=%s page=%d err=%v", hash, pageIdx, err)
			return
		}
	}
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
		chunkKey := cas.pageKey(hash, int64(chunkIdx))
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

	for _, dir := range []string{cas.pageDir(hash), cas.legacyPageDir(hash)} {
		info, err := os.Stat(dir)
		if err == nil && info.IsDir() {
			return true
		}
	}

	return false
}

func (cas *Store) Get(hash string, offset, length int64, dst []byte) (int64, error) {
	if length < 0 {
		return 0, fmt.Errorf("invalid read length: %d", length)
	}
	return cas.ReadAt(hash, offset, dst[:minInt64ToInt(length, int64(len(dst)))])
}

func minInt64ToInt(a int64, b int64) int {
	if a < b {
		return int(a)
	}
	return int(b)
}

func (cas *Store) ReadAt(hash string, offset int64, dst []byte) (read int64, err error) {
	atomic.AddInt64(&cachePathStats.storeReadAtRequests, 1)
	atomic.AddInt64(&cachePathStats.storeReadAtBytes, int64(len(dst)))
	if offset < 0 {
		return 0, fmt.Errorf("invalid read offset: %d", offset)
	}
	if cas.serverConfig.PageSizeBytes <= 0 {
		return 0, errors.New("invalid page size")
	}
	length := int64(len(dst))
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
		if elapsed := time.Since(start); elapsed > time.Second || (err != nil && !errors.Is(err, ErrContentNotFound)) {
			Logger.Warnf("cache store read-at result: hash=%s offset=%d length=%d read=%d err=%v elapsed=%s", hash, offset, len(dst), read, err, elapsed.Truncate(time.Millisecond))
		}
		// Update hit ratios
		cas.updateHitRatios()
	}()

	// Notify prefetcher about this read
	if cas.prefetcher != nil {
		cas.prefetcher.OnRead(hash, offset, length)
	}

	if cas.memoryCacheEnabled {
		cas.cache.ResetTTL(hash, time.Duration(cas.serverConfig.ObjectTtlS)*time.Second)
	}

	for remainingLength > 0 {
		chunkIdx := o / cas.serverConfig.PageSizeBytes
		chunkKey := cas.pageKey(hash, chunkIdx)
		pageOffset := o % cas.serverConfig.PageSizeBytes

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
			readLength, err := cas.readPageFromDisk(hash, chunkIdx, pageOffset, remainingLength, dst[dstOffset:])
			if err != nil {
				cas.metrics.L2Misses.Inc()
				atomic.AddInt64(&cachePathStats.storeMisses, 1)
				return 0, ErrContentNotFound
			}
			cas.metrics.L1Hits.Inc()
			cas.metrics.L1BytesServed.Add(int(readLength))

			remainingLength -= readLength
			o += readLength
			dstOffset += readLength
			continue
		}

		v, ok := value.(cacheValue)
		if !ok {
			return 0, fmt.Errorf("unexpected cache value type")
		}

		chunkBytes := v.Content
		start := o % cas.serverConfig.PageSizeBytes
		chunkRemaining := int64(len(chunkBytes)) - start
		if chunkRemaining <= 0 {
			atomic.AddInt64(&cachePathStats.storeMisses, 1)
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
			atomic.AddInt64(&cachePathStats.storeMemoryPages, 1)
			atomic.AddInt64(&cachePathStats.storeMemoryBytes, readLength)
		} else {
			cas.metrics.L1BytesServed.Add(int(readLength))
		}

		remainingLength -= readLength
		o += readLength
		dstOffset += readLength
	}

	return dstOffset, nil
}

func (cas *Store) readPageFromDisk(hash string, chunkIdx int64, pageOffset int64, maxLength int64, dst []byte) (int64, error) {
	started := time.Now()
	pageLock := cas.pageLock(hash, chunkIdx)
	lockStart := time.Now()
	pageLock.RLock()
	cachePageLockWaitMs.Update(float64(time.Since(lockStart).Milliseconds()))
	defer pageLock.RUnlock()

	if cas.memoryCacheEnabled {
		chunkKey := cas.pageKey(hash, chunkIdx)
		rawValue, found := cas.cache.Get(chunkKey)
		if found {
			v, ok := rawValue.(cacheValue)
			if !ok {
				return 0, fmt.Errorf("unexpected cache value type")
			}
			if pageOffset >= int64(len(v.Content)) {
				return 0, ErrContentNotFound
			}
			n := min(maxLength, min(int64(len(v.Content))-pageOffset, int64(len(dst))))
			copy(dst[:n], v.Content[pageOffset:pageOffset+n])
			atomic.AddInt64(&cachePathStats.storeMemoryPages, 1)
			atomic.AddInt64(&cachePathStats.storeMemoryBytes, n)
			return n, nil
		}
	}

	chunkPath, info, err := cas.existingPagePath(hash, chunkIdx)
	if err != nil {
		return 0, ErrContentNotFound
	}
	if pageOffset >= info.Size() {
		return 0, ErrContentNotFound
	}
	readLength := min(maxLength, min(info.Size()-pageOffset, int64(len(dst))))

	// Use fadvise to hint sequential/random access patterns
	file, err := os.Open(chunkPath)
	if err != nil {
		return 0, ErrContentNotFound
	}
	defer file.Close()

	// Hint sequential access for better readahead
	if err := fadviseSequential(file.Fd()); err == nil {
		Logger.Debugf("Set FADV_SEQUENTIAL for %s", chunkPath)
	}

	// Hint willneed for prefetch
	if err := fadviseWillneed(file.Fd(), pageOffset, readLength); err == nil {
		Logger.Debugf("Set FADV_WILLNEED for %s", chunkPath)
	}

	readStart := time.Now()
	n, err := file.ReadAt(dst[:readLength], pageOffset)
	cachePageReadLatencyMs.Update(float64(time.Since(readStart).Milliseconds()))
	if err != nil && !errors.Is(err, io.EOF) {
		atomic.AddInt64(&cachePathStats.storeMisses, 1)
		return int64(n), ErrContentNotFound
	}
	if n == 0 {
		atomic.AddInt64(&cachePathStats.storeMisses, 1)
		return 0, ErrContentNotFound
	}
	atomic.AddInt64(&cachePathStats.storeDiskPages, 1)
	atomic.AddInt64(&cachePathStats.storeDiskBytes, int64(n))
	cas.promoteDiskPageToMemory(hash, chunkIdx, file, info.Size(), pageOffset, dst[:n])
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		Logger.Warnf("cache store disk page read slow: hash=%s page=%d page_offset=%d max_length=%d read=%d elapsed=%s", hash, chunkIdx, pageOffset, maxLength, n, elapsed.Truncate(time.Millisecond))
	}
	return int64(n), nil
}

func (cas *Store) promoteDiskPageToMemory(hash string, chunkIdx int64, file *os.File, pageSize int64, readOffset int64, readBytes []byte) {
	if !cas.memoryCacheEnabled || pageSize <= 0 || pageSize > int64(int(^uint(0)>>1)) {
		return
	}

	chunkKey := cas.pageKey(hash, chunkIdx)
	if _, found := cas.cache.Get(chunkKey); found {
		return
	}

	var page []byte
	if readOffset == 0 && int64(len(readBytes)) == pageSize {
		page = append([]byte(nil), readBytes...)
	} else {
		page = make([]byte, int(pageSize))
		n, err := file.ReadAt(page, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			return
		}
		if n <= 0 {
			return
		}
		page = page[:n]
	}

	cas.cache.Set(chunkKey, cacheValue{Hash: hash, Content: page}, int64(len(page)))
}

func (cas *Store) PageRegion(hash string, offset int64, length int64) (path string, pageOffset int64, n int, ok bool, err error) {
	started := time.Now()
	defer func() {
		if elapsed := time.Since(started); elapsed > 100*time.Millisecond || (err != nil && !errors.Is(err, ErrContentNotFound)) {
			Logger.Debugf("cache store page-region result: hash=%s offset=%d length=%d path=%s page_offset=%d n=%d ok=%t err=%v elapsed=%s", hash, offset, length, path, pageOffset, n, ok, err, elapsed.Truncate(time.Millisecond))
		}
	}()
	atomic.AddInt64(&cachePathStats.storePageRegions, 1)
	if offset < 0 {
		atomic.AddInt64(&cachePathStats.storePageRegionMiss, 1)
		return "", 0, 0, false, fmt.Errorf("invalid read offset: %d", offset)
	}
	if length <= 0 || cas.serverConfig.PageSizeBytes <= 0 {
		atomic.AddInt64(&cachePathStats.storePageRegionMiss, 1)
		return "", 0, 0, false, nil
	}
	pageIdx := offset / cas.serverConfig.PageSizeBytes
	pageOffset = offset % cas.serverConfig.PageSizeBytes
	if pageOffset+length > cas.serverConfig.PageSizeBytes {
		atomic.AddInt64(&cachePathStats.storePageRegionMiss, 1)
		return "", 0, 0, false, nil
	}

	pageLock := cas.pageLock(hash, pageIdx)
	pageLock.RLock()
	defer pageLock.RUnlock()

	pagePath, info, err := cas.existingPagePath(hash, pageIdx)
	if err != nil {
		atomic.AddInt64(&cachePathStats.storePageRegionMiss, 1)
		return "", 0, 0, false, err
	}
	if pageOffset >= info.Size() {
		atomic.AddInt64(&cachePathStats.storePageRegionMiss, 1)
		return "", 0, 0, false, ErrContentNotFound
	}
	readLength := min(length, info.Size()-pageOffset)
	if readLength <= 0 {
		atomic.AddInt64(&cachePathStats.storePageRegionMiss, 1)
		return "", 0, 0, false, nil
	}
	atomic.AddInt64(&cachePathStats.storePageRegionHits, 1)
	atomic.AddInt64(&cachePathStats.storePageRegionBytes, readLength)
	return pagePath, pageOffset, int(readLength), true, nil
}

func (cas *Store) WarmRange(hash string, offset int64, length int64) {
	if length <= 0 || offset < 0 || cas.serverConfig.PageSizeBytes <= 0 {
		return
	}

	pageSize := cas.serverConfig.PageSizeBytes
	remaining := length
	currentOffset := offset
	for remaining > 0 {
		pageIdx := currentOffset / pageSize
		pageOffset := currentOffset % pageSize
		readLength := min(remaining, pageSize-pageOffset)

		pageLock := cas.pageLock(hash, pageIdx)
		pageLock.RLock()
		chunkPath, info, err := cas.existingPagePath(hash, pageIdx)
		if err == nil && pageOffset < info.Size() {
			if readLength > info.Size()-pageOffset {
				readLength = info.Size() - pageOffset
			}
			if file, err := os.Open(chunkPath); err == nil {
				_ = fadviseWillneed(file.Fd(), pageOffset, readLength)
				_ = file.Close()
				cachePrefetchPagesTotal.Inc()
			}
		}
		pageLock.RUnlock()

		if readLength <= 0 {
			return
		}
		remaining -= readLength
		currentOffset += readLength
	}
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

	Logger.Debugf("Evicted object from memory cache: %s", hash)
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

			Logger.Debugf("Memory Cache Usage: %dMB / %dMB (%.2f%%)", availableMemoryMb, totalMemoryMb, float64(availableMemoryMb)/float64(totalMemoryMb)*100)
			Logger.Debugf("Disk Cache Usage: %dMB / %dMB (%.2f%%)", currentUsage, totalDiskSpace, usagePercentage*100)

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
