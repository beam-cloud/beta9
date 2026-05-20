package cache

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

type CacheMetrics struct {
	DiskCacheUsageMB  *metrics.Histogram
	DiskCacheUsagePct *metrics.Histogram
	MemCacheUsageMB   *metrics.Histogram
	MemCacheUsagePct  *metrics.Histogram

	// Cache tier hit ratios
	L0HitRatio  *metrics.Histogram // In-memory cache hits
	L1HitRatio  *metrics.Histogram // Disk cache hits
	L2MissRatio *metrics.Histogram // Remote fetch required

	// Operation counters
	L0Hits     *metrics.Counter
	L1Hits     *metrics.Counter
	L2Misses   *metrics.Counter
	TotalReads *metrics.Counter

	// Bytes served per tier
	L0BytesServed  *metrics.Counter
	L1BytesServed  *metrics.Counter
	L2BytesFetched *metrics.Counter

	// FUSE operation latencies
	FUSEReadLatency    *metrics.Histogram
	FUSELookupLatency  *metrics.Histogram
	FUSEGetattrLatency *metrics.Histogram

	// Read throughput
	ReadThroughputMBps *metrics.Histogram
}

var (
	globalMetrics   CacheMetrics
	metricsInitOnce sync.Once
)

func initMetrics(ctx context.Context, config MetricsConfig, currentHost *Host, locality string) CacheMetrics {
	metricsInitOnce.Do(func() {
		globalMetrics = createMetrics(ctx, config, currentHost, locality)
	})
	return globalMetrics
}

func createMetrics(ctx context.Context, config MetricsConfig, currentHost *Host, locality string) CacheMetrics {
	// Only initialize metrics push if URL is configured
	if config.URL != "" {
		username := config.Username
		password := config.Password
		credentials := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))

		opts := &metrics.PushOptions{
			Headers: []string{
				fmt.Sprintf("Authorization: Basic %s", credentials),
			},
			ExtraLabels: "host=\"" + currentHost.HostId + "\",locality=\"" + locality + "\"",
		}

		pushURL := config.URL
		interval := time.Duration(config.PushIntervalS) * time.Second
		pushProcessMetrics := true

		err := metrics.InitPushWithOptions(ctx, pushURL, interval, pushProcessMetrics, opts)
		if err != nil && Logger != nil {
			Logger.Errorf("Failed to initialize metrics: %v", err)
		}
	}

	diskCacheUsageMB := metrics.NewHistogram(`cache_disk_cache_usage_mb`)
	diskCacheUsagePct := metrics.NewHistogram(`cache_disk_cache_usage_pct`)
	memCacheUsageMB := metrics.NewHistogram(`cache_mem_cache_usage_mb`)
	memCacheUsagePct := metrics.NewHistogram(`cache_mem_cache_usage_pct`)

	// Cache tier metrics
	l0HitRatio := metrics.NewHistogram(`cache_l0_hit_ratio`)
	l1HitRatio := metrics.NewHistogram(`cache_l1_hit_ratio`)
	l2MissRatio := metrics.NewHistogram(`cache_l2_miss_ratio`)

	// Operation counters
	l0Hits := metrics.NewCounter(`cache_l0_hits_total`)
	l1Hits := metrics.NewCounter(`cache_l1_hits_total`)
	l2Misses := metrics.NewCounter(`cache_l2_misses_total`)
	totalReads := metrics.NewCounter(`cache_reads_total`)

	// Bytes served
	l0BytesServed := metrics.NewCounter(`cache_l0_bytes_served_total`)
	l1BytesServed := metrics.NewCounter(`cache_l1_bytes_served_total`)
	l2BytesFetched := metrics.NewCounter(`cache_l2_bytes_fetched_total`)

	// FUSE latencies
	fuseReadLatency := metrics.NewHistogram(`cache_fuse_read_latency_ms`)
	fuseLookupLatency := metrics.NewHistogram(`cache_fuse_lookup_latency_ms`)
	fuseGetattrLatency := metrics.NewHistogram(`cache_fuse_getattr_latency_ms`)

	// Throughput
	readThroughputMBps := metrics.NewHistogram(`cache_read_throughput_mbps`)

	return CacheMetrics{
		DiskCacheUsageMB:   diskCacheUsageMB,
		DiskCacheUsagePct:  diskCacheUsagePct,
		MemCacheUsageMB:    memCacheUsageMB,
		MemCacheUsagePct:   memCacheUsagePct,
		L0HitRatio:         l0HitRatio,
		L1HitRatio:         l1HitRatio,
		L2MissRatio:        l2MissRatio,
		L0Hits:             l0Hits,
		L1Hits:             l1Hits,
		L2Misses:           l2Misses,
		TotalReads:         totalReads,
		L0BytesServed:      l0BytesServed,
		L1BytesServed:      l1BytesServed,
		L2BytesFetched:     l2BytesFetched,
		FUSEReadLatency:    fuseReadLatency,
		FUSELookupLatency:  fuseLookupLatency,
		FUSEGetattrLatency: fuseGetattrLatency,
		ReadThroughputMBps: readThroughputMBps,
	}
}
