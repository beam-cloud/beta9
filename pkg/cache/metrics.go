package cache

import (
	"github.com/VictoriaMetrics/metrics"
)

var (
	cacheReadLocalTotal               = metrics.GetOrCreateCounter(`cache_read_local_total`)
	cacheReadLocalFDTotal             = metrics.GetOrCreateCounter(`cache_read_local_fd_total`)
	cacheReadClientLocalPageFileTotal = metrics.GetOrCreateCounter(`cache_read_client_local_page_file_total`)
	cacheReadRawTransportTotal        = metrics.GetOrCreateCounter(`cache_read_raw_transport_total`)
	cacheReadRawSendfileTotal         = metrics.GetOrCreateCounter(`cache_read_raw_sendfile_total`)
	cacheReadRawFallbackTotal         = metrics.GetOrCreateCounter(`cache_read_raw_fallback_total`)
	cacheReadGRPCCodecV2Total         = metrics.GetOrCreateCounter(`cache_read_grpc_codec_v2_total`)
	cacheReadGRPCCodecV2FallbackTotal = metrics.GetOrCreateCounter(`cache_read_grpc_codec_v2_fallback_total`)
	cachePageReadLatencyMs            = metrics.GetOrCreateHistogram(`cache_page_read_latency_ms`)
	cachePageLockWaitMs               = metrics.GetOrCreateHistogram(`cache_page_lock_wait_ms`)
	cachePrefetchPagesTotal           = metrics.GetOrCreateCounter(`cache_prefetch_pages_total`)
	cachePrefetchHitTotal             = metrics.GetOrCreateCounter(`cache_prefetch_hit_total`)
)
