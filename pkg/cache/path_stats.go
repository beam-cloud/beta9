package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	cachePathStatsLogInterval = 10 * time.Second
	cachePathTraceInitial     = 4
	cachePathTraceEvery       = 1024
	cachePathTraceSlow        = 100 * time.Millisecond
)

var (
	cachePathStats     cachePathStatsCounters
	cachePathStatsOnce sync.Once
)

type cachePathStatsCounters struct {
	clientReadIntoRequests int64
	clientReadIntoBytes    int64
	clientLocalHits        int64
	clientLocalMisses      int64
	clientRawRequests      int64
	clientRawHits          int64
	clientRawMisses        int64
	clientRawErrors        int64
	clientRawWaitNanos     int64
	clientRawHeaderNanos   int64
	clientRawBodyNanos     int64
	clientGRPCHits         int64
	clientGRPCMisses       int64
	clientGRPCErrors       int64

	clientLocalPageFileRequests int64
	clientLocalPageFileHits     int64
	clientLocalPageFileMisses   int64
	clientLocalPageFileBytes    int64

	cacheFSReads             int64
	cacheFSReadBytes         int64
	cacheFSLocalFDHits       int64
	cacheFSDataReads         int64
	cacheFSMissStoreRetries  int64
	cacheFSStoreRetryErrors  int64
	cacheFSReadContentErrors int64

	serverGRPCGetRequests int64
	serverGRPCGetHits     int64
	serverGRPCGetMisses   int64
	serverGRPCGetBytes    int64
	serverStreamRequests  int64
	serverStreamChunks    int64
	serverStreamBytes     int64
	serverStreamErrors    int64
	serverRawConns        int64
	serverRawRequests     int64
	serverRawSendfileHits int64
	serverRawCopyHits     int64
	serverRawReadAtHits   int64
	serverRawMisses       int64
	serverRawErrors       int64
	serverRawBytes        int64
	serverRawRegionNanos  int64
	serverRawOpenNanos    int64
	serverRawSendNanos    int64

	storeReadAtRequests      int64
	storeReadAtBytes         int64
	storeMemoryPages         int64
	storeMemoryBytes         int64
	storeDiskPages           int64
	storeDiskBytes           int64
	storeMisses              int64
	storePageRegions         int64
	storePageRegionHits      int64
	storePageRegionMiss      int64
	storePageRegionBytes     int64
	storePageRegionLockNanos int64
	storePageRegionPathNanos int64
	storeDiskLockNanos       int64
	storeDiskOpenNanos       int64
	storeDiskReadNanos       int64
}

type cachePathStatsSnapshot = cachePathStatsCounters

func startCachePathStatsLogger() {
	if Logger == nil || !Logger.debug {
		return
	}
	cachePathStatsOnce.Do(func() {
		Logger.Debugf("cache read path stats logger started: interval=%s", cachePathStatsLogInterval)
		go func() {
			ticker := time.NewTicker(cachePathStatsLogInterval)
			defer ticker.Stop()
			prev := snapshotCachePathStats()
			for {
				select {
				case <-ticker.C:
					cur := snapshotCachePathStats()
					d := diffCachePathStats(cur, prev)
					prev = cur
					if d.isZero() {
						continue
					}
					Logger.Debugf(
						"cache read path summary: client(read_into=%d %.2fMiB local_hit=%d local_miss=%d raw_req=%d raw_hit=%d raw_miss=%d raw_err=%d raw_wait_avg=%s raw_header_avg=%s raw_body_avg=%s grpc_hit=%d grpc_miss=%d grpc_err=%d) client_local_page_file(req=%d hit=%d miss=%d %.2fMiB) cachefs(read=%d %.2fMiB fd_hit=%d data=%d miss_retry=%d store_retry_err=%d read_err=%d) server(grpc_req=%d grpc_hit=%d grpc_miss=%d %.2fMiB stream_req=%d stream_chunks=%d %.2fMiB stream_err=%d raw_conn=%d raw_req=%d raw_sendfile=%d raw_copy=%d raw_readat=%d raw_miss=%d raw_err=%d %.2fMiB raw_region_avg=%s raw_open_avg=%s raw_send_avg=%s) store(readat=%d %.2fMiB mem_pages=%d %.2fMiB disk_pages=%d %.2fMiB miss=%d page_region=%d hit=%d miss=%d %.2fMiB page_lock_avg=%s page_path_avg=%s disk_lock_avg=%s disk_open_avg=%s disk_read_avg=%s)",
						d.clientReadIntoRequests,
						bytesToMiB(d.clientReadIntoBytes),
						d.clientLocalHits,
						d.clientLocalMisses,
						d.clientRawRequests,
						d.clientRawHits,
						d.clientRawMisses,
						d.clientRawErrors,
						avgDurationNanos(d.clientRawWaitNanos, d.clientRawHits+d.clientRawMisses+d.clientRawErrors),
						avgDurationNanos(d.clientRawHeaderNanos, d.clientRawHits+d.clientRawMisses+d.clientRawErrors),
						avgDurationNanos(d.clientRawBodyNanos, d.clientRawHits),
						d.clientGRPCHits,
						d.clientGRPCMisses,
						d.clientGRPCErrors,
						d.clientLocalPageFileRequests,
						d.clientLocalPageFileHits,
						d.clientLocalPageFileMisses,
						bytesToMiB(d.clientLocalPageFileBytes),
						d.cacheFSReads,
						bytesToMiB(d.cacheFSReadBytes),
						d.cacheFSLocalFDHits,
						d.cacheFSDataReads,
						d.cacheFSMissStoreRetries,
						d.cacheFSStoreRetryErrors,
						d.cacheFSReadContentErrors,
						d.serverGRPCGetRequests,
						d.serverGRPCGetHits,
						d.serverGRPCGetMisses,
						bytesToMiB(d.serverGRPCGetBytes),
						d.serverStreamRequests,
						d.serverStreamChunks,
						bytesToMiB(d.serverStreamBytes),
						d.serverStreamErrors,
						d.serverRawConns,
						d.serverRawRequests,
						d.serverRawSendfileHits,
						d.serverRawCopyHits,
						d.serverRawReadAtHits,
						d.serverRawMisses,
						d.serverRawErrors,
						bytesToMiB(d.serverRawBytes),
						avgDurationNanos(d.serverRawRegionNanos, d.serverRawRequests),
						avgDurationNanos(d.serverRawOpenNanos, d.serverRawSendfileHits+d.serverRawCopyHits),
						avgDurationNanos(d.serverRawSendNanos, d.serverRawSendfileHits+d.serverRawCopyHits+d.serverRawReadAtHits),
						d.storeReadAtRequests,
						bytesToMiB(d.storeReadAtBytes),
						d.storeMemoryPages,
						bytesToMiB(d.storeMemoryBytes),
						d.storeDiskPages,
						bytesToMiB(d.storeDiskBytes),
						d.storeMisses,
						d.storePageRegions,
						d.storePageRegionHits,
						d.storePageRegionMiss,
						bytesToMiB(d.storePageRegionBytes),
						avgDurationNanos(d.storePageRegionLockNanos, d.storePageRegions),
						avgDurationNanos(d.storePageRegionPathNanos, d.storePageRegions),
						avgDurationNanos(d.storeDiskLockNanos, d.storeDiskPages+d.storeMisses),
						avgDurationNanos(d.storeDiskOpenNanos, d.storeDiskPages),
						avgDurationNanos(d.storeDiskReadNanos, d.storeDiskPages),
					)
				}
			}
		}()
	})
}

func snapshotCachePathStats() cachePathStatsSnapshot {
	return cachePathStatsSnapshot{
		clientReadIntoRequests: atomic.LoadInt64(&cachePathStats.clientReadIntoRequests),
		clientReadIntoBytes:    atomic.LoadInt64(&cachePathStats.clientReadIntoBytes),
		clientLocalHits:        atomic.LoadInt64(&cachePathStats.clientLocalHits),
		clientLocalMisses:      atomic.LoadInt64(&cachePathStats.clientLocalMisses),
		clientRawRequests:      atomic.LoadInt64(&cachePathStats.clientRawRequests),
		clientRawHits:          atomic.LoadInt64(&cachePathStats.clientRawHits),
		clientRawMisses:        atomic.LoadInt64(&cachePathStats.clientRawMisses),
		clientRawErrors:        atomic.LoadInt64(&cachePathStats.clientRawErrors),
		clientRawWaitNanos:     atomic.LoadInt64(&cachePathStats.clientRawWaitNanos),
		clientRawHeaderNanos:   atomic.LoadInt64(&cachePathStats.clientRawHeaderNanos),
		clientRawBodyNanos:     atomic.LoadInt64(&cachePathStats.clientRawBodyNanos),
		clientGRPCHits:         atomic.LoadInt64(&cachePathStats.clientGRPCHits),
		clientGRPCMisses:       atomic.LoadInt64(&cachePathStats.clientGRPCMisses),
		clientGRPCErrors:       atomic.LoadInt64(&cachePathStats.clientGRPCErrors),

		clientLocalPageFileRequests: atomic.LoadInt64(&cachePathStats.clientLocalPageFileRequests),
		clientLocalPageFileHits:     atomic.LoadInt64(&cachePathStats.clientLocalPageFileHits),
		clientLocalPageFileMisses:   atomic.LoadInt64(&cachePathStats.clientLocalPageFileMisses),
		clientLocalPageFileBytes:    atomic.LoadInt64(&cachePathStats.clientLocalPageFileBytes),

		cacheFSReads:             atomic.LoadInt64(&cachePathStats.cacheFSReads),
		cacheFSReadBytes:         atomic.LoadInt64(&cachePathStats.cacheFSReadBytes),
		cacheFSLocalFDHits:       atomic.LoadInt64(&cachePathStats.cacheFSLocalFDHits),
		cacheFSDataReads:         atomic.LoadInt64(&cachePathStats.cacheFSDataReads),
		cacheFSMissStoreRetries:  atomic.LoadInt64(&cachePathStats.cacheFSMissStoreRetries),
		cacheFSStoreRetryErrors:  atomic.LoadInt64(&cachePathStats.cacheFSStoreRetryErrors),
		cacheFSReadContentErrors: atomic.LoadInt64(&cachePathStats.cacheFSReadContentErrors),

		serverGRPCGetRequests: atomic.LoadInt64(&cachePathStats.serverGRPCGetRequests),
		serverGRPCGetHits:     atomic.LoadInt64(&cachePathStats.serverGRPCGetHits),
		serverGRPCGetMisses:   atomic.LoadInt64(&cachePathStats.serverGRPCGetMisses),
		serverGRPCGetBytes:    atomic.LoadInt64(&cachePathStats.serverGRPCGetBytes),
		serverStreamRequests:  atomic.LoadInt64(&cachePathStats.serverStreamRequests),
		serverStreamChunks:    atomic.LoadInt64(&cachePathStats.serverStreamChunks),
		serverStreamBytes:     atomic.LoadInt64(&cachePathStats.serverStreamBytes),
		serverStreamErrors:    atomic.LoadInt64(&cachePathStats.serverStreamErrors),
		serverRawConns:        atomic.LoadInt64(&cachePathStats.serverRawConns),
		serverRawRequests:     atomic.LoadInt64(&cachePathStats.serverRawRequests),
		serverRawSendfileHits: atomic.LoadInt64(&cachePathStats.serverRawSendfileHits),
		serverRawCopyHits:     atomic.LoadInt64(&cachePathStats.serverRawCopyHits),
		serverRawReadAtHits:   atomic.LoadInt64(&cachePathStats.serverRawReadAtHits),
		serverRawMisses:       atomic.LoadInt64(&cachePathStats.serverRawMisses),
		serverRawErrors:       atomic.LoadInt64(&cachePathStats.serverRawErrors),
		serverRawBytes:        atomic.LoadInt64(&cachePathStats.serverRawBytes),
		serverRawRegionNanos:  atomic.LoadInt64(&cachePathStats.serverRawRegionNanos),
		serverRawOpenNanos:    atomic.LoadInt64(&cachePathStats.serverRawOpenNanos),
		serverRawSendNanos:    atomic.LoadInt64(&cachePathStats.serverRawSendNanos),

		storeReadAtRequests:      atomic.LoadInt64(&cachePathStats.storeReadAtRequests),
		storeReadAtBytes:         atomic.LoadInt64(&cachePathStats.storeReadAtBytes),
		storeMemoryPages:         atomic.LoadInt64(&cachePathStats.storeMemoryPages),
		storeMemoryBytes:         atomic.LoadInt64(&cachePathStats.storeMemoryBytes),
		storeDiskPages:           atomic.LoadInt64(&cachePathStats.storeDiskPages),
		storeDiskBytes:           atomic.LoadInt64(&cachePathStats.storeDiskBytes),
		storeMisses:              atomic.LoadInt64(&cachePathStats.storeMisses),
		storePageRegions:         atomic.LoadInt64(&cachePathStats.storePageRegions),
		storePageRegionHits:      atomic.LoadInt64(&cachePathStats.storePageRegionHits),
		storePageRegionMiss:      atomic.LoadInt64(&cachePathStats.storePageRegionMiss),
		storePageRegionBytes:     atomic.LoadInt64(&cachePathStats.storePageRegionBytes),
		storePageRegionLockNanos: atomic.LoadInt64(&cachePathStats.storePageRegionLockNanos),
		storePageRegionPathNanos: atomic.LoadInt64(&cachePathStats.storePageRegionPathNanos),
		storeDiskLockNanos:       atomic.LoadInt64(&cachePathStats.storeDiskLockNanos),
		storeDiskOpenNanos:       atomic.LoadInt64(&cachePathStats.storeDiskOpenNanos),
		storeDiskReadNanos:       atomic.LoadInt64(&cachePathStats.storeDiskReadNanos),
	}
}

func diffCachePathStats(cur, prev cachePathStatsSnapshot) cachePathStatsSnapshot {
	return cachePathStatsSnapshot{
		clientReadIntoRequests: cur.clientReadIntoRequests - prev.clientReadIntoRequests,
		clientReadIntoBytes:    cur.clientReadIntoBytes - prev.clientReadIntoBytes,
		clientLocalHits:        cur.clientLocalHits - prev.clientLocalHits,
		clientLocalMisses:      cur.clientLocalMisses - prev.clientLocalMisses,
		clientRawRequests:      cur.clientRawRequests - prev.clientRawRequests,
		clientRawHits:          cur.clientRawHits - prev.clientRawHits,
		clientRawMisses:        cur.clientRawMisses - prev.clientRawMisses,
		clientRawErrors:        cur.clientRawErrors - prev.clientRawErrors,
		clientRawWaitNanos:     cur.clientRawWaitNanos - prev.clientRawWaitNanos,
		clientRawHeaderNanos:   cur.clientRawHeaderNanos - prev.clientRawHeaderNanos,
		clientRawBodyNanos:     cur.clientRawBodyNanos - prev.clientRawBodyNanos,
		clientGRPCHits:         cur.clientGRPCHits - prev.clientGRPCHits,
		clientGRPCMisses:       cur.clientGRPCMisses - prev.clientGRPCMisses,
		clientGRPCErrors:       cur.clientGRPCErrors - prev.clientGRPCErrors,

		clientLocalPageFileRequests: cur.clientLocalPageFileRequests - prev.clientLocalPageFileRequests,
		clientLocalPageFileHits:     cur.clientLocalPageFileHits - prev.clientLocalPageFileHits,
		clientLocalPageFileMisses:   cur.clientLocalPageFileMisses - prev.clientLocalPageFileMisses,
		clientLocalPageFileBytes:    cur.clientLocalPageFileBytes - prev.clientLocalPageFileBytes,

		cacheFSReads:             cur.cacheFSReads - prev.cacheFSReads,
		cacheFSReadBytes:         cur.cacheFSReadBytes - prev.cacheFSReadBytes,
		cacheFSLocalFDHits:       cur.cacheFSLocalFDHits - prev.cacheFSLocalFDHits,
		cacheFSDataReads:         cur.cacheFSDataReads - prev.cacheFSDataReads,
		cacheFSMissStoreRetries:  cur.cacheFSMissStoreRetries - prev.cacheFSMissStoreRetries,
		cacheFSStoreRetryErrors:  cur.cacheFSStoreRetryErrors - prev.cacheFSStoreRetryErrors,
		cacheFSReadContentErrors: cur.cacheFSReadContentErrors - prev.cacheFSReadContentErrors,

		serverGRPCGetRequests: cur.serverGRPCGetRequests - prev.serverGRPCGetRequests,
		serverGRPCGetHits:     cur.serverGRPCGetHits - prev.serverGRPCGetHits,
		serverGRPCGetMisses:   cur.serverGRPCGetMisses - prev.serverGRPCGetMisses,
		serverGRPCGetBytes:    cur.serverGRPCGetBytes - prev.serverGRPCGetBytes,
		serverStreamRequests:  cur.serverStreamRequests - prev.serverStreamRequests,
		serverStreamChunks:    cur.serverStreamChunks - prev.serverStreamChunks,
		serverStreamBytes:     cur.serverStreamBytes - prev.serverStreamBytes,
		serverStreamErrors:    cur.serverStreamErrors - prev.serverStreamErrors,
		serverRawConns:        cur.serverRawConns - prev.serverRawConns,
		serverRawRequests:     cur.serverRawRequests - prev.serverRawRequests,
		serverRawSendfileHits: cur.serverRawSendfileHits - prev.serverRawSendfileHits,
		serverRawCopyHits:     cur.serverRawCopyHits - prev.serverRawCopyHits,
		serverRawReadAtHits:   cur.serverRawReadAtHits - prev.serverRawReadAtHits,
		serverRawMisses:       cur.serverRawMisses - prev.serverRawMisses,
		serverRawErrors:       cur.serverRawErrors - prev.serverRawErrors,
		serverRawBytes:        cur.serverRawBytes - prev.serverRawBytes,
		serverRawRegionNanos:  cur.serverRawRegionNanos - prev.serverRawRegionNanos,
		serverRawOpenNanos:    cur.serverRawOpenNanos - prev.serverRawOpenNanos,
		serverRawSendNanos:    cur.serverRawSendNanos - prev.serverRawSendNanos,

		storeReadAtRequests:      cur.storeReadAtRequests - prev.storeReadAtRequests,
		storeReadAtBytes:         cur.storeReadAtBytes - prev.storeReadAtBytes,
		storeMemoryPages:         cur.storeMemoryPages - prev.storeMemoryPages,
		storeMemoryBytes:         cur.storeMemoryBytes - prev.storeMemoryBytes,
		storeDiskPages:           cur.storeDiskPages - prev.storeDiskPages,
		storeDiskBytes:           cur.storeDiskBytes - prev.storeDiskBytes,
		storeMisses:              cur.storeMisses - prev.storeMisses,
		storePageRegions:         cur.storePageRegions - prev.storePageRegions,
		storePageRegionHits:      cur.storePageRegionHits - prev.storePageRegionHits,
		storePageRegionMiss:      cur.storePageRegionMiss - prev.storePageRegionMiss,
		storePageRegionBytes:     cur.storePageRegionBytes - prev.storePageRegionBytes,
		storePageRegionLockNanos: cur.storePageRegionLockNanos - prev.storePageRegionLockNanos,
		storePageRegionPathNanos: cur.storePageRegionPathNanos - prev.storePageRegionPathNanos,
		storeDiskLockNanos:       cur.storeDiskLockNanos - prev.storeDiskLockNanos,
		storeDiskOpenNanos:       cur.storeDiskOpenNanos - prev.storeDiskOpenNanos,
		storeDiskReadNanos:       cur.storeDiskReadNanos - prev.storeDiskReadNanos,
	}
}

func (s cachePathStatsSnapshot) isZero() bool {
	return s.clientReadIntoRequests == 0 &&
		s.clientLocalPageFileRequests == 0 &&
		s.cacheFSReads == 0 &&
		s.serverGRPCGetRequests == 0 &&
		s.serverStreamRequests == 0 &&
		s.serverRawConns == 0 &&
		s.serverRawRequests == 0 &&
		s.storeReadAtRequests == 0 &&
		s.storePageRegions == 0
}

func shouldTraceCachePath(count int64, elapsed time.Duration, failed bool) bool {
	if Logger == nil || !Logger.debug {
		return false
	}
	return failed || elapsed >= cachePathTraceSlow || count <= cachePathTraceInitial || count%cachePathTraceEvery == 0
}

func bytesToMiB(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func avgDurationNanos(totalNanos int64, count int64) time.Duration {
	if count <= 0 {
		return 0
	}
	return time.Duration(totalNanos / count)
}
