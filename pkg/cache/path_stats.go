package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

const cachePathStatsLogInterval = 10 * time.Second

var (
	cachePathStats     cachePathStatsCounters
	cachePathStatsOnce sync.Once
)

type cachePathStatsCounters struct {
	clientReadIntoRequests int64
	clientReadIntoBytes    int64
	clientLocalHits        int64
	clientLocalMisses      int64
	clientRawHits          int64
	clientRawMisses        int64
	clientRawErrors        int64
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
	serverRawRequests     int64
	serverRawSendfileHits int64
	serverRawCopyHits     int64
	serverRawReadAtHits   int64
	serverRawMisses       int64
	serverRawErrors       int64

	storeReadAtRequests  int64
	storeReadAtBytes     int64
	storeMemoryPages     int64
	storeMemoryBytes     int64
	storeDiskPages       int64
	storeDiskBytes       int64
	storeMisses          int64
	storePageRegions     int64
	storePageRegionHits  int64
	storePageRegionMiss  int64
	storePageRegionBytes int64
}

type cachePathStatsSnapshot = cachePathStatsCounters

func startCachePathStatsLogger() {
	if Logger == nil || !Logger.debug {
		return
	}
	cachePathStatsOnce.Do(func() {
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
						"cache read path summary: client(read_into=%d %.2fMiB local_hit=%d local_miss=%d raw_hit=%d raw_miss=%d raw_err=%d grpc_hit=%d grpc_miss=%d grpc_err=%d) client_local_page_file(req=%d hit=%d miss=%d %.2fMiB) cachefs(read=%d %.2fMiB fd_hit=%d data=%d miss_retry=%d store_retry_err=%d read_err=%d) server(grpc_req=%d grpc_hit=%d grpc_miss=%d %.2fMiB stream_req=%d stream_chunks=%d %.2fMiB stream_err=%d raw_req=%d raw_sendfile=%d raw_copy=%d raw_readat=%d raw_miss=%d raw_err=%d) store(readat=%d %.2fMiB mem_pages=%d %.2fMiB disk_pages=%d %.2fMiB miss=%d page_region=%d hit=%d miss=%d %.2fMiB)",
						d.clientReadIntoRequests,
						bytesToMiB(d.clientReadIntoBytes),
						d.clientLocalHits,
						d.clientLocalMisses,
						d.clientRawHits,
						d.clientRawMisses,
						d.clientRawErrors,
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
						d.serverRawRequests,
						d.serverRawSendfileHits,
						d.serverRawCopyHits,
						d.serverRawReadAtHits,
						d.serverRawMisses,
						d.serverRawErrors,
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
		clientRawHits:          atomic.LoadInt64(&cachePathStats.clientRawHits),
		clientRawMisses:        atomic.LoadInt64(&cachePathStats.clientRawMisses),
		clientRawErrors:        atomic.LoadInt64(&cachePathStats.clientRawErrors),
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
		serverRawRequests:     atomic.LoadInt64(&cachePathStats.serverRawRequests),
		serverRawSendfileHits: atomic.LoadInt64(&cachePathStats.serverRawSendfileHits),
		serverRawCopyHits:     atomic.LoadInt64(&cachePathStats.serverRawCopyHits),
		serverRawReadAtHits:   atomic.LoadInt64(&cachePathStats.serverRawReadAtHits),
		serverRawMisses:       atomic.LoadInt64(&cachePathStats.serverRawMisses),
		serverRawErrors:       atomic.LoadInt64(&cachePathStats.serverRawErrors),

		storeReadAtRequests:  atomic.LoadInt64(&cachePathStats.storeReadAtRequests),
		storeReadAtBytes:     atomic.LoadInt64(&cachePathStats.storeReadAtBytes),
		storeMemoryPages:     atomic.LoadInt64(&cachePathStats.storeMemoryPages),
		storeMemoryBytes:     atomic.LoadInt64(&cachePathStats.storeMemoryBytes),
		storeDiskPages:       atomic.LoadInt64(&cachePathStats.storeDiskPages),
		storeDiskBytes:       atomic.LoadInt64(&cachePathStats.storeDiskBytes),
		storeMisses:          atomic.LoadInt64(&cachePathStats.storeMisses),
		storePageRegions:     atomic.LoadInt64(&cachePathStats.storePageRegions),
		storePageRegionHits:  atomic.LoadInt64(&cachePathStats.storePageRegionHits),
		storePageRegionMiss:  atomic.LoadInt64(&cachePathStats.storePageRegionMiss),
		storePageRegionBytes: atomic.LoadInt64(&cachePathStats.storePageRegionBytes),
	}
}

func diffCachePathStats(cur, prev cachePathStatsSnapshot) cachePathStatsSnapshot {
	return cachePathStatsSnapshot{
		clientReadIntoRequests: cur.clientReadIntoRequests - prev.clientReadIntoRequests,
		clientReadIntoBytes:    cur.clientReadIntoBytes - prev.clientReadIntoBytes,
		clientLocalHits:        cur.clientLocalHits - prev.clientLocalHits,
		clientLocalMisses:      cur.clientLocalMisses - prev.clientLocalMisses,
		clientRawHits:          cur.clientRawHits - prev.clientRawHits,
		clientRawMisses:        cur.clientRawMisses - prev.clientRawMisses,
		clientRawErrors:        cur.clientRawErrors - prev.clientRawErrors,
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
		serverRawRequests:     cur.serverRawRequests - prev.serverRawRequests,
		serverRawSendfileHits: cur.serverRawSendfileHits - prev.serverRawSendfileHits,
		serverRawCopyHits:     cur.serverRawCopyHits - prev.serverRawCopyHits,
		serverRawReadAtHits:   cur.serverRawReadAtHits - prev.serverRawReadAtHits,
		serverRawMisses:       cur.serverRawMisses - prev.serverRawMisses,
		serverRawErrors:       cur.serverRawErrors - prev.serverRawErrors,

		storeReadAtRequests:  cur.storeReadAtRequests - prev.storeReadAtRequests,
		storeReadAtBytes:     cur.storeReadAtBytes - prev.storeReadAtBytes,
		storeMemoryPages:     cur.storeMemoryPages - prev.storeMemoryPages,
		storeMemoryBytes:     cur.storeMemoryBytes - prev.storeMemoryBytes,
		storeDiskPages:       cur.storeDiskPages - prev.storeDiskPages,
		storeDiskBytes:       cur.storeDiskBytes - prev.storeDiskBytes,
		storeMisses:          cur.storeMisses - prev.storeMisses,
		storePageRegions:     cur.storePageRegions - prev.storePageRegions,
		storePageRegionHits:  cur.storePageRegionHits - prev.storePageRegionHits,
		storePageRegionMiss:  cur.storePageRegionMiss - prev.storePageRegionMiss,
		storePageRegionBytes: cur.storePageRegionBytes - prev.storePageRegionBytes,
	}
}

func (s cachePathStatsSnapshot) isZero() bool {
	return s.clientReadIntoRequests == 0 &&
		s.clientLocalPageFileRequests == 0 &&
		s.cacheFSReads == 0 &&
		s.serverGRPCGetRequests == 0 &&
		s.serverStreamRequests == 0 &&
		s.serverRawRequests == 0 &&
		s.storeReadAtRequests == 0 &&
		s.storePageRegions == 0
}

func bytesToMiB(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024)
}
