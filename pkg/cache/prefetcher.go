package cache

import (
	"context"
	"sync"
	"time"
)

const (
	// Prefetch configuration aligned with optimization plan
	prefetchAheadChunks    = 16               // Prefetch 16-64 MiB ahead with 4MB chunks
	prefetchThresholdBytes = 2 * 1024 * 1024  // 2MB - detect sequential after 2 adjacent reads
	prefetchWorkers        = 4                // Parallel prefetch workers
	prefetchCacheTime      = 30 * time.Second // How long to keep prefetch state
)

// PrefetchState tracks sequential read patterns per file/hash
type PrefetchState struct {
	lastOffset     int64
	lastAccessTime time.Time
	sequential     bool
	prefetching    bool
}

// Prefetcher detects sequential reads and prefetches ahead
type Prefetcher struct {
	ctx        context.Context
	cas        *Store
	states     map[string]*PrefetchState
	mu         sync.RWMutex
	workQueue  chan *prefetchTask
	bufferPool *BufferPool
}

type prefetchTask struct {
	hash   string
	offset int64
	length int64
}

// NewPrefetcher creates a new prefetcher instance
func NewPrefetcher(ctx context.Context, cas *Store, bufferPool *BufferPool) *Prefetcher {
	pf := &Prefetcher{
		ctx:        ctx,
		cas:        cas,
		states:     make(map[string]*PrefetchState),
		workQueue:  make(chan *prefetchTask, 100),
		bufferPool: bufferPool,
	}

	// Start prefetch workers
	for i := 0; i < prefetchWorkers; i++ {
		go pf.worker()
	}

	// Cleanup stale states periodically
	go pf.cleanupWorker()

	return pf
}

// OnRead should be called on each read to detect patterns
func (pf *Prefetcher) OnRead(hash string, offset, length int64) {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	now := time.Now()
	state, exists := pf.states[hash]

	if !exists {
		state = &PrefetchState{
			lastOffset:     offset + length,
			lastAccessTime: now,
			sequential:     false,
			prefetching:    false,
		}
		pf.states[hash] = state
		return
	}

	// Check if this read is sequential (adjacent or nearly adjacent)
	isSequential := offset >= state.lastOffset &&
		(offset-state.lastOffset) <= prefetchThresholdBytes

	if isSequential && !state.sequential {
		// Just detected sequential pattern, start prefetching
		state.sequential = true
		pf.startPrefetch(hash, offset+length)
	} else if isSequential && state.sequential && !state.prefetching {
		// Continue prefetching for ongoing sequential reads
		pf.startPrefetch(hash, offset+length)
	} else if !isSequential {
		// Pattern broken, mark as non-sequential
		state.sequential = false
	}

	state.lastOffset = offset + length
	state.lastAccessTime = now
}

// startPrefetch queues prefetch tasks for future chunks
func (pf *Prefetcher) startPrefetch(hash string, startOffset int64) {
	state := pf.states[hash]
	if state.prefetching {
		return
	}

	state.prefetching = true

	go func() {
		defer func() {
			pf.mu.Lock()
			if s, exists := pf.states[hash]; exists {
				s.prefetching = false
			}
			pf.mu.Unlock()
		}()

		// Queue multiple chunks for prefetch
		chunkSize := int64(4 * 1024 * 1024) // 4MB chunks
		for i := int64(0); i < prefetchAheadChunks; i++ {
			offset := startOffset + (i * chunkSize)

			select {
			case pf.workQueue <- &prefetchTask{
				hash:   hash,
				offset: offset,
				length: chunkSize,
			}:
			case <-pf.ctx.Done():
				return
			default:
				// Queue full, stop prefetching
				return
			}
		}
	}()
}

// worker processes prefetch tasks
func (pf *Prefetcher) worker() {
	for {
		select {
		case task := <-pf.workQueue:
			// Check if content is already in cache
			if pf.cas.Exists(task.hash) {
				// Try to warm up the chunk in memory if it's on disk
				dst := pf.bufferPool.Get(int(task.length))
				_, err := pf.cas.Get(task.hash, task.offset, task.length, dst)
				pf.bufferPool.Put(dst)

				if err == nil {
					Logger.Debugf("Prefetched chunk [%s] offset=%d length=%d",
						task.hash, task.offset, task.length)
				}
			}
		case <-pf.ctx.Done():
			return
		}
	}
}

// cleanupWorker removes stale prefetch states
func (pf *Prefetcher) cleanupWorker() {
	ticker := time.NewTicker(prefetchCacheTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pf.cleanup()
		case <-pf.ctx.Done():
			return
		}
	}
}

func (pf *Prefetcher) cleanup() {
	pf.mu.Lock()
	defer pf.mu.Unlock()

	now := time.Now()
	for hash, state := range pf.states {
		if now.Sub(state.lastAccessTime) > prefetchCacheTime {
			delete(pf.states, hash)
		}
	}
}
