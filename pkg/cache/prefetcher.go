package cache

import (
	"context"
	"sync"
	"time"
)

const (
	// Prefetch configuration aligned with optimization plan
	prefetchDefaultAheadBytes      = 64 * 1024 * 1024
	prefetchDefaultPartLengthBytes = 4 * 1024 * 1024
	prefetchDefaultWorkers         = 4
	prefetchDefaultMaxPartsPerRead = 16
	prefetchMaxWorkers             = 64
	prefetchMaxPartsPerRead        = 1024
	prefetchThresholdBytes         = 2 * 1024 * 1024  // 2MB - detect sequential after 2 adjacent reads
	prefetchCacheTime              = 30 * time.Second // How long to keep prefetch state
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
	aheadBytes int64
	partLength int64
	maxParts   int
}

type prefetchTask struct {
	hash   string
	offset int64
	length int64
}

// NewPrefetcher creates a new prefetcher instance
func NewPrefetcher(ctx context.Context, cas *Store, bufferPool *BufferPool) *Prefetcher {
	cfg := cas.prefetchConfig
	aheadBytes := cfg.AheadBytes
	if aheadBytes <= 0 {
		aheadBytes = prefetchDefaultAheadBytes
	}
	partLength := cfg.PartLengthBytes
	if partLength <= 0 {
		partLength = prefetchDefaultPartLengthBytes
	}
	workers := cfg.Workers
	if workers <= 0 {
		workers = prefetchDefaultWorkers
	}
	if workers > prefetchMaxWorkers {
		workers = prefetchMaxWorkers
	}
	maxParts := cfg.MaxPartsPerRead
	if maxParts <= 0 {
		maxParts = prefetchDefaultMaxPartsPerRead
	}
	if maxParts > prefetchMaxPartsPerRead {
		maxParts = prefetchMaxPartsPerRead
	}

	pf := &Prefetcher{
		ctx:        ctx,
		cas:        cas,
		states:     make(map[string]*PrefetchState),
		workQueue:  make(chan *prefetchTask, workers*maxParts),
		bufferPool: bufferPool,
		aheadBytes: aheadBytes,
		partLength: partLength,
		maxParts:   maxParts,
	}

	// Start prefetch workers
	for i := 0; i < workers; i++ {
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

		parts := int(pf.aheadBytes / pf.partLength)
		if parts <= 0 {
			parts = 1
		}
		if parts > pf.maxParts {
			parts = pf.maxParts
		}
		for i := 0; i < parts; i++ {
			offset := startOffset + (int64(i) * pf.partLength)

			select {
			case pf.workQueue <- &prefetchTask{
				hash:   hash,
				offset: offset,
				length: pf.partLength,
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
			pf.cas.WarmRange(task.hash, task.offset, task.length)
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
