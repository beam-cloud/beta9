package cache

import (
	"sync"
)

// BufferPool provides a pool of reusable byte slices to reduce allocations
// Optimized for 1-4MB chunks as recommended in the optimization plan
type BufferPool struct {
	pools map[int]*sync.Pool
}

// Standard buffer sizes aligned with typical chunk sizes
var (
	BufferSize1MB  = 1 * 1024 * 1024
	BufferSize4MB  = 4 * 1024 * 1024
	BufferSize16MB = 16 * 1024 * 1024
)

// NewBufferPool creates a new buffer pool with predefined size buckets
func NewBufferPool() *BufferPool {
	bp := &BufferPool{
		pools: make(map[int]*sync.Pool),
	}

	// Initialize pools for common sizes
	for _, size := range []int{BufferSize1MB, BufferSize4MB, BufferSize16MB} {
		size := size // capture for closure
		bp.pools[size] = &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		}
	}

	return bp
}

// Get retrieves a buffer of at least the requested size
func (bp *BufferPool) Get(size int) []byte {
	// Find the smallest pool that fits
	poolSize := bp.selectPoolSize(size)

	if pool, exists := bp.pools[poolSize]; exists {
		bufPtr := pool.Get().(*[]byte)
		buf := *bufPtr
		return buf[:size]
	}

	// Fallback: allocate directly if no suitable pool
	return make([]byte, size)
}

// Put returns a buffer to the pool
func (bp *BufferPool) Put(buf []byte) {
	if buf == nil {
		return
	}

	capacity := cap(buf)

	// Only return buffers that match our pool sizes
	if pool, exists := bp.pools[capacity]; exists {
		// Reset length to capacity before returning
		buf = buf[:capacity]
		pool.Put(&buf)
	}
}

// selectPoolSize selects the appropriate pool size for the request
func (bp *BufferPool) selectPoolSize(size int) int {
	if size <= BufferSize1MB {
		return BufferSize1MB
	} else if size <= BufferSize4MB {
		return BufferSize4MB
	} else {
		return BufferSize16MB
	}
}
