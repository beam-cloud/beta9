package common

import (
	"sync"
)

type RingBuffer[T any] struct {
	buffer []T
	size   int
	head   int
	tail   int
	count  int
	mu     sync.Mutex
}

func NewRingBuffer[T any](size int) *RingBuffer[T] {
	return &RingBuffer[T]{
		buffer: make([]T, size),
		size:   size,
	}
}

// Push adds a new request to the buffer
func (rb *RingBuffer[T]) Push(request T) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == rb.size {
		rb.head = (rb.head + 1) % rb.size
	} else {
		rb.count++
	}

	rb.buffer[rb.tail] = request
	rb.tail = (rb.tail + 1) % rb.size
}

// Pop retrieves and removes the oldest request from the buffer
func (rb *RingBuffer[T]) Pop() (T, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	var request T
	if rb.count == 0 {
		return request, false
	}

	request = rb.buffer[rb.head]
	rb.head = (rb.head + 1) % rb.size
	rb.count--

	return request, true
}

func (rb *RingBuffer[T]) Len() int {
	return rb.count
}
