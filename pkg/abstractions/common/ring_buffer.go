package abstractions

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

// Push adds a new request to the buffer. If priority is true, it gets inserted at the front of the buffer
func (rb *RingBuffer[T]) Push(request T, priority bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if priority {
		if rb.count == rb.size {
			// Buffer is full, overwrite the oldest element at head without moving pointers
			// No adjustment to head or tail, since the count remains the same
			rb.buffer[rb.head] = request
		} else {
			// Move head backward to insert at front
			rb.head = (rb.head - 1 + rb.size) % rb.size
			rb.buffer[rb.head] = request
			rb.count++
		}
	} else {
		// Normal FIFO insert at tail
		rb.buffer[rb.tail] = request
		rb.tail = (rb.tail + 1) % rb.size

		// Buffer is full, move head forward to overwrite oldest element
		if rb.count == rb.size {
			rb.head = (rb.head + 1) % rb.size
		} else {
			rb.count++
		}
	}
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
	rb.buffer[rb.head] = *new(T) // Clear the value
	rb.head = (rb.head + 1) % rb.size
	rb.count--

	return request, true
}

func (rb *RingBuffer[T]) Len() int {
	return rb.count
}
