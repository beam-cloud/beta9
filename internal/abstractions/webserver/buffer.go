package webserver

import (
	"context"
	"io"
	"net/http"
	"sync"
)

type RequestData struct {
	ctx    context.Context
	stubId string

	Method         string
	URL            string
	Headers        http.Header
	Body           io.ReadCloser
	ResponseWriter *io.PipeWriter
}

type RingBuffer struct {
	buffer []RequestData
	size   int
	head   int
	tail   int
	count  int
	mu     sync.Mutex
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		buffer: make([]RequestData, size),
		size:   size,
	}
}

// Push adds a new request to the buffer
func (rb *RingBuffer) Push(request RequestData) {
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
func (rb *RingBuffer) Pop() (RequestData, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 {
		return RequestData{}, false
	}

	request := rb.buffer[rb.head]
	rb.head = (rb.head + 1) % rb.size
	rb.count--

	return request, true
}

func (rb *RingBuffer) Len() int {
	return rb.count
}
