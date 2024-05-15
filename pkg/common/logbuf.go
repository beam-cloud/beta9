package common

import (
	"io"
	"sync"
)

type LogBuffer struct {
	mu        sync.Mutex
	logs      []byte
	writeChan chan []byte
	closed    bool
}

func NewLogBuffer() *LogBuffer {
	lb := &LogBuffer{
		writeChan: make(chan []byte, 2048),
	}

	go lb.processWrites()
	return lb
}

func (lb *LogBuffer) processWrites() {
	for log := range lb.writeChan {
		lb.mu.Lock()
		lb.logs = append(lb.logs, log...)
		lb.mu.Unlock()
	}

	lb.mu.Lock()
	lb.closed = true
	lb.mu.Unlock()
}

func (lb *LogBuffer) Write(buf []byte) {
	select {
	case lb.writeChan <- buf:
		// Log message sent to channel
	default:
		// Non-blocking, log message is dropped
	}
}

func (lb *LogBuffer) Read(p []byte) (n int, err error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if len(lb.logs) == 0 {
		if lb.closed {
			return 0, io.EOF
		}

		return 0, nil // No data to read
	}

	n = copy(p, lb.logs)
	lb.logs = lb.logs[n:] // Remove read data from buffer
	return n, nil
}

func (lb *LogBuffer) Close() {
	close(lb.writeChan)
}
