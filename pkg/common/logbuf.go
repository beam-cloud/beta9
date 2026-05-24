package common

import (
	"io"
	"sync"
)

type LogBuffer struct {
	mu          sync.Mutex
	closeMu     sync.RWMutex
	logs        []byte
	writeChan   chan []byte
	closed      bool
	writeClosed bool
	closeOnce   sync.Once
	closedChan  chan struct{}
}

func NewLogBuffer() *LogBuffer {
	lb := &LogBuffer{
		writeChan:  make(chan []byte, 2048),
		closedChan: make(chan struct{}),
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
	close(lb.closedChan)
}

func (lb *LogBuffer) Write(buf []byte) bool {
	lb.closeMu.RLock()
	defer lb.closeMu.RUnlock()

	if lb.writeClosed {
		return false
	}

	select {
	case lb.writeChan <- buf:
		return true
	default:
		return false
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
	lb.closeOnce.Do(func() {
		lb.closeMu.Lock()
		defer lb.closeMu.Unlock()
		lb.writeClosed = true
		close(lb.writeChan)
	})
}
