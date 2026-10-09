package common

import (
	"io"
	"os"
	"sync"
)

// LogBuffer spools logs locally so readers can reattach at their own offset.
// The unlinked file belongs to the container and is released by Dispose.
type LogBuffer struct {
	mu          sync.Mutex
	closeMu     sync.RWMutex
	file        *os.File
	err         error
	size        int64
	offset      int64
	writeChan   chan []byte
	closed      bool
	writeClosed bool
	closeOnce   sync.Once
	closedChan  chan struct{}
}

func NewLogBuffer() *LogBuffer {
	file, err := os.CreateTemp("", "beta9-logs-*")
	if err == nil {
		err = os.Remove(file.Name())
	}
	lb := &LogBuffer{file: file, err: err, writeChan: make(chan []byte, 2048), closedChan: make(chan struct{})}
	go lb.processWrites()
	return lb
}

func (lb *LogBuffer) processWrites() {
	for data := range lb.writeChan {
		lb.mu.Lock()
		if lb.err == nil {
			n, err := lb.file.Write(data)
			lb.size += int64(n)
			lb.err = err
		}
		lb.mu.Unlock()
	}
	lb.mu.Lock()
	lb.closed = true
	lb.mu.Unlock()
	close(lb.closedChan)
}

func (lb *LogBuffer) Write(data []byte) bool {
	lb.closeMu.RLock()
	defer lb.closeMu.RUnlock()
	if lb.writeClosed {
		return false
	}
	select {
	case lb.writeChan <- append([]byte(nil), data...):
		return true
	default:
		return false
	}
}

func (lb *LogBuffer) readAt(p []byte, offset int64) (int, error) {
	if lb.err != nil {
		return 0, lb.err
	}
	if offset < 0 || offset > lb.size {
		return 0, io.ErrUnexpectedEOF
	}
	if offset == lb.size {
		if lb.closed {
			return 0, io.EOF
		}
		return 0, nil
	}
	n, err := lb.file.ReadAt(p, offset)
	if err == io.EOF {
		err = nil
	}
	return n, err
}

func (lb *LogBuffer) ReadAt(p []byte, offset int64) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.readAt(p, offset)
}

func (lb *LogBuffer) Read(p []byte) (int, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	n, err := lb.readAt(p, lb.offset)
	lb.offset += int64(n)
	return n, err
}

func (lb *LogBuffer) Close() {
	lb.closeOnce.Do(func() {
		lb.closeMu.Lock()
		defer lb.closeMu.Unlock()
		lb.writeClosed = true
		close(lb.writeChan)
	})
}

func (lb *LogBuffer) Dispose() {
	lb.Close()
	<-lb.closedChan
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.file != nil {
		lb.file.Close()
		lb.file = nil
	}
	lb.err = io.ErrClosedPipe
}
