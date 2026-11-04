package common

import (
	"io"
	"os"
	"sync"

	"github.com/rs/zerolog"
)

// BufferedJSONLogWriter batches already encoded JSON records onto one sink.
// Its byte and record limits apply backpressure rather than dropping logs.
// Only the drain goroutine writes the sink, keeping standard-stream FD locks
// off the guest-readiness path. Flush/Close surface asynchronous sink failures.
type BufferedJSONLogWriter struct {
	writeMu              sync.Mutex
	mu                   sync.Mutex
	changed              *sync.Cond
	sink                 io.Writer
	queue                []bufferedLogRecord
	bytes                int
	maxBytes, maxRecords int
	enqueued, completed  uint64
	peakBytes, peakQueue int
	backpressureWaits    uint64
	sinkWrites           uint64
	closed               bool
	err                  error
	done                 chan struct{}
}

type bufferedLogRecord struct {
	data []byte
	seq  uint64
}

func NewBufferedJSONLogWriter(sink io.Writer, maxBytes, maxRecords int) *BufferedJSONLogWriter {
	w := &BufferedJSONLogWriter{sink: sink, maxBytes: max(1, maxBytes), maxRecords: max(1, maxRecords), done: make(chan struct{})}
	w.changed = sync.NewCond(&w.mu)
	go w.drain()
	return w
}

func (w *BufferedJSONLogWriter) Write(data []byte) (int, error) {
	// Preserve record ordering even when an oversized record must be split.
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	written := 0
	for len(data) > 0 {
		n := min(len(data), w.maxBytes, 64<<10)
		w.mu.Lock()
		for !w.closed && w.err == nil && (w.bytes+n > w.maxBytes || len(w.queue) >= w.maxRecords) {
			w.backpressureWaits++
			w.changed.Wait()
		}
		if w.err != nil || w.closed {
			err := w.err
			if err == nil {
				err = os.ErrClosed
			}
			w.mu.Unlock()
			return written, err
		}
		w.enqueued++
		w.queue = append(w.queue, bufferedLogRecord{data: append([]byte(nil), data[:n]...), seq: w.enqueued})
		w.bytes += n
		w.peakBytes = max(w.peakBytes, w.bytes)
		w.peakQueue = max(w.peakQueue, len(w.queue))
		w.changed.Broadcast()
		w.mu.Unlock()
		written += n
		data = data[n:]
	}
	return written, nil
}

func (w *BufferedJSONLogWriter) WriteLevel(level zerolog.Level, data []byte) (int, error) {
	n, err := w.Write(data)
	// zerolog Fatal exits without running defers; panic may be recovered.
	if err == nil && (level == zerolog.FatalLevel || level == zerolog.PanicLevel) {
		err = w.Flush()
	}
	return n, err
}

func (w *BufferedJSONLogWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	target := w.enqueued
	for w.completed < target && w.err == nil {
		w.changed.Wait()
	}
	return w.err
}

func (w *BufferedJSONLogWriter) Close() error {
	w.mu.Lock()
	w.closed = true
	w.changed.Broadcast()
	w.mu.Unlock()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *BufferedJSONLogWriter) Stats() map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	return map[string]any{
		"queued_bytes": w.bytes, "queued_chunks": len(w.queue),
		"peak_queued_bytes": w.peakBytes, "peak_queued_chunks": w.peakQueue,
		"byte_limit": w.maxBytes, "chunk_limit": w.maxRecords,
		"backpressure_waits": w.backpressureWaits, "sink_writes": w.sinkWrites,
		"accepted_chunks": w.enqueued, "flushed_chunks": w.completed, "sink_error": w.err != nil,
	}
}

func (w *BufferedJSONLogWriter) drain() {
	defer close(w.done)
	for {
		w.mu.Lock()
		for len(w.queue) == 0 && !w.closed && w.err == nil {
			w.changed.Wait()
		}
		if len(w.queue) == 0 || w.err != nil {
			w.mu.Unlock()
			return
		}
		batch := make([]byte, 0, min(w.bytes, 64<<10))
		var last uint64
		for len(w.queue) > 0 && len(batch)+len(w.queue[0].data) <= 64<<10 {
			record := w.queue[0]
			batch = append(batch, record.data...)
			last = record.seq
			w.bytes -= len(record.data)
			w.queue[0] = bufferedLogRecord{}
			w.queue = w.queue[1:]
		}
		if len(w.queue) == 0 {
			w.queue = nil
		}
		w.changed.Broadcast()
		w.sinkWrites++
		w.mu.Unlock()
		n, err := w.sink.Write(batch)
		if err == nil && n != len(batch) {
			err = io.ErrShortWrite
		}
		w.mu.Lock()
		if err != nil {
			w.err = err
			w.queue, w.bytes = nil, 0
		} else {
			w.completed = last
		}
		w.changed.Broadcast()
		w.mu.Unlock()
	}
}
