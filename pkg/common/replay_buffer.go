package common

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReplayBuffer retains unacknowledged bytes and applies backpressure at capacity.
// Read acknowledges only its requested offset; sending a response is not an ack.
type ReplayBuffer struct {
	mu       sync.Mutex
	data     []byte
	offset   uint64
	capacity int
	closed   bool
	changed  chan struct{}
}

func NewReplayBuffer(capacity int) *ReplayBuffer {
	return &ReplayBuffer{capacity: capacity, changed: make(chan struct{})}
}

func (b *ReplayBuffer) notify() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *ReplayBuffer) Append(ctx context.Context, data []byte) error {
	for len(data) > 0 {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return io.ErrClosedPipe
		}
		n := min(len(data), b.capacity-len(b.data))
		if n > 0 {
			b.data = append(b.data, data[:n]...)
			data = data[n:]
			b.notify()
		}
		changed := b.changed
		b.mu.Unlock()
		if len(data) > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
		}
	}
	return nil
}

func (b *ReplayBuffer) Read(ctx context.Context, offset uint64, size int) ([]byte, bool, error) {
	for {
		b.mu.Lock()
		if offset < b.offset || offset > b.offset+uint64(len(b.data)) {
			b.mu.Unlock()
			return nil, false, status.Error(codes.OutOfRange, "invalid stream acknowledgement")
		}
		if offset > b.offset {
			b.data = b.data[offset-b.offset:]
			b.offset = offset
			b.notify()
		}
		if len(b.data) > 0 || b.closed {
			data := append([]byte(nil), b.data[:min(size, len(b.data))]...)
			eof := b.closed && len(data) == 0
			b.mu.Unlock()
			return data, eof, nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-changed:
		}
	}
}

func (b *ReplayBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.notify()
	}
}
