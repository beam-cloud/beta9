package worker

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	tunnelReconnectTimeout = 2 * time.Minute
	tunnelChunkSize        = 64 << 10
	tunnelBufferSize       = 1 << 20
	tunnelSessionLimit     = 32
)

type tunnelSessions struct {
	mu       sync.Mutex
	sessions map[string]*tunnelSession
	closed   bool
}

type tunnelSession struct {
	port        uint32
	conn        net.Conn
	output      *tunnelBuffer
	ctx         context.Context
	cancel      context.CancelFunc
	lease       *time.Timer
	inputMu     sync.Mutex
	inputOffset uint64
	inputEOF    bool
	closedAt    atomic.Int64
}

func (s *ContainerRuntimeServer) ContainerTunnel(ctx context.Context, in *pb.ContainerTunnelRequest) (*pb.ContainerTunnelResponse, error) {
	if _, err := uuid.Parse(in.SessionId); err != nil || in.Port == 0 || in.Port > 65535 || len(in.Input) > tunnelChunkSize {
		return nil, status.Error(codes.InvalidArgument, "invalid tunnel request")
	}
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return nil, status.Error(codes.NotFound, "container not found")
	}
	session, err := instance.tunnels.get(in, func() (net.Conn, error) {
		address := instance.containerAddress(int32(in.Port))
		if address == "" {
			return nil, status.Error(codes.FailedPrecondition, "port is not bound")
		}
		return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", address)
	})
	if err != nil {
		return nil, err
	}
	if in.Close {
		session.close()
		return &pb.ContainerTunnelResponse{}, nil
	}
	inputOffset, err := session.write(in.InputOffset, in.Input, in.InputEof)
	if err != nil {
		return nil, err
	}
	session.inputMu.Lock()
	inputEOF := session.inputEOF
	session.inputMu.Unlock()
	response := &pb.ContainerTunnelResponse{InputOffset: inputOffset, OutputOffset: in.OutputOffset, InputEof: inputEOF}
	if in.Read {
		readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		response.Output, response.OutputEof, err = session.output.Read(readCtx, in.OutputOffset, tunnelChunkSize)
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			err = nil
		}
	}
	return response, err
}

func (s *tunnelSessions) get(in *pb.ContainerTunnelRequest, dial func() (net.Conn, error)) (*tunnelSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, status.Error(codes.NotFound, "container stopped")
	}
	if s.sessions == nil {
		s.sessions = make(map[string]*tunnelSession)
	}
	active := 0
	for id, session := range s.sessions {
		if closed := session.closedAt.Load(); closed > 0 {
			if time.Since(time.Unix(0, closed)) > 5*time.Minute {
				delete(s.sessions, id)
			}
		} else {
			active++
		}
	}
	if session, ok := s.sessions[in.SessionId]; ok {
		if session.port != in.Port {
			return nil, status.Error(codes.InvalidArgument, "tunnel port changed")
		}
		if session.ctx.Err() != nil {
			return nil, status.Error(codes.NotFound, "tunnel expired")
		}
		session.lease.Reset(tunnelReconnectTimeout)
		return session, nil
	}
	if !in.Create {
		return nil, status.Error(codes.NotFound, "tunnel not found")
	}
	if active >= tunnelSessionLimit || len(s.sessions) >= 4096 {
		return nil, status.Error(codes.ResourceExhausted, "too many tunnels")
	}
	conn, err := dial()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &tunnelSession{port: in.Port, conn: conn, output: newTunnelBuffer(tunnelBufferSize), ctx: ctx, cancel: cancel}
	s.sessions[in.SessionId] = session
	session.lease = time.AfterFunc(tunnelReconnectTimeout, session.close)
	go session.read()
	return session, nil
}

func (s *tunnelSessions) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, session := range s.sessions {
		session.lease.Stop()
		session.close()
	}
	s.sessions = nil
}

func (s *tunnelSession) close() {
	s.closedAt.CompareAndSwap(0, time.Now().UnixNano())
	s.cancel()
	s.conn.Close()
	s.output.Close()
}

func (s *tunnelSession) read() {
	defer s.output.Close()
	buffer := make([]byte, tunnelChunkSize)
	for {
		n, err := s.conn.Read(buffer)
		if n > 0 {
			if appendErr := s.output.Append(s.ctx, buffer[:n]); appendErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *tunnelSession) write(offset uint64, data []byte, eof bool) (uint64, error) {
	s.inputMu.Lock()
	defer s.inputMu.Unlock()
	if offset > s.inputOffset || (len(data) > 0 && offset+uint64(len(data)) < s.inputOffset) {
		return s.inputOffset, status.Error(codes.OutOfRange, "invalid tunnel input offset")
	}
	if len(data) > 0 {
		data = data[min(s.inputOffset-offset, uint64(len(data))):]
		if len(data) > 0 && s.inputEOF {
			return s.inputOffset, status.Error(codes.FailedPrecondition, "tunnel input closed")
		}
		_ = s.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		for len(data) > 0 {
			n, err := s.conn.Write(data)
			s.inputOffset += uint64(n)
			data = data[n:]
			if err != nil {
				return s.inputOffset, err
			}
			if n == 0 {
				return s.inputOffset, io.ErrNoProgress
			}
		}
	}
	if eof && !s.inputEOF {
		if offset != s.inputOffset {
			return s.inputOffset, status.Error(codes.OutOfRange, "invalid tunnel EOF offset")
		}
		half, ok := s.conn.(interface{ CloseWrite() error })
		if !ok {
			return s.inputOffset, status.Error(codes.Unimplemented, "half close unavailable")
		}
		if err := half.CloseWrite(); err != nil {
			return s.inputOffset, err
		}
		s.inputEOF = true
	}
	return s.inputOffset, nil
}

// tunnelBuffer retains unacknowledged bytes and applies backpressure at capacity.
// Read acknowledges only its requested offset; sending a response is not an ack.
type tunnelBuffer struct {
	mu       sync.Mutex
	data     []byte
	offset   uint64
	capacity int
	closed   bool
	changed  chan struct{}
}

func newTunnelBuffer(capacity int) *tunnelBuffer {
	return &tunnelBuffer{capacity: capacity, changed: make(chan struct{})}
}

func (b *tunnelBuffer) notify() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *tunnelBuffer) Append(ctx context.Context, data []byte) error {
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

func (b *tunnelBuffer) Read(ctx context.Context, offset uint64, size int) ([]byte, bool, error) {
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

func (b *tunnelBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		b.notify()
	}
}
