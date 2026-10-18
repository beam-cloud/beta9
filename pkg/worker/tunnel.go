package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime"
	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const tunnelReconnectTimeout = 2 * time.Minute

// Tunnel reads and writes use the same request journal as sandbox process I/O.
// Only the TCP connection and its reconnection lease belong to this collection.
type tunnelSessions struct {
	mu       sync.Mutex
	sessions map[string]*tunnelSession
	pending  map[string]*tunnelDial
	closed   bool
}

type tunnelDial struct {
	port    uint32
	done    chan struct{}
	cancel  context.CancelFunc
	session *tunnelSession
	err     error
}

type tunnelSession struct {
	port     uint32
	conn     net.Conn
	lease    *time.Timer
	closedAt atomic.Int64
}

func (s *tunnelSession) close() {
	s.closedAt.CompareAndSwap(0, time.Now().UnixNano())
	s.conn.Close()
}

func (s *ContainerRuntimeServer) ContainerTunnel(ctx context.Context, in *pb.ContainerTunnelRequest) (*pb.ContainerTunnelResponse, error) {
	if _, err := uuid.Parse(in.SessionId); err != nil || in.Port == 0 || in.Port > 65535 || len(in.Input) > 64<<10 {
		return nil, status.Error(codes.InvalidArgument, "invalid tunnel request")
	}
	instance, exists := s.containerInstances.Get(in.ContainerId)
	if !exists {
		return nil, status.Error(codes.NotFound, "container not found")
	}
	session, err := instance.tunnels.get(ctx, in, func(ctx context.Context) (net.Conn, error) {
		address := instance.containerAddress(int32(in.Port))
		if address == "" {
			return nil, status.Error(codes.FailedPrecondition, "port is not bound")
		}
		if guest, ok := instance.Runtime.(runtime.GuestPortDialer); ok {
			conn, err := guest.DialGuestPort(ctx, in.ContainerId, in.Port)
			if !errors.Is(err, microvm.ErrPortUnavailable) {
				return conn, err
			}
			// A listener bound only to the NIC refuses the loopback connection.
		}
		return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", address)
	})
	if err != nil {
		return nil, err
	}
	response := &pb.ContainerTunnelResponse{}
	switch {
	case in.Close:
		session.close()
	case in.Read:
		session.conn.SetReadDeadline(time.Now().Add(20 * time.Second))
		buffer := make([]byte, 64<<10)
		n, readErr := session.conn.Read(buffer)
		response.Output = buffer[:n]
		response.OutputEof = n == 0 && errors.Is(readErr, io.EOF)
		var timeout net.Error
		if readErr != nil && !errors.Is(readErr, io.EOF) && !(errors.As(readErr, &timeout) && timeout.Timeout()) {
			session.close()
			return nil, readErr
		}
	case in.InputEof:
		half, ok := session.conn.(interface{ CloseWrite() error })
		if !ok {
			return nil, status.Error(codes.Unimplemented, "half close unavailable")
		}
		err = half.CloseWrite()
	case len(in.Input) > 0:
		session.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		_, err = io.Copy(session.conn, bytes.NewReader(in.Input))
	}
	if err != nil {
		session.close()
	}
	return response, err
}

func (s *tunnelSessions) get(ctx context.Context, in *pb.ContainerTunnelRequest, dial func(context.Context) (net.Conn, error)) (*tunnelSession, error) {
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
			if time.Since(time.Unix(0, closed)) > requestReplayTTL {
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
		if session.closedAt.Load() != 0 {
			return nil, status.Error(codes.NotFound, "tunnel expired")
		}
		session.lease.Reset(tunnelReconnectTimeout)
		return session, nil
	}
	if pending := s.pending[in.SessionId]; pending != nil {
		if pending.port != in.Port {
			return nil, status.Error(codes.InvalidArgument, "tunnel port changed")
		}
		s.mu.Unlock()
		select {
		case <-pending.done:
		case <-ctx.Done():
		}
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if pending.err == nil {
			if pending.session.closedAt.Load() != 0 {
				return nil, status.Error(codes.NotFound, "tunnel expired")
			}
			pending.session.lease.Reset(tunnelReconnectTimeout)
		}
		return pending.session, pending.err
	}
	if !in.Create {
		return nil, status.Error(codes.NotFound, "tunnel not found")
	}
	if active+len(s.pending) >= 8 || len(s.sessions)+len(s.pending) >= requestJournalLimit {
		return nil, status.Error(codes.ResourceExhausted, "too many tunnels")
	}
	dialCtx, cancel := context.WithCancel(ctx)
	pending := &tunnelDial{port: in.Port, done: make(chan struct{}), cancel: cancel}
	if s.pending == nil {
		s.pending = make(map[string]*tunnelDial)
	}
	s.pending[in.SessionId] = pending
	s.mu.Unlock()
	conn, err := dial(dialCtx)
	cancel()
	s.mu.Lock()
	delete(s.pending, in.SessionId)
	if s.closed && err == nil {
		conn.Close()
		err = status.Error(codes.NotFound, "container stopped")
	}
	if err == nil {
		session := &tunnelSession{port: in.Port, conn: conn}
		session.lease = time.AfterFunc(tunnelReconnectTimeout, session.close)
		s.sessions[in.SessionId] = session
		pending.session = session
	}
	pending.err = err
	close(pending.done)
	return pending.session, err
}

func (s *tunnelSessions) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, pending := range s.pending {
		pending.cancel()
	}
	for _, session := range s.sessions {
		session.lease.Stop()
		session.close()
	}
	s.sessions = nil
}
