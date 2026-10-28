package pod

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	// How long readiness waits for a relay or port proxy to hang up.
	readinessHoldMin time.Duration = time.Millisecond * 20
	readinessHoldMax time.Duration = time.Millisecond * 250
)

// backendTarget is one container port as the proxy reaches it. A routed target
// sits behind an agent relay or worker port proxy, which accepts a connection
// before the app does and hangs up if the app isn't listening.
type backendTarget struct {
	address string
	routed  bool
}

func newBackendTarget(address string) backendTarget {
	_, routed := types.ParseBackendRouteAddress(address)
	return backendTarget{address: address, routed: routed}
}

// ready reports whether a fresh connection reached the app: for a routed
// target, the relay kept it open or the app spoke first.
func (t backendTarget) ready(conn net.Conn, dialLatency time.Duration) bool {
	if !t.routed {
		return true
	}
	hold := min(max(2*dialLatency, readinessHoldMin), readinessHoldMax)
	if err := conn.SetReadDeadline(time.Now().Add(hold)); err != nil {
		return true
	}
	var b [1]byte
	n, err := conn.Read(b[:])
	var netErr net.Error
	return n > 0 || errors.As(err, &netErr) && netErr.Timeout()
}

// unanswered reports whether a failed request never reached the app, so it can
// be sent again: the dial failed, or a routed target hung up on a bodiless
// idempotent request without sending anything back.
func (t backendTarget) unanswered(req *http.Request, err error, attempt *backendAttempt) bool {
	if req.Context().Err() != nil {
		return false
	}
	var dialErr *backendDialError
	if errors.As(err, &dialErr) {
		return true
	}
	return t.routed && idempotentWithoutBody(req) && attempt.hungUp()
}

func idempotentWithoutBody(req *http.Request) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return req.ContentLength == 0 && (req.Body == nil || req.Body == http.NoBody)
	}
	return false
}

// dialBackend connects to a target and records the dial under phase.
func (pb *PodProxyBuffer) dialBackend(ctx context.Context, target backendTarget, timeout time.Duration, phase string) (*backendConn, error) {
	start := time.Now()
	conn, err := network.ConnectToBackend(ctx, target.address, timeout, pb.tailscale, pb.tsConfig, pb.containerRepo)
	metrics.RecordProxyBackendDialLatency("pod", pb.workspaceName(), pb.stubId, phase, err == nil, time.Since(start))
	if err != nil {
		return nil, &backendDialError{err: err}
	}
	return &backendConn{Conn: conn}, nil
}

// backendConn records what the backend sent and whether the connection failed.
type backendConn struct {
	net.Conn
	received atomic.Int64
	failed   atomic.Bool
}

func (c *backendConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.received.Add(int64(n))
	c.noteFailure(err)
	return n, err
}

func (c *backendConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.noteFailure(err)
	return n, err
}

func (c *backendConn) noteFailure(err error) {
	var netErr net.Error
	if err != nil && !(errors.As(err, &netErr) && netErr.Timeout()) {
		c.failed.Store(true)
	}
}

// backendAttempt tracks the connection one request went out on.
type backendAttempt struct {
	conn     *backendConn
	received int64
}

func (a *backendAttempt) trace(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if conn, ok := info.Conn.(*backendConn); ok {
				a.conn, a.received = conn, conn.received.Load()
			}
		},
	})
}

// hungUp reports whether the connection failed before the backend sent
// anything in reply.
func (a *backendAttempt) hungUp() bool {
	return a.conn != nil && a.conn.failed.Load() && a.conn.received.Load() == a.received
}
