package network

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

// fakeRouteResolver returns one state per call; the last state repeats.
type fakeRouteResolver struct {
	mu          sync.Mutex
	states      []string
	calls       int
	proxyTarget string
}

func (f *fakeRouteResolver) GetBackendRoute(ctx context.Context, routeID string) (*types.BackendRoute, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	idx := f.calls
	if idx >= len(f.states) {
		idx = len(f.states) - 1
	}
	f.calls++

	route := &types.BackendRoute{
		RouteID:   routeID,
		State:     f.states[idx],
		Transport: types.BackendRouteTransportDirect,
	}
	if route.State == types.BackendRouteStateReady {
		route.ProxyTarget = f.proxyTarget
	}
	return route, nil
}

func (f *fakeRouteResolver) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func withFastRoutePolling(t *testing.T) {
	t.Helper()
	previous := backendRouteReadyPollInterval
	backendRouteReadyPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { backendRouteReadyPollInterval = previous })
}

func withFastPeerAdvisory(t *testing.T) {
	t.Helper()
	previous := tailnetPeerAdvisoryTimeout
	tailnetPeerAdvisoryTimeout = 5 * time.Millisecond
	t.Cleanup(func() { tailnetPeerAdvisoryTimeout = previous })
}

// A dial that races route warm-up (cold start or agent stream reconnect after
// idle) must wait for readiness within its budget instead of failing with
// "backend route ... is opening".
func TestBackendDialerWaitsForOpeningRoute(t *testing.T) {
	withFastRoutePolling(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	resolver := &fakeRouteResolver{
		states: []string{
			types.BackendRouteStateOpening,
			types.BackendRouteStateOpening,
			types.BackendRouteStateReady,
		},
		proxyTarget: listener.Addr().String(),
	}

	conn, err := ConnectToBackend(context.Background(), types.BackendRouteAddress("route-a"), 2*time.Second, nil, types.TailscaleConfig{}, resolver)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	_ = conn.Close()

	if calls := resolver.callCount(); calls < 3 {
		t.Fatalf("resolver calls = %d, want >= 3", calls)
	}
}

func TestBackendDialerOpeningRouteTimesOut(t *testing.T) {
	withFastRoutePolling(t)

	resolver := &fakeRouteResolver{states: []string{types.BackendRouteStateOpening}}

	_, err := ConnectToBackend(context.Background(), types.BackendRouteAddress("route-a"), 100*time.Millisecond, nil, types.TailscaleConfig{}, resolver)
	if err == nil {
		t.Fatal("dial succeeded, want timeout error")
	}
	if !strings.Contains(err.Error(), "is opening") {
		t.Fatalf("dial error = %v, want 'is opening'", err)
	}
	if calls := resolver.callCount(); calls < 2 {
		t.Fatalf("resolver calls = %d, want >= 2 (should poll before giving up)", calls)
	}
}

func TestBackendDialerFailsFastForNonTransientStates(t *testing.T) {
	withFastRoutePolling(t)

	for _, state := range []string{types.BackendRouteStateDegraded, types.BackendRouteStateClosing} {
		resolver := &fakeRouteResolver{states: []string{state}}

		start := time.Now()
		_, err := ConnectToBackend(context.Background(), types.BackendRouteAddress("route-a"), 2*time.Second, nil, types.TailscaleConfig{}, resolver)
		if err == nil {
			t.Fatalf("dial succeeded for %s route, want error", state)
		}
		if !strings.Contains(err.Error(), state) {
			t.Fatalf("dial error = %v, want state %q", err, state)
		}
		if calls := resolver.callCount(); calls != 1 {
			t.Fatalf("resolver calls = %d, want 1 (no polling for %s)", calls, state)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("dial took %s, want fail-fast", elapsed)
		}
	}
}

// tsnetRouteResolver always returns a ready tsnet route.
type tsnetRouteResolver struct {
	proxyTarget string
}

func (f *tsnetRouteResolver) GetBackendRoute(ctx context.Context, routeID string) (*types.BackendRoute, error) {
	return &types.BackendRoute{
		RouteID:     routeID,
		State:       types.BackendRouteStateReady,
		Transport:   types.BackendRouteTransportTSNet,
		ProxyTarget: f.proxyTarget,
	}, nil
}

func TestBackendDialerTSNetRetriesTransientDialFailures(t *testing.T) {
	withFastRoutePolling(t)
	withFastPeerPolling(t)

	ts := testTailscale(t, statusWithPeers("beam-agent-machine"))
	resolver := &tsnetRouteResolver{proxyTarget: "beam-agent-machine.tailnet.ts.net:29443"}

	var attempts atomic.Int32
	serverSide := make(chan net.Conn, 1)
	dialer := NewBackendDialer(ts, types.TailscaleConfig{Enabled: true}, resolver, 2*time.Second)
	dialer.tsnetDial = func(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
		if attempts.Add(1) < 3 {
			return nil, context.DeadlineExceeded
		}
		client, server := net.Pipe()
		serverSide <- server
		return client, nil
	}

	done := make(chan error, 1)
	go func() {
		conn, err := dialer.Dial(context.Background(), types.BackendRouteAddress("route-ts"))
		if conn != nil {
			defer conn.Close()
		}
		done <- err
	}()

	// The preface is written synchronously on the pipe; consume it.
	var server net.Conn
	select {
	case server = <-serverSide:
	case err := <-done:
		t.Fatalf("dial finished early: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("dial never reached a successful attempt")
	}
	buf := make([]byte, 64)
	n, err := server.Read(buf)
	if err != nil {
		t.Fatalf("read preface: %v", err)
	}
	if got := string(buf[:n]); !strings.HasPrefix(got, BackendRoutePreface+"route-ts") {
		t.Fatalf("preface = %q, want prefix %q", got, BackendRoutePreface+"route-ts")
	}
	if err := <-done; err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("dial attempts = %d, want 3 (two transient failures retried)", got)
	}
}

// newMissingPeerBackendDialer builds a dialer whose tsnet route targets a peer
// that is absent from the tailscale netmap, with fast polling for tests.
// Shared by the netmap-miss tests below.
func newMissingPeerBackendDialer(t *testing.T, dialTimeout time.Duration) (*Tailscale, *BackendDialer) {
	t.Helper()
	withFastRoutePolling(t)
	withFastPeerPolling(t)
	withFastPeerAdvisory(t)

	ts := testTailscale(t, statusWithPeers("some-other-agent"))
	resolver := &tsnetRouteResolver{proxyTarget: "beam-agent-missing.tailnet.ts.net:29443"}
	return ts, NewBackendDialer(ts, types.TailscaleConfig{Enabled: true}, resolver, dialTimeout)
}

func TestBackendDialerTSNetDialsWhenPeerMissingFromStatus(t *testing.T) {
	_, dialer := newMissingPeerBackendDialer(t, 300*time.Millisecond)
	serverSide := make(chan net.Conn, 1)
	dialer.tsnetDial = func(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
		client, server := net.Pipe()
		serverSide <- server
		return client, nil
	}

	done := make(chan error, 1)
	go func() {
		conn, err := dialer.Dial(context.Background(), types.BackendRouteAddress("route-ts"))
		if conn != nil {
			defer conn.Close()
		}
		done <- err
	}()

	var server net.Conn
	select {
	case server = <-serverSide:
	case err := <-done:
		t.Fatalf("dial finished early: %v", err)
	case <-time.After(time.Second):
		t.Fatal("dial was not attempted after netmap miss")
	}
	buf := make([]byte, 64)
	if _, err := server.Read(buf); err != nil {
		t.Fatalf("read preface: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("dial failed: %v", err)
	}
}

func TestBackendDialerTSNetIncludesPeerMissWhenDialFails(t *testing.T) {
	_, dialer := newMissingPeerBackendDialer(t, 250*time.Millisecond)
	dialer.tsnetDial = func(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
		return nil, context.DeadlineExceeded
	}

	_, err := dialer.Dial(context.Background(), types.BackendRouteAddress("route-ts"))
	if err == nil {
		t.Fatal("dial succeeded, want error")
	}
	if !strings.Contains(err.Error(), "netmap") || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("dial error = %v, want netmap miss and dial failure", err)
	}
}

// A dead peer (offline seller machine) produces netmap miss + NXDOMAIN on
// every dial. That must feed the rate-limited stale-netmap detector only —
// recycling the shared tsnet server per dial would drop every other route.
func TestBackendDialerTSNetDeadPeerDoesNotRecycleServer(t *testing.T) {
	ts, dialer := newMissingPeerBackendDialer(t, 250*time.Millisecond)
	originalServer := ts.currentServer()
	dialer.tsnetDial = func(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
		return nil, &net.DNSError{Err: "no such host", Name: "beam-agent-missing", IsNotFound: true}
	}

	_, err := dialer.Dial(context.Background(), types.BackendRouteAddress("route-ts"))
	if err == nil {
		t.Fatal("dial succeeded, want error")
	}
	if ts.currentServer() != originalServer {
		t.Fatal("a single dead peer must not recycle the shared tsnet server")
	}
}

func TestBackendDialerTSNetExhaustsBudgetWithLastError(t *testing.T) {
	withFastRoutePolling(t)
	withFastPeerPolling(t)

	ts := testTailscale(t, statusWithPeers("beam-agent-machine"))
	resolver := &tsnetRouteResolver{proxyTarget: "beam-agent-machine.tailnet.ts.net:29443"}

	dialer := NewBackendDialer(ts, types.TailscaleConfig{Enabled: true}, resolver, 200*time.Millisecond)
	dialer.tsnetDial = func(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
		return nil, context.DeadlineExceeded
	}

	_, err := dialer.Dial(context.Background(), types.BackendRouteAddress("route-ts"))
	if err == nil {
		t.Fatal("dial succeeded, want timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("dial error = %v, want timeout wrapping the last dial error", err)
	}
}

func TestBackendDialerReadyRouteDialsImmediately(t *testing.T) {
	withFastRoutePolling(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		_ = conn.Close()
	}()

	resolver := &fakeRouteResolver{
		states:      []string{types.BackendRouteStateReady},
		proxyTarget: listener.Addr().String(),
	}

	conn, err := ConnectToBackend(context.Background(), types.BackendRouteAddress("route-a"), 2*time.Second, nil, types.TailscaleConfig{}, resolver)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	_ = conn.Close()

	if calls := resolver.callCount(); calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", calls)
	}
}
