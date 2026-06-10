package network

import (
	"context"
	"net"
	"strings"
	"sync"
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
