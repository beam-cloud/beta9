package network

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const BackendRoutePreface = "BEAMROUTE/1 "

const backendRouteDialConcurrency = 16

var backendRouteDialSlots = make(chan struct{}, backendRouteDialConcurrency)

// backendRouteReadyPollInterval is how often an "opening" route is re-resolved
// while waiting for the remote agent to confirm readiness.
var backendRouteReadyPollInterval = 250 * time.Millisecond

type BackendRouteResolver interface {
	GetBackendRoute(ctx context.Context, routeID string) (*types.BackendRoute, error)
}

type BackendDialer struct {
	tailscale *Tailscale
	tsConfig  types.TailscaleConfig
	resolver  BackendRouteResolver
	timeout   time.Duration

	// tsnetDial overrides the tsnet dial in tests.
	tsnetDial func(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error)
}

func NewBackendDialer(tailscale *Tailscale, tsConfig types.TailscaleConfig, resolver BackendRouteResolver, timeout time.Duration) *BackendDialer {
	return &BackendDialer{
		tailscale: tailscale,
		tsConfig:  tsConfig,
		resolver:  resolver,
		timeout:   timeout,
	}
}

func (d *BackendDialer) Dial(ctx context.Context, address string) (net.Conn, error) {
	timeout := d.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	routeID, ok := types.ParseBackendRouteAddress(address)
	if !ok {
		return ConnectToHost(ctx, address, timeout, d.tailscale, d.tsConfig)
	}
	if d.resolver == nil {
		return nil, fmt.Errorf("backend route resolver is required for %s", address)
	}

	deadline := time.Now().Add(timeout)
	route, err := d.resolveReadyRoute(ctx, routeID, deadline)
	if err != nil {
		return nil, err
	}

	// Spend whatever is left of the dial budget on the connect itself.
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, fmt.Errorf("backend route %s dial timed out", routeID)
	}

	switch route.Transport {
	case "", types.BackendRouteTransportDirect:
		return ConnectToHost(ctx, route.ProxyTarget, remaining, d.tailscale, d.tsConfig)
	case types.BackendRouteTransportLocalDirect:
		conn, err := ConnectToHost(ctx, route.ProxyTarget, remaining, d.tailscale, d.tsConfig)
		if err != nil {
			return nil, err
		}
		return writeBackendRoutePreface(conn, routeID)
	case types.BackendRouteTransportTSNet:
		if d.tailscale == nil {
			return nil, fmt.Errorf("tailscale dialer is unavailable for backend route %s", routeID)
		}
		return d.dialTSNetRoute(ctx, route, routeID, deadline)
	default:
		return nil, fmt.Errorf("unsupported backend route transport %q", route.Transport)
	}
}

// dialTSNetRoute connects to the agent's tsnet proxy target. A ready route is
// dialed immediately: tsnet.Status can omit reachable peers, so checking the
// netmap first only adds latency. After a failed dial, WaitForPeer enriches the
// error and feeds the rate-limited stale-netmap detector before retrying.
func (d *BackendDialer) dialTSNetRoute(ctx context.Context, route *types.BackendRoute, routeID string, deadline time.Time) (net.Conn, error) {
	host := tailnetHostFromAddr(route.ProxyTarget)
	var lastErr error

	for remaining := time.Until(deadline); remaining > 0; remaining = time.Until(deadline) {
		// A failed MagicDNS dial must still reach WaitForPeer so stale netmaps
		// are observable. Keep a bounded part of each attempt for that check.
		peerReserve := time.Duration(0)
		if host != "" {
			peerReserve = tsnetPeerProbeReserve(remaining)
		}
		dialBudget := remaining - peerReserve
		if dialBudget <= 0 {
			break
		}
		if err := acquireBackendRouteDial(ctx, dialBudget); err != nil {
			return nil, err
		}
		dialTimeout := time.Until(deadline) - peerReserve
		if dialTimeout <= 0 {
			<-backendRouteDialSlots
			return nil, context.DeadlineExceeded
		}
		conn, err := d.dialTSNetTarget(ctx, route.ProxyTarget, dialTimeout)
		<-backendRouteDialSlots
		if err == nil {
			return writeBackendRoutePreface(conn, routeID)
		}
		lastErr = err

		if host != "" {
			peerWait := min(time.Until(deadline), tailnetPeerAdvisoryTimeout)
			if peerWait > 0 {
				if peerErr := d.tailscale.WaitForPeer(ctx, host, peerWait); peerErr != nil {
					lastErr = fmt.Errorf("%w; tsnet dial failed: %w", peerErr, err)
				}
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backendRouteReadyPollInterval):
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("backend route %s dial timed out: %w", routeID, lastErr)
	}
	return nil, fmt.Errorf("backend route %s dial timed out", routeID)
}

func acquireBackendRouteDial(ctx context.Context, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case backendRouteDialSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

func (d *BackendDialer) dialTSNetTarget(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	if d.tsnetDial != nil {
		return d.tsnetDial(ctx, addr, timeout)
	}
	return d.tailscale.DialContextTimeout(ctx, "tcp", addr, timeout)
}

// tsnetPeerProbeReserve leaves enough of the route deadline to inspect the
// netmap after a failed MagicDNS dial without consuming more than half of a
// short request or the normal advisory timeout.
func tsnetPeerProbeReserve(remaining time.Duration) time.Duration {
	return min(max(remaining/3, 10*time.Millisecond), remaining/2, tailnetPeerAdvisoryTimeout)
}

// resolveReadyRoute returns the backend route once it is ready to accept
// connections. Routes are registered in the "opening" state before the remote
// agent has confirmed the proxy path, and after long idle periods the agent's
// route stream may need to reconnect before it can do so. A dial that races
// that warm-up waits for readiness within the dial budget instead of failing
// immediately; non-transient states (closing, degraded) still fail fast.
func (d *BackendDialer) resolveReadyRoute(ctx context.Context, routeID string, deadline time.Time) (*types.BackendRoute, error) {
	for {
		route, err := d.resolver.GetBackendRoute(ctx, routeID)
		if err != nil {
			return nil, err
		}
		if route == nil {
			return nil, fmt.Errorf("backend route %s not found", routeID)
		}
		if route.State == "" || route.State == types.BackendRouteStateReady {
			if route.ProxyTarget == "" {
				return nil, fmt.Errorf("backend route %s has no proxy target", routeID)
			}
			return route, nil
		}
		if route.State != types.BackendRouteStateOpening {
			return nil, fmt.Errorf("backend route %s is %s", routeID, route.State)
		}
		if time.Until(deadline) <= backendRouteReadyPollInterval {
			return nil, fmt.Errorf("backend route %s is %s", routeID, route.State)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backendRouteReadyPollInterval):
		}
	}
}

func ConnectToBackend(ctx context.Context, address string, timeout time.Duration, tailscale *Tailscale, tsConfig types.TailscaleConfig, resolver BackendRouteResolver) (net.Conn, error) {
	return NewBackendDialer(tailscale, tsConfig, resolver, timeout).Dial(ctx, address)
}

func writeBackendRoutePreface(conn net.Conn, routeID string) (net.Conn, error) {
	if _, err := fmt.Fprintf(conn, "%s%s\n", BackendRoutePreface, routeID); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// tailnetHostFromAddr extracts the bare host from a "host:port" proxy target.
func tailnetHostFromAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return strings.TrimSuffix(host, ".")
}
