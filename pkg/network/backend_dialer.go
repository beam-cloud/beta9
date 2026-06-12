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

// dialTSNetRoute connects to the agent's tsnet proxy target. It first
// confirms the agent peer is visible in this node's netmap (a MagicDNS dial
// for an unknown peer silently falls back to the system resolver and fails
// with a misleading NXDOMAIN), then dials, retrying transient failures until
// the dial budget is exhausted so single blips never surface to callers.
func (d *BackendDialer) dialTSNetRoute(ctx context.Context, route *types.BackendRoute, routeID string, deadline time.Time) (net.Conn, error) {
	host := tailnetHostFromAddr(route.ProxyTarget)
	var lastErr error

	for remaining := time.Until(deadline); remaining > 0; remaining = time.Until(deadline) {
		if host != "" {
			// Leave headroom for the dial itself: a peer that appears at the
			// very end of the budget still needs time to handshake.
			if err := d.tailscale.WaitForPeer(ctx, host, remaining-tsnetDialReserve(remaining)); err != nil {
				return nil, fmt.Errorf("backend route %s: %w", routeID, err)
			}
			if remaining = time.Until(deadline); remaining <= 0 {
				break
			}
		}

		conn, err := d.dialTSNetTarget(ctx, route.ProxyTarget, remaining)
		if err == nil {
			return writeBackendRoutePreface(conn, routeID)
		}
		lastErr = err

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

func (d *BackendDialer) dialTSNetTarget(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	if d.tsnetDial != nil {
		return d.tsnetDial(ctx, addr, timeout)
	}
	return d.tailscale.DialContextTimeout(ctx, "tcp", addr, timeout)
}

// tsnetDialReserve is how much of the remaining dial budget is held back for
// the connect itself while waiting for the peer to appear in the netmap:
// a third of the budget, clamped to [100ms, 2s], never more than remains.
func tsnetDialReserve(remaining time.Duration) time.Duration {
	return min(max(remaining/3, 100*time.Millisecond), 2*time.Second, remaining)
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
