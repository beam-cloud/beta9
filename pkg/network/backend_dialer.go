package network

import (
	"context"
	"fmt"
	"net"
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
		conn, err := d.tailscale.DialContextTimeout(ctx, "tcp", route.ProxyTarget, remaining)
		if err != nil {
			return nil, err
		}
		return writeBackendRoutePreface(conn, routeID)
	default:
		return nil, fmt.Errorf("unsupported backend route transport %q", route.Transport)
	}
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
