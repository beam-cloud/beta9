package network

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"tailscale.com/tailcfg"
)

type agentSecurityRouteResolver struct{ route types.BackendRoute }

func (r agentSecurityRouteResolver) GetBackendRoute(context.Context, string) (*types.BackendRoute, error) {
	return &r.route, nil
}

func TestAgentRoutePinsAuthenticatedDestination(t *testing.T) {
	for _, test := range []struct{ name, reported, ip string }{
		{"ipv4", "100.64.0.10:29443", "100.64.0.10"},
		{"ipv6", "[fd7a:115c:a1e0::10]:29443", "fd7a:115c:a1e0::10"},
		{"legacy DNS", "beam-agent-private.tailnet.ts.net:29443", "100.64.0.10"},
		{"collision DNS", "beam-agent-private-1.tailnet.ts.net:29443", "100.64.0.10"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ip := netip.MustParseAddr(test.ip)
			ts := testTailscale(t, nil)
			ts.agentPeerFunc = func(context.Context, string) (netip.Addr, *tailcfg.Node, error) {
				return ip, &tailcfg.Node{Hostinfo: (&tailcfg.Hostinfo{Hostname: "beam-agent-private"}).View(), Addresses: []netip.Prefix{netip.PrefixFrom(ip, ip.BitLen())}}, nil
			}
			target, err := ts.agentRouteTarget(context.Background(), "private", test.reported)
			if err != nil || target != net.JoinHostPort(test.ip, "29443") {
				t.Fatalf("target = %q, %v", target, err)
			}
		})
	}
}

func TestAgentRouteRejectsMissingOrMismatchedIdentity(t *testing.T) {
	ip := netip.MustParseAddr("100.64.0.10")
	for name, node := range map[string]*tailcfg.Node{
		"missing node":          nil,
		"missing host identity": {Addresses: []netip.Prefix{netip.PrefixFrom(ip, 32)}},
		"wrong machine":         {Hostinfo: (&tailcfg.Hostinfo{Hostname: "beam-agent-other"}).View(), Addresses: []netip.Prefix{netip.PrefixFrom(ip, 32)}},
		"address not owned":     {Hostinfo: (&tailcfg.Hostinfo{Hostname: "beam-agent-private"}).View(), Addresses: []netip.Prefix{netip.MustParsePrefix("100.64.0.20/32")}},
		"routed subnet":         {Hostinfo: (&tailcfg.Hostinfo{Hostname: "beam-agent-private"}).View(), Addresses: []netip.Prefix{netip.MustParsePrefix("100.64.0.0/10")}},
	} {
		t.Run(name, func(t *testing.T) {
			ts := testTailscale(t, nil)
			ts.agentPeerFunc = func(context.Context, string) (netip.Addr, *tailcfg.Node, error) { return ip, node, nil }
			if _, err := ts.agentRouteTarget(context.Background(), "private", "100.64.0.10:29443"); err == nil {
				t.Fatal("unverified destination was accepted")
			}
		})
	}
}

// An agent may report only its own route, but its reported destination is still
// untrusted. The gateway must never turn that into a dial to another machine.
func TestBackendDialerRejectsForeignAgentDestination(t *testing.T) {
	for _, target := range []string{"100.64.0.20:6379", "100.64.0.20:22", "100.64.0.20:29443", "127.0.0.1:6379"} {
		t.Run(target, func(t *testing.T) {
			status := statusWithPeers("beam-agent-private", "control-plane-redis")
			for _, peer := range status.Peer {
				if peer.HostName == "beam-agent-private" {
					peer.TailscaleIPs = []netip.Addr{netip.MustParseAddr("100.64.0.10")}
				} else {
					peer.TailscaleIPs = []netip.Addr{netip.MustParseAddr("100.64.0.20")}
				}
			}
			resolver := agentSecurityRouteResolver{route: types.BackendRoute{RouteID: "private-worker", MachineID: "private", Transport: types.BackendRouteTransportTSNet, State: types.BackendRouteStateReady, ProxyTarget: target}}
			dialer := NewBackendDialer(testTailscale(t, status), types.TailscaleConfig{Enabled: true}, resolver, 25*time.Millisecond)
			foreignDial := false
			dialer.tsnetDial = func(_ context.Context, addr string, _ time.Duration) (net.Conn, error) {
				if addr == target {
					foreignDial = true
				}
				return nil, context.DeadlineExceeded
			}
			_, _ = dialer.Dial(context.Background(), types.BackendRouteAddress("private-worker"))
			if foreignDial {
				t.Fatalf("private agent caused the gateway to dial foreign destination %s", target)
			}
		})
	}
}
