package agent

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

type fakeTSNetStatusClient struct {
	status *ipnstate.Status
	err    error
	calls  []string
}

func (c *fakeTSNetStatusClient) Status(context.Context) (*ipnstate.Status, error) {
	c.calls = append(c.calls, "full")
	return c.status, c.err
}

func (c *fakeTSNetStatusClient) StatusWithoutPeers(context.Context) (*ipnstate.Status, error) {
	c.calls = append(c.calls, "light")
	return c.status, c.err
}

type fakeAgentPoolVirtualizationClient struct {
	resp       *pb.GetAgentPoolVirtualizationResponse
	err        error
	agentToken string
}

func (c *fakeAgentPoolVirtualizationClient) GetAgentPoolVirtualization(ctx context.Context, in *pb.GetAgentPoolVirtualizationRequest, _ ...grpc.CallOption) (*pb.GetAgentPoolVirtualizationResponse, error) {
	c.agentToken = in.GetAgentToken()
	if c.err != nil {
		return nil, c.err
	}
	return c.resp, nil
}

func TestRequestAgentPoolGPUVirtualizedUsesAgentTokenRPC(t *testing.T) {
	client := &fakeAgentPoolVirtualizationClient{resp: &pb.GetAgentPoolVirtualizationResponse{Ok: true, GpuVirtualized: true}}

	got, err := requestAgentPoolGPUVirtualized(context.Background(), client, "agent-token")
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("gpu virtualized = false, want true")
	}
	if client.agentToken != "agent-token" {
		t.Fatalf("agent token = %q", client.agentToken)
	}
}

func TestRequestAgentPoolGPUVirtualizedReturnsRPCError(t *testing.T) {
	client := &fakeAgentPoolVirtualizationClient{resp: &pb.GetAgentPoolVirtualizationResponse{Ok: false, ErrMsg: "invalid agent token"}}

	_, err := requestAgentPoolGPUVirtualized(context.Background(), client, "agent-token")
	if err == nil || err.Error() != "invalid agent token" {
		t.Fatalf("requestAgentPoolGPUVirtualized() error = %v", err)
	}
}

func TestLogThunderNodeEnrollmentSkippedWritesStructuredWarningToProvidedWriter(t *testing.T) {
	var stderr bytes.Buffer
	logThunderNodeEnrollmentSkipped(&stderr, errors.New("gateway unavailable"))

	got := stderr.String()
	if !strings.Contains(got, `"level":"warn"`) {
		t.Fatalf("missing warning level: %q", got)
	}
	if !strings.Contains(got, `"error":"gateway unavailable"`) {
		t.Fatalf("missing error field: %q", got)
	}
	if !strings.Contains(got, `"message":"Thunder node enrollment skipped"`) {
		t.Fatalf("missing message field: %q", got)
	}
}

func TestTSNetSnapshotSuppressesTransientTimeouts(t *testing.T) {
	telemetry := newAgentTelemetry(nil, "", bootstrapConfig{}, "", nil)
	client := &fakeTSNetStatusClient{err: context.DeadlineExceeded}
	reporter := &tsnetSnapshotReporter{
		telemetry:   telemetry,
		client:      client,
		proxyTarget: "agent.test:29443",
	}

	reporter.emit(context.Background())
	assertNoAgentTelemetry(t, telemetry)
	reporter.emit(context.Background())
	assertNoAgentTelemetry(t, telemetry)

	reporter.emit(context.Background())
	record := readAgentEvent(t, telemetry)
	if record.Status != types.BackendRouteStateDegraded {
		t.Fatalf("expected degraded status, got %q", record.Status)
	}
	if record.Message != "transport snapshot timed out" {
		t.Fatalf("expected normalized timeout message, got %q", record.Message)
	}
	if record.Attrs["error_kind"] != "deadline_exceeded" {
		t.Fatalf("expected deadline error kind, got %q", record.Attrs["error_kind"])
	}
}

func TestTSNetSnapshotDoesNotEmitCanceledProbe(t *testing.T) {
	telemetry := newAgentTelemetry(nil, "", bootstrapConfig{}, "", nil)
	client := &fakeTSNetStatusClient{err: context.Canceled}
	reporter := &tsnetSnapshotReporter{
		telemetry:   telemetry,
		client:      client,
		proxyTarget: "agent.test:29443",
	}

	for i := 0; i < tsnetSnapshotFailureThreshold+1; i++ {
		reporter.emit(context.Background())
	}
	assertNoAgentTelemetry(t, telemetry)
}

func TestTSNetSnapshotUsesLightweightStatusBetweenFullSnapshots(t *testing.T) {
	telemetry := newAgentTelemetry(nil, "", bootstrapConfig{}, "", nil)
	client := &fakeTSNetStatusClient{status: &ipnstate.Status{BackendState: "Running"}}
	reporter := &tsnetSnapshotReporter{
		telemetry:   telemetry,
		client:      client,
		proxyTarget: "agent.test:29443",
	}

	for i := 0; i < tsnetFullSnapshotEvery; i++ {
		reporter.emit(context.Background())
		_ = readAgentEvent(t, telemetry)
	}

	want := []string{"full", "light", "light", "light", "full"}
	if !stringSlicesEqual(client.calls, want) {
		t.Fatalf("unexpected status calls: got %v want %v", client.calls, want)
	}
}

func TestTSNetSnapshotFailureClassifiesWrappedDeadline(t *testing.T) {
	kind, message := tsnetSnapshotFailure(errors.New("localapi status: context deadline exceeded"))
	if kind != "deadline_exceeded" || message != "transport snapshot timed out" {
		t.Fatalf("unexpected classification: kind=%q message=%q", kind, message)
	}
}

func readAgentEvent(t *testing.T, telemetry *agentTelemetry) *pb.AgentEventRecord {
	t.Helper()
	select {
	case req := <-telemetry.ch:
		if len(req.Events) != 1 {
			t.Fatalf("expected one event, got %d", len(req.Events))
		}
		return req.Events[0]
	default:
		t.Fatal("expected telemetry event")
		return nil
	}
}

func assertNoAgentTelemetry(t *testing.T, telemetry *agentTelemetry) {
	t.Helper()
	select {
	case req := <-telemetry.ch:
		t.Fatalf("unexpected telemetry request: %+v", req)
	default:
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestTailnetRouteHostPrefersTailnetIPv4(t *testing.T) {
	status := &ipnstate.Status{
		Self:         &ipnstate.PeerStatus{DNSName: "beam-agent-machine-1.tailnet.ts.net."},
		TailscaleIPs: []netip.Addr{netip.MustParseAddr("fd7a:115c:a1e0::1"), netip.MustParseAddr("100.64.0.7")},
	}
	if got := tailnetRouteHost(status, "fallback"); got != "100.64.0.7" {
		t.Fatalf("route host = %q, want the tailnet IPv4", got)
	}
	status.TailscaleIPs = status.TailscaleIPs[:1]
	if got := tailnetRouteHost(status, "fallback"); got != "fd7a:115c:a1e0::1" {
		t.Fatalf("route host = %q, want the tailnet IPv6 when there is no IPv4", got)
	}
	status.TailscaleIPs = nil
	if got := tailnetRouteHost(status, "fallback"); got != "beam-agent-machine-1.tailnet.ts.net" {
		t.Fatalf("route host = %q, want the MagicDNS name when the node has no IPs yet", got)
	}
	if got := tailnetRouteHost(nil, "fallback"); got != "fallback" {
		t.Fatalf("route host = %q, want the requested hostname without a status", got)
	}
}

func TestHostInterfacesHideWorkerLinks(t *testing.T) {
	ifaces, err := hostInterfacesWithoutWorkerLinks()
	require.NoError(t, err)
	for _, iface := range ifaces {
		require.False(t, strings.HasPrefix(iface.Name, types.WorkerLinkPrefix), iface.Name)
	}
	all, err := net.Interfaces()
	require.NoError(t, err)
	shown := 0
	for _, iface := range all {
		if !strings.HasPrefix(iface.Name, types.WorkerLinkPrefix) {
			shown++
		}
	}
	require.Len(t, ifaces, shown)
}

type fakeTailnetPinger struct {
	status *ipnstate.Status
	mu     sync.Mutex
	pinged []netip.Addr
}

func (p *fakeTailnetPinger) Status(context.Context) (*ipnstate.Status, error) {
	return p.status, nil
}

func (p *fakeTailnetPinger) Ping(_ context.Context, ip netip.Addr, pingType tailcfg.PingType) (*ipnstate.PingResult, error) {
	if pingType != tailcfg.PingTSMP {
		return nil, errors.New("want a TSMP ping, which goes through WireGuard")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pinged = append(p.pinged, ip)
	return &ipnstate.PingResult{}, nil
}

func TestPingTailnetGatewaysPingsOnlyOnlineGateways(t *testing.T) {
	gateway := netip.MustParseAddr("100.64.0.2")
	pinger := &fakeTailnetPinger{status: &ipnstate.Status{Peer: map[key.NodePublic]*ipnstate.PeerStatus{
		key.NewNode().Public(): {HostName: "beam-gateway-beta9-gateway-1", Online: true, TailscaleIPs: []netip.Addr{gateway, netip.MustParseAddr("fd7a:115c:a1e0::2")}},
		key.NewNode().Public(): {HostName: "beam-gateway-beta9-gateway-2", Online: false, TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.3")}},
		key.NewNode().Public(): {HostName: "beam-gateway-beta9-gateway-3", Online: true},
		key.NewNode().Public(): {HostName: "beam-agent-machine-2", Online: true, TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.4")}},
		key.NewNode().Public(): {HostName: "beam-gatewayish", Online: true, TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.5")}},
	}}}

	pingTailnetGateways(context.Background(), pinger, &bytes.Buffer{})

	require.Equal(t, []netip.Addr{gateway}, pinger.pinged)
}
