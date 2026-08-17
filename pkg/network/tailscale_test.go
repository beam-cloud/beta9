package network

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/types/key"
)

func statusWithPeers(hosts ...string) *ipnstate.Status {
	status := &ipnstate.Status{
		Peer: map[key.NodePublic]*ipnstate.PeerStatus{},
	}
	for _, host := range hosts {
		status.Peer[key.NewNode().Public()] = &ipnstate.PeerStatus{
			HostName: host,
			DNSName:  host + ".tailnet.ts.net.",
		}
	}
	return status
}

func testTailscale(t *testing.T, status *ipnstate.Status) *Tailscale {
	t.Helper()
	ts := newTailscale(TailscaleConfig{Hostname: "test-node"}, nil)
	ts.initialized = true
	ts.statusFunc = func(ctx context.Context) (*ipnstate.Status, error) {
		return status, nil
	}
	return ts
}

func withFastPeerPolling(t *testing.T) {
	t.Helper()
	prev := tailnetPeerPollInterval
	tailnetPeerPollInterval = time.Millisecond
	t.Cleanup(func() { tailnetPeerPollInterval = prev })
}

func TestConnectToHostRetriesDialWhenPeerIsMissingFromNetmap(t *testing.T) {
	withFastPeerPolling(t)

	ts := testTailscale(t, statusWithPeers())
	dialAttempts := 0
	var peer net.Conn
	ts.dialFunc = func(context.Context, string, string) (net.Conn, error) {
		dialAttempts++
		if dialAttempts == 1 {
			return nil, errors.New("initial dial failed")
		}
		client, server := net.Pipe()
		peer = server
		return client, nil
	}

	conn, err := ConnectToHost(
		context.Background(),
		"worker.tailnet:2222",
		500*time.Millisecond,
		ts,
		types.TailscaleConfig{Enabled: true, HostName: "tailnet"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	defer peer.Close()

	if dialAttempts != 2 {
		t.Fatalf("dial attempts = %d, want initial dial plus retry", dialAttempts)
	}
}

func TestWaitForPeerFindsPeerByDNSName(t *testing.T) {
	withFastPeerPolling(t)
	ts := testTailscale(t, statusWithPeers("beam-agent-machine"))

	if err := ts.WaitForPeer(context.Background(), "beam-agent-machine.tailnet.ts.net", 100*time.Millisecond); err != nil {
		t.Fatalf("WaitForPeer() error = %v, want nil", err)
	}
}

func TestWaitForPeerSkipsIPLiterals(t *testing.T) {
	withFastPeerPolling(t)
	// No peers at all: an IP target must not be blocked on netmap visibility.
	ts := testTailscale(t, statusWithPeers())

	for _, host := range []string{"100.71.206.108", "fd7a:115c:a1e0::3233:ce6d"} {
		if err := ts.WaitForPeer(context.Background(), host, 10*time.Millisecond); err != nil {
			t.Fatalf("WaitForPeer(%q) error = %v, want nil for IP literal", host, err)
		}
	}
}

func TestWaitForPeerMatchesSelf(t *testing.T) {
	withFastPeerPolling(t)
	status := statusWithPeers()
	status.Self = &ipnstate.PeerStatus{HostName: "test-node", DNSName: "test-node.tailnet.ts.net."}
	ts := testTailscale(t, status)

	if err := ts.WaitForPeer(context.Background(), "test-node.tailnet.ts.net", 50*time.Millisecond); err != nil {
		t.Fatalf("WaitForPeer() error = %v, want nil", err)
	}
}

func TestWaitForPeerMissingPeerReturnsClearError(t *testing.T) {
	withFastPeerPolling(t)
	ts := testTailscale(t, statusWithPeers("some-other-node"))

	err := ts.WaitForPeer(context.Background(), "beam-agent-missing.tailnet.ts.net", 10*time.Millisecond)
	if err == nil {
		t.Fatal("WaitForPeer() error = nil, want netmap error")
	}
	if !strings.Contains(err.Error(), "netmap") {
		t.Fatalf("WaitForPeer() error = %v, want mention of netmap", err)
	}
}

func TestWaitForPeerReturnsCallerCancellationWithoutReplacingServer(t *testing.T) {
	withFastPeerPolling(t)
	ts := testTailscale(t, statusWithPeers())
	originalServer := ts.currentServer()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ts.WaitForPeer(ctx, "beam-agent-missing", time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForPeer() error = %v, want context.Canceled", err)
	}
	if ts.currentServer() != originalServer {
		t.Fatal("caller cancellation replaced the shared tsnet server")
	}
}

func TestWaitForPeerBoundsSlowStatusToAdvisoryTimeout(t *testing.T) {
	ts := testTailscale(t, nil)
	ts.statusFunc = func(ctx context.Context) (*ipnstate.Status, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
			return statusWithPeers(), nil
		}
	}

	start := time.Now()
	err := ts.WaitForPeer(context.Background(), "beam-agent-missing", 20*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "netmap status unavailable") {
		t.Fatalf("WaitForPeer() error = %v, want bounded netmap status error", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("WaitForPeer() took %s, want less than 100ms", elapsed)
	}
}

func TestRepeatedCleanPeerMissesPreserveHealthyPeerRouting(t *testing.T) {
	withFastPeerPolling(t)
	ts := testTailscale(t, statusWithPeers("beam-agent-healthy"))
	originalServer := ts.currentServer()

	for i := 0; i < 10; i++ {
		_ = ts.WaitForPeer(context.Background(), "beam-agent-missing", time.Millisecond)
		if err := ts.WaitForPeer(context.Background(), "beam-agent-healthy", 10*time.Millisecond); err != nil {
			t.Fatalf("healthy peer lookup %d failed after missing peer: %v", i, err)
		}
	}

	if ts.currentServer() != originalServer {
		t.Fatal("server was recycled for a peer absent from a healthy netmap")
	}
}

func TestRepeatedStatusFailuresDoNotReplaceSharedServer(t *testing.T) {
	withFastPeerPolling(t)
	ts := testTailscale(t, nil)
	ts.statusFunc = func(context.Context) (*ipnstate.Status, error) {
		return nil, errors.New("tailnet status unavailable")
	}
	originalServer := ts.currentServer()

	for i := 0; i < 10; i++ {
		_ = ts.WaitForPeer(context.Background(), "beam-agent-missing", time.Millisecond)
	}

	if ts.currentServer() != originalServer {
		t.Fatal("netmap status failures replaced the shared tsnet server")
	}
}

func TestConcurrentPeerChecksDoNotMutateSharedServer(t *testing.T) {
	withFastPeerPolling(t)
	ts := testTailscale(t, statusWithPeers("beam-agent-healthy"))
	originalServer := ts.currentServer()

	const missingChecks = 32
	const healthyChecks = 128
	errs := make(chan error, healthyChecks)
	var checks sync.WaitGroup
	checks.Add(missingChecks + healthyChecks)
	for range missingChecks {
		go func() {
			defer checks.Done()
			_ = ts.WaitForPeer(context.Background(), "beam-agent-removed", time.Millisecond)
		}()
	}
	for range healthyChecks {
		go func() {
			defer checks.Done()
			if err := ts.WaitForPeer(context.Background(), "beam-agent-healthy", 10*time.Millisecond); err != nil {
				errs <- err
			}
		}()
	}
	checks.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("healthy peer lookup failed during missing-peer storm: %v", err)
	}
	if ts.currentServer() != originalServer {
		t.Fatal("concurrent peer checks replaced the shared tsnet server")
	}
}
