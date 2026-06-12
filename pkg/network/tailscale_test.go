package network

import (
	"context"
	"strings"
	"testing"
	"time"

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

func TestWaitForPeerFindsPeerByDNSName(t *testing.T) {
	withFastPeerPolling(t)
	ts := testTailscale(t, statusWithPeers("beam-agent-machine"))

	if err := ts.WaitForPeer(context.Background(), "beam-agent-machine.tailnet.ts.net", 100*time.Millisecond); err != nil {
		t.Fatalf("WaitForPeer() error = %v, want nil", err)
	}
	if ts.staleNetmap.misses != 0 {
		t.Fatalf("missCount = %d, want 0", ts.staleNetmap.misses)
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
	if ts.staleNetmap.misses != 0 {
		t.Fatalf("missCount = %d, want 0 (IP literals must not count as misses)", ts.staleNetmap.misses)
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
	if ts.staleNetmap.misses != 1 {
		t.Fatalf("missCount = %d, want 1", ts.staleNetmap.misses)
	}
}

func TestWaitForPeerSuccessResetsMissCount(t *testing.T) {
	withFastPeerPolling(t)
	ts := testTailscale(t, statusWithPeers("beam-agent-machine"))
	ts.staleNetmap.misses = 2
	ts.staleNetmap.firstMissAt = time.Now().Add(-time.Minute)

	if err := ts.WaitForPeer(context.Background(), "beam-agent-machine", 50*time.Millisecond); err != nil {
		t.Fatalf("WaitForPeer() error = %v, want nil", err)
	}
	if ts.staleNetmap.misses != 0 || !ts.staleNetmap.firstMissAt.IsZero() {
		t.Fatalf("miss state = (%d, %v), want reset", ts.staleNetmap.misses, ts.staleNetmap.firstMissAt)
	}
}

func TestRepeatedMissesRecycleServer(t *testing.T) {
	withFastPeerPolling(t)
	prevThreshold, prevWindow, prevCooldown := staleNetmapMissThreshold, staleNetmapMissWindow, staleNetmapRestartCooldown
	staleNetmapMissThreshold = 3
	staleNetmapMissWindow = 0
	staleNetmapRestartCooldown = 0
	t.Cleanup(func() {
		staleNetmapMissThreshold, staleNetmapMissWindow, staleNetmapRestartCooldown = prevThreshold, prevWindow, prevCooldown
	})

	ts := testTailscale(t, statusWithPeers())
	originalServer := ts.currentServer()

	for i := 0; i < 3; i++ {
		_ = ts.WaitForPeer(context.Background(), "beam-agent-missing", time.Millisecond)
	}

	if ts.currentServer() == originalServer {
		t.Fatal("server was not recycled after repeated netmap misses")
	}
	if ts.initialized {
		t.Fatal("initialized = true after recycle, want false so the next dial re-ups")
	}
}

func TestRepeatedMissesDoNotRecycleServingNode(t *testing.T) {
	withFastPeerPolling(t)
	prevThreshold, prevWindow, prevCooldown := staleNetmapMissThreshold, staleNetmapMissWindow, staleNetmapRestartCooldown
	staleNetmapMissThreshold = 2
	staleNetmapMissWindow = 0
	staleNetmapRestartCooldown = 0
	t.Cleanup(func() {
		staleNetmapMissThreshold, staleNetmapMissWindow, staleNetmapRestartCooldown = prevThreshold, prevWindow, prevCooldown
	})

	ts := testTailscale(t, statusWithPeers())
	ts.served = true
	originalServer := ts.currentServer()

	for i := 0; i < 4; i++ {
		_ = ts.WaitForPeer(context.Background(), "beam-agent-missing", time.Millisecond)
	}

	if ts.currentServer() != originalServer {
		t.Fatal("serving node's tsnet server was recycled, want untouched")
	}
}
