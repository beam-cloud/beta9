package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-iptables/iptables"
	"github.com/stretchr/testify/require"
)

func TestNetworkRuleCleanupBatchBurst(t *testing.T) {
	m := &ContainerNetworkManager{ctx: context.Background()}
	var mu sync.Mutex
	seen := make(map[string]int)
	var sizes []int
	remove := func(slots []*containerNetworkSlot) error {
		mu.Lock()
		defer mu.Unlock()
		sizes = append(sizes, len(slots))
		for _, slot := range slots {
			seen[slot.id]++
		}
		return nil
	}
	start := make(chan struct{})
	results := make(chan error, 300)
	for i := range 300 {
		go func() {
			<-start
			results <- m.queueNetworkSlotRuleCleanup(&containerNetworkSlot{id: fmt.Sprint(i)}, remove)
		}()
	}
	close(start)
	for range 300 {
		require.NoError(t, <-results)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 300)
	for _, count := range seen {
		require.Equal(t, 1, count)
	}
	require.Less(t, len(sizes), 300)
	for _, size := range sizes {
		require.Positive(t, size)
		require.LessOrEqual(t, size, 128)
	}
}

func TestNetworkRuleCleanupBatchIsolatesFailingSlots(t *testing.T) {
	failure := errors.New("delete failed; keep only this reservation quarantined")
	var removed []string
	remove := func(slots []*containerNetworkSlot) error {
		for _, slot := range slots {
			if slot.id == "bad" {
				return failure
			}
			removed = append(removed, slot.id)
		}
		return nil
	}
	requests := []networkRuleCleanupRequest{}
	for _, id := range []string{"bad", "first", "second"} {
		requests = append(requests, networkRuleCleanupRequest{slot: &containerNetworkSlot{id: id}, done: make(chan error, 1), remove: remove})
	}
	flushNetworkRuleCleanup(requests)
	require.ErrorIs(t, <-requests[0].done, failure)
	require.NoError(t, <-requests[1].done)
	require.NoError(t, <-requests[2].done)
	require.Equal(t, []string{"first", "second"}, removed)
}

func TestNetworkRuleCleanupBatchJoinsDeletionAfterWorkerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &ContainerNetworkManager{ctx: ctx}
	started, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	result := make(chan error, 1)
	go func() {
		result <- m.queueNetworkSlotRuleCleanup(&containerNetworkSlot{}, func([]*containerNetworkSlot) error { close(started); <-release; return nil })
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		t.Fatalf("abandoned cleanup before deletion: %v", err)
	default:
	}
	unblock.Do(func() { close(release) })
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("cleanup did not complete")
	}
	called := false
	require.NoError(t, m.queueNetworkSlotRuleCleanup(nil, func([]*containerNetworkSlot) error { called = true; return nil }))
	require.True(t, called, "shutdown cleanup must also run after worker cancellation")
}

func TestNetworkRuleCleanupBatchRealFirewallPreservesOtherAddresses(t *testing.T) {
	if os.Getenv("BETA9_NETWORK_CLEANUP_REAL_IPTABLES") != "1" {
		t.Skip("requires a private privileged network namespace")
	}
	ipt, err := iptables.NewWithProtocol(iptables.ProtocolIPv4)
	require.NoError(t, err)
	m := &ContainerNetworkManager{ipt: ipt}
	ips := []string{"192.168.1.1", "192.168.1.2", "192.168.1.10"}
	t.Cleanup(func() {
		for _, ip := range ips {
			_ = ipt.DeleteIfExists("nat", "PREROUTING", "-p", "tcp", "--dport", "7111", "-j", "DNAT", "--to-destination", ip+":7111")
			_ = ipt.DeleteIfExists("filter", "FORWARD", "-s", ip, "-j", "DROP")
			_ = ipt.DeleteIfExists("filter", "INPUT", "-s", ip, "-m", "comment", "--comment", "owned rule with spaces", "-j", "DROP")
		}
	})
	for _, ip := range ips {
		require.NoError(t, ipt.AppendUnique("nat", "PREROUTING", "-p", "tcp", "--dport", "7111", "-j", "DNAT", "--to-destination", ip+":7111"))
		require.NoError(t, ipt.AppendUnique("filter", "FORWARD", "-s", ip, "-j", "DROP"))
		require.NoError(t, ipt.AppendUnique("filter", "INPUT", "-s", ip, "-m", "comment", "--comment", "owned rule with spaces", "-j", "DROP"))
	}
	require.NoError(t, m.removeNetworkSlotRuleBatch([]*containerNetworkSlot{{id: "first", ip: ips[0]}, {id: "second", ip: ips[1]}}))
	for _, tc := range [][2]string{{"nat", "PREROUTING"}, {"filter", "FORWARD"}, {"filter", "INPUT"}} {
		rules, err := ipt.List(tc[0], tc[1])
		require.NoError(t, err)
		foundOther := false
		for _, rule := range rules {
			require.False(t, iptablesRuleMatchesAnyIP(rule, ips[:2]), rule)
			foundOther = foundOther || iptablesRuleMatchesIP(rule, ips[2])
		}
		require.True(t, foundOther, "unrelated address must retain its rules in %v", tc)
	}
	// Retrying a partially completed cleanup must still preserve other IPs.
	require.NoError(t, m.removeNetworkSlotRuleBatch([]*containerNetworkSlot{{id: "first", ip: ips[0]}}))
}
