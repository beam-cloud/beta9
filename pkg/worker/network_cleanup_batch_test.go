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

func TestNetworkRuleCleanupBatchBurstAndFailure(t *testing.T) {
	m := &ContainerNetworkManager{ctx: context.Background()}
	var mu sync.Mutex
	seen := make(map[string]int)
	var sizes []int
	failure := errors.New("delete failed; keep reservations quarantined")
	remove := func(slots []*containerNetworkSlot) error {
		mu.Lock()
		defer mu.Unlock()
		sizes = append(sizes, len(slots))
		for _, slot := range slots {
			seen[slot.id]++
		}
		return failure
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
		require.ErrorIs(t, <-results, failure)
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

func TestNetworkRuleCleanupBatchCanceledCallerDoesNotBlockCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &ContainerNetworkManager{ctx: ctx}
	started, release, complete := make(chan struct{}), make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- m.queueNetworkSlotRuleCleanup(&containerNetworkSlot{}, func([]*containerNetworkSlot) error {
			close(started)
			<-release
			close(complete)
			return nil
		})
	}()
	<-started
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	close(release)
	select {
	case <-complete:
	case <-time.After(time.Second):
		t.Fatal("canceled caller blocked shared cleanup")
	}
	require.ErrorIs(t, m.queueNetworkSlotRuleCleanup(nil, func([]*containerNetworkSlot) error {
		t.Fatal("already canceled request was enqueued")
		return nil
	}), context.Canceled)
}

func TestNetworkRuleCleanupBatchRealFirewallPreservesOtherAddresses(t *testing.T) {
	if os.Getenv("BETA9_NETWORK_CLEANUP_REAL_IPTABLES") != "1" {
		t.Skip("requires a private privileged network namespace")
	}
	ipt, err := iptables.NewWithProtocol(iptables.ProtocolIPv4)
	require.NoError(t, err)
	m := &ContainerNetworkManager{ipt: ipt}
	ips := []string{"192.168.1.1", "192.168.1.2", "192.168.1.10"}
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
