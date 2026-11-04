package worker

import (
	"context"

	"github.com/beam-cloud/beta9/pkg/common"
)

type networkRuleCleanupRequest struct {
	slot   *containerNetworkSlot
	done   chan error
	remove func([]*containerNetworkSlot) error
}

// Collect only a millisecond of concurrent teardown. The existing firewall
// mutex still fences rule reads and deletes against all rule installation.
// Each caller keeps its slot and IP quarantined until its own rules are removed.
func (m *ContainerNetworkManager) removeNetworkSlotRulesBatched(slot *containerNetworkSlot) error {
	return m.queueNetworkSlotRuleCleanup(slot, m.removeNetworkSlotRuleBatch)
}

func (m *ContainerNetworkManager) queueNetworkSlotRuleCleanup(slot *containerNetworkSlot, remove func([]*containerNetworkSlot) error) error {
	// Worker cancellation must not abandon firewall cleanup and permanently
	// quarantine an otherwise reusable IP. Bound admission, then join deletion.
	ctx, cancel := context.WithTimeout(context.Background(), containerNetworkCleanupRPCTimeout)
	defer cancel()
	m.ruleCleanupOnce.Do(func() {
		m.ruleCleanupQueue = make(chan networkRuleCleanupRequest, 256)
		go common.RunBatchQueue(m.ruleCleanupQueue, 128, flushNetworkRuleCleanup)
	})
	request := networkRuleCleanupRequest{slot: slot, done: make(chan error, 1), remove: remove}
	select {
	case m.ruleCleanupQueue <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	return <-request.done
}

func flushNetworkRuleCleanup(requests []networkRuleCleanupRequest) {
	slots := make([]*containerNetworkSlot, len(requests))
	for i, request := range requests {
		slots[i] = request.slot
	}
	err := requests[0].remove(slots)
	for _, request := range requests {
		resultErr := err
		if err != nil && len(requests) > 1 {
			// Deletes are idempotent. Isolate a failed batch so one bad
			// slot cannot quarantine addresses whose cleanup succeeded.
			resultErr = request.remove([]*containerNetworkSlot{request.slot})
		}
		request.done <- resultErr
	}
}

func iptablesRuleMatchesAnyIP(rule string, ips []string) bool {
	for _, ip := range ips {
		if iptablesRuleMatchesIP(rule, ip) {
			return true
		}
	}
	return false
}
