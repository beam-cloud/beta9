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
// Callers keep their slot and IP quarantined until the entire batch succeeds.
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
		go common.RunBatchQueue(m.ruleCleanupQueue, 128, func(requests []networkRuleCleanupRequest) {
			slots := make([]*containerNetworkSlot, len(requests))
			for i, request := range requests {
				slots[i] = request.slot
			}
			err := requests[0].remove(slots)
			for _, request := range requests {
				request.done <- err
			}
		})
	})
	request := networkRuleCleanupRequest{slot: slot, done: make(chan error, 1), remove: remove}
	select {
	case m.ruleCleanupQueue <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	return <-request.done
}

func iptablesRuleMatchesAnyIP(rule string, ips []string) bool {
	for _, ip := range ips {
		if iptablesRuleMatchesIP(rule, ip) {
			return true
		}
	}
	return false
}
