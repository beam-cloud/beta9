package worker

import (
	"context"
	"time"
)

type networkRuleCleanupRequest struct {
	slot *containerNetworkSlot
	done chan error
}

type networkRuleCleanupBatch struct {
	requests []networkRuleCleanupRequest
}

// Collect only a millisecond of concurrent teardown. The existing firewall
// mutex still fences rule reads and deletes against all rule installation.
// Callers keep their slot and IP quarantined until the entire batch succeeds.
func (m *ContainerNetworkManager) removeNetworkSlotRulesBatched(slot *containerNetworkSlot) error {
	return m.queueNetworkSlotRuleCleanup(slot, m.removeNetworkSlotRuleBatch)
}

func (m *ContainerNetworkManager) queueNetworkSlotRuleCleanup(slot *containerNetworkSlot, remove func([]*containerNetworkSlot) error) error {
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	request := networkRuleCleanupRequest{slot: slot, done: make(chan error, 1)}
	m.ruleCleanupMu.Lock()
	batch := m.ruleCleanupPending
	start := batch == nil || len(batch.requests) == 128
	if start {
		batch = &networkRuleCleanupBatch{}
		m.ruleCleanupPending = batch
	}
	batch.requests = append(batch.requests, request)
	m.ruleCleanupMu.Unlock()
	if start {
		go func() {
			time.Sleep(time.Millisecond)
			m.ruleCleanupMu.Lock()
			if m.ruleCleanupPending == batch {
				m.ruleCleanupPending = nil
			}
			requests := batch.requests
			m.ruleCleanupMu.Unlock()
			slots := make([]*containerNetworkSlot, len(requests))
			for i, request := range requests {
				slots[i] = request.slot
			}
			err := remove(slots)
			for _, request := range requests {
				request.done <- err
			}
		}()
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
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
