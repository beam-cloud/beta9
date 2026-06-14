package compute

import (
	"fmt"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

func (s *Service) emitComputeEvent(eventType string, event types.EventComputeSchema) {
	if s.eventRepo == nil {
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	s.eventRepo.PushComputeEvent(eventType, event)
}

func computePoolEvent(workspaceID string, state *model.PoolState, action, status string) types.EventComputeSchema {
	if state == nil {
		return types.EventComputeSchema{}
	}
	if status == "" {
		status = state.Status
	}
	event := types.EventComputeSchema{
		WorkspaceID: workspaceID,
		PoolName:    state.Name,
		Action:      action,
		Status:      status,
		Transport:   state.Transport,
		Fallback:    state.Fallback,
		Source:      string(state.Source),
		GPUCount:    state.ReservedGPUs,
		NodeCount:   state.ReservedNodes,
		Attrs: map[string]string{
			"selector":               state.Selector,
			"mode":                   state.Mode,
			"priority":               fmt.Sprintf("%d", state.Priority),
			"reservation_count":      fmt.Sprintf("%d", len(state.Reservations)),
			"committed_spend_micros": fmt.Sprintf("%d", state.CommittedSpendMicros),
		},
	}
	if state.Config != nil {
		event.Attrs["ttl"] = state.Config.Ttl
		event.Attrs["max_spend"] = fmt.Sprintf("%g", state.Config.MaxSpend)
	}
	if !state.ExpiresAt.IsZero() {
		event.Attrs["expires_at"] = state.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return event
}
