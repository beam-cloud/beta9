package compute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// ReportAgentInterruption is a machine's agent passing on its provider's
// notice that the machine is about to be taken back.
//
// A machine that its pool bought is cordoned, the way an operator cordons
// one: it is given no new work and no longer counts as room in its pool, so
// a pool that keeps capacity free replaces it while it still runs. What runs
// on it is left to finish if it can. Once the provider has taken the machine
// it is cleaned up like any other that its provider took.
//
// Any other machine is left as it is, and the notice is only passed on. Its
// cloud may stop it and start it again, and nothing would lift the cordon.
func (s *Service) ReportAgentInterruption(ctx context.Context, in *pb.ReportAgentInterruptionRequest) (*pb.ReportAgentInterruptionResponse, error) {
	machine, errMsg := s.requireAgentState(ctx, in.AgentToken)
	if errMsg != "" {
		return &pb.ReportAgentInterruptionResponse{Ok: false, ErrMsg: errMsg}, nil
	}

	event := types.EventComputeSchema{
		WorkspaceID: machine.WorkspaceID,
		PoolName:    machine.PoolName,
		MachineID:   machine.MachineID,
		Action:      types.EventComputeActionMachineInterruptionNotice,
		Message:     interruptionMessage(in),
		Attrs:       map[string]string{"notice_source": strings.TrimSpace(in.Source)},
	}
	// The event is dated when the agent saw the notice, or now.
	if in.ObservedAtUnixNano > 0 {
		event.Timestamp = time.Unix(0, in.ObservedAtUnixNano).UTC()
	}

	bought, err := s.poolBuys(ctx, machine)
	if err != nil {
		return &pb.ReportAgentInterruptionResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if bought {
		// A machine that has no worker yet keeps the cordon on its own
		// record, and the worker it gets starts out cordoned.
		var noWorker *types.ErrWorkerNotFound
		if err := s.setAgentMachineCordon(ctx, machine, true); err != nil && !errors.As(err, &noWorker) {
			return &pb.ReportAgentInterruptionResponse{Ok: false, ErrMsg: err.Error()}, nil
		}
		event.Schedulable = boolPtr(false)
	}

	s.emitComputeEvent(types.EventComputeMachine, event)
	return &pb.ReportAgentInterruptionResponse{Ok: true}, nil
}

// poolBuys reports whether the machine's pool buys its machines. Such a pool
// takes none that are joined by hand, so the machine is one it bought.
func (s *Service) poolBuys(ctx context.Context, machine *model.AgentTokenState) (bool, error) {
	if machine.ManagedPoolInstanceID == "" {
		return false, nil // a workspace's own pool
	}
	pool, err := s.getAgentPoolState(ctx, machine)
	if err != nil {
		return false, err
	}
	// Nil is a pool this replica's config no longer has.
	return pool != nil && pool.WorkerConfig.Buys(), nil
}

// interruptionMessage says what the provider told the machine.
func interruptionMessage(in *pb.ReportAgentInterruptionRequest) string {
	message := "the machine's provider is about to take it back"
	if detail := strings.TrimSpace(in.Detail); detail != "" {
		message += ": " + detail
	}
	if in.ExpectedAtUnixNano > 0 {
		message += fmt.Sprintf(" (at %s)", time.Unix(0, in.ExpectedAtUnixNano).UTC().Format(time.RFC3339))
	}
	return message
}
