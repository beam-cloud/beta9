package pod

import (
	"context"
	"testing"
	"time"

	computemodel "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type podMachinePlacementRepo struct {
	repository.ComputeRepository
	machine *computemodel.AgentTokenState
}

func (r podMachinePlacementRepo) GetAgentMachineStateForWorkspace(context.Context, string, string) (*computemodel.AgentTokenState, error) {
	return r.machine, nil
}

func connectedPodMachine(pool string) *computemodel.AgentTokenState {
	return &computemodel.AgentTokenState{
		PoolName:        pool,
		MachineID:       "machine-1",
		Schedulable:     true,
		LastHeartbeatAt: time.Now(),
	}
}

func TestConfigureMachinePlacementPinsOwnedMachine(t *testing.T) {
	service := &GenericPodService{computeRepo: podMachinePlacementRepo{machine: connectedPodMachine("training")}}
	config := &types.StubConfigV1{}

	require.NoError(t, service.configureMachinePlacement(context.Background(), "workspace-1", "machine-1", config))
	require.Equal(t, "machine-1", config.MachineID)
	require.Equal(t, "training", config.PoolSelector())
}

func TestConfigureMachinePlacementRejectsPoolMismatch(t *testing.T) {
	service := &GenericPodService{computeRepo: podMachinePlacementRepo{machine: connectedPodMachine("training")}}
	config := &types.StubConfigV1{Pool: &types.PoolConfig{Name: "other", Selector: "other"}}

	err := service.configureMachinePlacement(context.Background(), "workspace-1", "machine-1", config)
	require.ErrorContains(t, err, "does not belong to pool")
}

func TestCreatePodRunOptionsPreserveResourceOverrideForColdAndCheckpointLaunches(t *testing.T) {
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(types.ForceResourceLimitsMetadata, "true"),
	)
	request := &pb.CreatePodRequest{StubId: "legacy-stub"}
	checkpoint := &types.Checkpoint{CheckpointId: "checkpoint-1"}

	for _, test := range []struct {
		name       string
		checkpoint *types.Checkpoint
	}{
		{name: "cold launch"},
		{name: "checkpoint clone", checkpoint: checkpoint},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := createPodRunOptions(ctx, request, test.checkpoint)
			require.True(t, opts.forceResourceLimits)
			require.Same(t, test.checkpoint, opts.checkpoint)
		})
	}

	require.False(t, createPodRunOptions(context.Background(), request, nil).forceResourceLimits)
}
