package pod

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
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

func TestRunVMValidatesGPUColdBootBeforeLaunch(t *testing.T) {
	launchError := errors.New("secret resolution failed")
	for _, test := range []struct {
		name, gpu, checkpoint, want string
		count                       uint32
	}{
		{name: "CPU cold boot"},
		{name: "GPU cold boot", gpu: "RTX4090", count: 1},
		{name: "GPU without count", gpu: "RTX4090", want: "exactly one GPU"},
		{name: "multiple GPUs", gpu: "RTX4090", count: 2, want: "exactly one GPU"},
		{name: "GPU memory restore", gpu: "RTX4090", count: 1, checkpoint: "memory", want: "cold boot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, err := json.Marshal(types.StubConfigV1{
				UseVM:   true,
				Env:     []string{"BEAM_VM_SYSTEMD=1"},
				Runtime: types.Runtime{Gpu: types.GpuType(test.gpu), GpuCount: test.count},
				Secrets: []types.Secret{{Name: "APP_KEY"}},
			})
			require.NoError(t, err)
			service := &GenericPodService{backendRepo: &sandboxBackendRepository{
				stub:      &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeVM), Config: string(config)}},
				secretErr: launchError,
			}}
			info := &auth.AuthInfo{Workspace: &types.Workspace{ExternalId: "workspace"}}
			err = service.RunVM(context.Background(), info, "stub", "container", types.VMSpec{}, test.checkpoint, nil)
			if test.want != "" {
				require.ErrorContains(t, err, test.want)
			} else {
				require.ErrorIs(t, err, launchError)
			}
		})
	}
}
