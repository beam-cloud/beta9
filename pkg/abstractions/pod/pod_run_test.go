package pod

import (
	"context"
	"testing"
	"time"

	computemodel "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
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

func TestContainerManagementURLIsOptionalAndServerDefined(t *testing.T) {
	service := &GenericPodService{}
	require.Empty(t, service.containerManagementURL("pod-1"))

	service.config.GatewayService.ContainerURLTemplate = "https://console.example/containers/{container_id}"
	require.Equal(t, "https://console.example/containers/pod-1", service.containerManagementURL("pod-1"))
}
