package repository_services

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type lifecycleContainerRepo struct {
	repository.ContainerRepository
	state  *types.ContainerState
	states map[string]*types.ContainerState
}

func (r *lifecycleContainerRepo) GetContainerState(containerID string) (*types.ContainerState, error) {
	if r.states != nil {
		if state, ok := r.states[containerID]; ok {
			return state, nil
		}
		return nil, &types.ErrContainerStateNotFound{ContainerId: containerID}
	}
	return r.state, nil
}

type lifecycleWorkerRepo struct {
	repository.WorkerRepository
	worker *types.Worker
}

func (r *lifecycleWorkerRepo) GetWorkerById(string) (*types.Worker, error) {
	return r.worker, nil
}

type lifecycleEventRepo struct {
	repository.EventRepository
	events []types.EventContainerLifecycleSchema
}

type lifecycleComputeRepo struct {
	repository.ComputeRepository
	slots []*compute.AgentWorkerSlotState
}

func (r *lifecycleComputeRepo) ListAgentWorkerSlotStates(context.Context, string, string, string) ([]*compute.AgentWorkerSlotState, error) {
	return r.slots, nil
}

func (r *lifecycleEventRepo) PushContainerLifecycleEvent(event types.EventContainerLifecycleSchema) {
	r.events = append(r.events, event)
}

func TestPushContainerLifecycleEventsUsesAuthoritativeIdentity(t *testing.T) {
	state := &types.ContainerState{
		ContainerId: "container-1",
		StubId:      "stub-1",
		WorkspaceId: "workspace-1",
		WorkerId:    "worker-1",
	}
	events := &lifecycleEventRepo{}
	service := &WorkerRepositoryService{
		containerRepo: &lifecycleContainerRepo{state: state},
		workerRepo:    &lifecycleWorkerRepo{worker: &types.Worker{Id: "worker-1", PoolName: "pool-1", MachineId: "machine-1"}},
		computeRepo: &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{
			WorkerID: "worker-1", WorkerTokenID: "token-1",
		}}},
		eventRepo: events,
	}
	data, err := json.Marshal(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleImageLoad,
		ContainerID: state.ContainerId,
		StubID:      "forged-stub",
		WorkspaceID: "forged-workspace",
		WorkerID:    "forged-worker",
		MachineID:   "forged-machine",
	})
	require.NoError(t, err)

	response, err := service.PushContainerLifecycleEvents(workerLifecycleContext("workspace-1"), &pb.PushContainerLifecycleEventsRequest{
		WorkerId: "worker-1",
		Events:   [][]byte{data},
	})
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Len(t, events.events, 1)
	require.Equal(t, "workspace-1", events.events[0].WorkspaceID)
	require.Equal(t, "stub-1", events.events[0].StubID)
	require.Equal(t, "worker-1", events.events[0].WorkerID)
	require.Equal(t, "machine-1", events.events[0].MachineID)
}

func TestPushContainerLifecycleEventsSkipsAnotherWorkspace(t *testing.T) {
	events := &lifecycleEventRepo{}
	service := &WorkerRepositoryService{
		containerRepo: &lifecycleContainerRepo{state: &types.ContainerState{
			ContainerId: "container-1",
			WorkspaceId: "workspace-2",
			WorkerId:    "worker-1",
		}},
		workerRepo: &lifecycleWorkerRepo{worker: &types.Worker{Id: "worker-1"}},
		computeRepo: &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{
			WorkerID: "worker-1", WorkerTokenID: "token-1",
		}}},
		eventRepo: events,
	}
	data, err := json.Marshal(types.EventContainerLifecycleSchema{ID: types.ContainerLifecycleImageLoad, ContainerID: "container-1"})
	require.NoError(t, err)

	response, err := service.PushContainerLifecycleEvents(workerLifecycleContext("workspace-1"), &pb.PushContainerLifecycleEventsRequest{
		WorkerId: "worker-1",
		Events:   [][]byte{data},
	})
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Empty(t, events.events)
}

func TestPushContainerLifecycleEventsSkipsForeignContainerButKeepsValid(t *testing.T) {
	events := &lifecycleEventRepo{}
	service := &WorkerRepositoryService{
		containerRepo: &lifecycleContainerRepo{states: map[string]*types.ContainerState{
			"container-foreign": {ContainerId: "container-foreign", StubId: "stub-2", WorkspaceId: "workspace-1", WorkerId: "worker-2"},
			"container-good":    {ContainerId: "container-good", StubId: "stub-1", WorkspaceId: "workspace-1", WorkerId: "worker-1"},
		}},
		workerRepo: &lifecycleWorkerRepo{worker: &types.Worker{Id: "worker-1", PoolName: "pool-1", MachineId: "machine-1"}},
		computeRepo: &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{
			WorkerID: "worker-1", WorkerTokenID: "token-1",
		}}},
		eventRepo: events,
	}
	foreign, err := json.Marshal(types.EventContainerLifecycleSchema{ID: types.ContainerLifecycleImageLoad, ContainerID: "container-foreign"})
	require.NoError(t, err)
	missing, err := json.Marshal(types.EventContainerLifecycleSchema{ID: types.ContainerLifecycleImageLoad, ContainerID: "container-missing"})
	require.NoError(t, err)
	good, err := json.Marshal(types.EventContainerLifecycleSchema{ID: types.ContainerLifecycleImageLoad, ContainerID: "container-good"})
	require.NoError(t, err)

	response, err := service.PushContainerLifecycleEvents(workerLifecycleContext("workspace-1"), &pb.PushContainerLifecycleEventsRequest{
		WorkerId: "worker-1",
		Events:   [][]byte{foreign, missing, []byte("not json"), good},
	})
	require.NoError(t, err)
	require.True(t, response.Ok)
	require.Len(t, events.events, 1)
	require.Equal(t, "container-good", events.events[0].ContainerID)
	require.Equal(t, "stub-1", events.events[0].StubID)
	require.Equal(t, "worker-1", events.events[0].WorkerID)
}

func TestPushContainerLifecycleEventsRejectsAnotherWorkerToken(t *testing.T) {
	service := &WorkerRepositoryService{
		containerRepo: &lifecycleContainerRepo{state: &types.ContainerState{
			ContainerId: "container-1",
			WorkspaceId: "workspace-1",
			WorkerId:    "worker-1",
		}},
		workerRepo: &lifecycleWorkerRepo{worker: &types.Worker{Id: "worker-1", PoolName: "pool-1", MachineId: "machine-1"}},
		computeRepo: &lifecycleComputeRepo{slots: []*compute.AgentWorkerSlotState{{
			WorkerID: "worker-1", WorkerTokenID: "token-2",
		}}},
		eventRepo: &lifecycleEventRepo{},
	}
	data, err := json.Marshal(types.EventContainerLifecycleSchema{ID: types.ContainerLifecycleImageLoad, ContainerID: "container-1"})
	require.NoError(t, err)

	response, err := service.PushContainerLifecycleEvents(workerLifecycleContext("workspace-1"), &pb.PushContainerLifecycleEventsRequest{
		WorkerId: "worker-1",
		Events:   [][]byte{data},
	})
	require.NoError(t, err)
	require.False(t, response.Ok)
	require.Equal(t, errWorkerLifecycleUnauthorized.Error(), response.ErrorMsg)
}

func workerLifecycleContext(workspaceID string) context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Token: &types.Token{ExternalId: "token-1", TokenType: types.TokenTypeWorkerPrivate},
		Workspace: &types.Workspace{
			ExternalId: workspaceID,
		},
	})
}
