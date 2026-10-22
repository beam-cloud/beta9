package compute

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type rolloutTokenBackend struct{ repository.BackendRepository }

func (r *rolloutTokenBackend) GetWorkspaceByExternalId(_ context.Context, id string) (types.Workspace, error) {
	return types.Workspace{Id: 1, ExternalId: id}, nil
}
func (r *rolloutTokenBackend) GetTokenByExternalId(context.Context, uint, string) (*types.Token, error) {
	return &types.Token{ExternalId: "token-id", Key: "test-token", Active: true, TokenType: types.TokenTypeWorker}, nil
}
func (r *rolloutTokenBackend) CreateToken(_ context.Context, _ uint, tokenType string, _ bool) (types.Token, error) {
	return types.Token{ExternalId: "token-id", Key: "test-token", Active: true, TokenType: tokenType}, nil
}

func TestAgentRolloutBusyThenRevertUsesOriginalSlotAndRecoversWorker(t *testing.T) {
	ctx := context.Background()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	computeRepo := repository.NewComputeRedisRepository(rdb)
	agent := &model.AgentTokenState{TokenHash: "agent-token", WorkspaceID: "admin", MachineID: "machine", PoolName: "pool", Mode: string(types.PoolModeExternal)}
	worker := &types.Worker{Id: model.AgentMachineWorkerID(agent.MachineID), WorkspaceId: "admin", MachineId: "machine", PoolName: "pool", Status: types.WorkerStatusAvailable, TotalCpu: 8000, TotalMemory: 16000}
	require.NoError(t, workerRepo.AddWorker(worker))
	service := &Service{workerRepo: workerRepo, computeRepo: computeRepo, backendRepo: &rolloutTokenBackend{}, appConfig: types.AppConfig{Worker: types.WorkerConfig{ImageTag: "A"}}}
	require.NoError(t, service.saveComputeAgentTokenState(ctx, agent))
	slot := agentWorkerSlotState(service.appConfig, agent, worker, types.WorkerPoolConfig{}, "token-id", hashComputeToken("test-token"))
	require.NoError(t, setAgentWorkerSlotGeneration(slot))
	require.NoError(t, computeRepo.SaveAgentWorkerSlotState(ctx, slot))
	state := &types.ContainerState{ContainerId: "busy", WorkerId: worker.Id, Status: types.ContainerStatusRunning, Cpu: 2000, Memory: 4000}
	stateKey := common.RedisKeys.SchedulerContainerState(state.ContainerId)
	require.NoError(t, rdb.HSet(ctx, stateKey, common.ToSlice(state)).Err())
	require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())
	service.appConfig.Worker.ImageTag = "B"
	returned, _, err := service.ensureAgentWorkerSlot(ctx, agent, worker, types.WorkerPoolConfig{}, []*model.AgentWorkerSlotState{slot})
	require.NoError(t, err)
	require.Equal(t, slot.Generation, returned.Generation, "B is not published while A is busy")
	worker, err = workerRepo.GetWorkerById(worker.Id)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusDisabled, worker.Status)
	require.NotEmpty(t, worker.RolloutGeneration)
	require.NotEqual(t, slot.Generation, worker.RolloutGeneration)
	service.appConfig.Worker.ImageTag = "A"
	returned, _, err = service.ensureAgentWorkerSlot(ctx, agent, worker, types.WorkerPoolConfig{}, []*model.AgentWorkerSlotState{slot})
	require.NoError(t, err)
	require.Equal(t, slot.Generation, returned.Generation)
	worker, err = workerRepo.GetWorkerById(worker.Id)
	require.NoError(t, err)
	require.Equal(t, types.WorkerStatusAvailable, worker.Status)
	require.Empty(t, worker.RolloutGeneration)
	require.EqualValues(t, 6000, worker.FreeCpu)

	handled, err := service.SetAgentWorkerCordon(ctx, worker, true)
	require.NoError(t, err)
	require.True(t, handled)
	agent, err = computeRepo.GetAgentMachineState(ctx, agent.WorkspaceID, agent.PoolName, agent.MachineID)
	require.NoError(t, err)
	require.True(t, agent.Cordoned)
	worker, err = workerRepo.GetWorkerById(worker.Id)
	require.NoError(t, err)
	require.True(t, worker.CordonRequested)
	slots, err := service.agentSlotsForMachineLocked(ctx, agent)
	require.NoError(t, err)
	require.Len(t, slots, 1)
	require.Equal(t, slot.Generation, slots[0].Generation)
	saved, err := computeRepo.ListAgentWorkerSlotStates(ctx, agent.WorkspaceID, agent.PoolName, agent.MachineID)
	require.NoError(t, err)
	require.Len(t, saved, 1)
	require.Equal(t, slot.Generation, saved[0].Generation)
	require.Equal(t, string(types.ContainerStatusRunning), rdb.HGet(ctx, stateKey, "status").Val())
}

func TestAgentWorkerImageFollowsDeploymentRevisionAcrossGateways(t *testing.T) {
	ctx := context.Background()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	computeRepo := repository.NewComputeRedisRepository(rdb)
	agent := &model.AgentTokenState{WorkspaceID: "admin", MachineID: "machine", PoolName: "pool", Mode: string(types.PoolModeExternal)}
	worker := &types.Worker{Id: model.AgentMachineWorkerID(agent.MachineID), WorkspaceId: agent.WorkspaceID, MachineId: agent.MachineID, PoolName: agent.PoolName, Status: types.WorkerStatusAvailable, BuildVersion: "A"}
	require.NoError(t, workerRepo.AddWorker(worker))
	service := func(revision int64, image string) *Service {
		return &Service{workerRepo: workerRepo, computeRepo: computeRepo, backendRepo: &rolloutTokenBackend{}, redisClient: rdb,
			appConfig: types.AppConfig{GatewayService: types.GatewayServiceConfig{DeploymentRevision: revision}, Worker: types.WorkerConfig{ImageTag: image}}}
	}
	oldGateway, newGateway := service(1, "A"), service(2, "B")
	slot, _, err := oldGateway.ensureAgentWorkerSlot(ctx, agent, worker, types.WorkerPoolConfig{}, nil)
	require.NoError(t, err)
	seedState := &types.ContainerState{ContainerId: "busy", WorkerId: worker.Id, Status: types.ContainerStatusRunning}
	stateKey := common.RedisKeys.SchedulerContainerState(seedState.ContainerId)
	require.NoError(t, rdb.HSet(ctx, stateKey, common.ToSlice(seedState)).Err())
	require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())
	oldGeneration := slot.Generation

	// The new gateway starts draining A. An old replica must not cancel B.
	for _, gateway := range []*Service{newGateway, oldGateway} {
		worker, err = workerRepo.GetWorkerById(worker.Id)
		require.NoError(t, err)
		slot, _, err = gateway.ensureAgentWorkerSlot(ctx, agent, worker, types.WorkerPoolConfig{}, []*model.AgentWorkerSlotState{slot})
		require.NoError(t, err)
		require.Equal(t, oldGeneration, slot.Generation)
		worker, err = workerRepo.GetWorkerById(worker.Id)
		require.NoError(t, err)
		require.NotEmpty(t, worker.RolloutGeneration)
	}
	require.NoError(t, rdb.Del(ctx, stateKey).Err())
	slot, _, err = oldGateway.ensureAgentWorkerSlot(ctx, agent, worker, types.WorkerPoolConfig{}, []*model.AgentWorkerSlotState{slot})
	require.NoError(t, err)
	require.Equal(t, agentWorkerImage(newGateway.appConfig), slot.WorkerImage)
	require.NotEqual(t, oldGeneration, slot.Generation)
	require.NoError(t, workerRepo.ToggleWorkerAvailable(worker.Id, slot.Generation))
	worker, err = workerRepo.GetWorkerById(worker.Id)
	require.NoError(t, err)
	require.Equal(t, "B", worker.BuildVersion)

	// An intentional rollback has a newer deployment revision and wins even
	// while the previous release's gateway continues serving agent snapshots.
	for _, gateway := range []*Service{service(3, "A"), newGateway, oldGateway} {
		slot, _, err = gateway.ensureAgentWorkerSlot(ctx, agent, worker, types.WorkerPoolConfig{}, []*model.AgentWorkerSlotState{slot})
		require.NoError(t, err)
		require.Equal(t, agentWorkerImage(oldGateway.appConfig), slot.WorkerImage)
	}
	require.NoError(t, workerRepo.ToggleWorkerAvailable(worker.Id, slot.Generation))
	worker, err = workerRepo.GetWorkerById(worker.Id)
	require.NoError(t, err)
	require.Equal(t, "A", worker.BuildVersion)
}
