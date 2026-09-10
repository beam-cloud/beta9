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

func TestAgentRolloutBusyThenRevertUsesOriginalSlotAndRecoversWorker(t *testing.T) {
	ctx := context.Background()
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	computeRepo := repository.NewComputeRedisRepository(rdb)
	agent := &model.AgentTokenState{WorkspaceID: "admin", MachineID: "machine", PoolName: "pool", Mode: string(types.PoolModeExternal)}
	worker := &types.Worker{Id: "worker", MachineId: "machine", PoolName: "pool", Status: types.WorkerStatusAvailable, TotalCpu: 8000, TotalMemory: 16000}
	require.NoError(t, workerRepo.AddWorker(worker))
	service := &Service{workerRepo: workerRepo, computeRepo: computeRepo, backendRepo: &rolloutTokenBackend{}, appConfig: types.AppConfig{Worker: types.WorkerConfig{ImageTag: "A"}}}
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
}
