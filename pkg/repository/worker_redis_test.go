package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

type lostRedisScriptReplyHook struct {
	hash  string
	lost  atomic.Bool
	after func()
}

func (h *lostRedisScriptReplyHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *lostRedisScriptReplyHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && cmd.Name() == "evalsha" && len(cmd.Args()) > 1 && cmd.Args()[1] == h.hash && h.lost.CompareAndSwap(false, true) {
			if h.after != nil {
				h.after()
			}
			return errors.New("simulated lost Redis reply")
		}
		return err
	}
}

func (h *lostRedisScriptReplyHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func setPendingContainerRequests(t *testing.T, rdb *common.RedisClient, requests ...*types.ContainerRequest) {
	t.Helper()
	for _, request := range requests {
		assert.NoError(t, rdb.HSet(
			context.TODO(),
			common.RedisKeys.SchedulerContainerState(request.ContainerId),
			"container_id", request.ContainerId,
			"status", string(types.ContainerStatusPending),
			"worker_id", "",
			"machine_id", "",
		).Err())
	}
}

// runScheduleScript runs the schedule script for one request that evicts nothing.
func runScheduleScript(rdb *common.RedisClient, workerID string, request *types.ContainerRequest, payload []byte, batchID, stateKey, assignment string) (interface{}, error) {
	return scheduleContainerRequestsScript.Run(context.TODO(), rdb, []string{
		common.RedisKeys.SchedulerWorkerState(workerID),
		common.RedisKeys.SchedulerWorkerRequests(workerID),
		common.RedisKeys.SchedulerContainerWorkerIndex(workerID),
	}, request.CapacityCpu(), capacityMemoryForRequest(request), 0, 1, workerID,
		schedulerAssignmentIDField, schedulerDeliveryTokenField, schedulerDeliveryAttemptField, batchID,
		0, 0, 0, 0, types.ContainerStateTtlSWhileStopping, request.ContainerId,
		stateKey, payload, "", assignment, request.CapacityCpu()).Result()
}

func TestNewWorkerRedisRepository(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.NotNil(t, repo)
}

func TestAddAndRemoveWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:         "worker1",
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "",
	}

	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)

	err = repo.RemoveWorker(newWorker.Id)
	assert.Nil(t, err)

	err = repo.RemoveWorker(newWorker.Id)
	assert.Error(t, err)

	var notFound *types.ErrWorkerNotFound
	assert.True(t, errors.As(err, &notFound))
}

func TestRemoveWorkerRequeuesPendingWorkerRequests(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-with-queued-requests",
		Status:      types.WorkerStatusAvailable,
		PoolName:    "default",
		FreeCpu:     10_000,
		FreeMemory:  10_000,
		TotalCpu:    10_000,
		TotalMemory: 10_000,
	}
	assert.Nil(t, repo.AddWorker(worker))

	requests := []*types.ContainerRequest{
		{
			ContainerId: "container-1", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, RetryCount: 2,
			Env: []string{}, Ports: []uint32{}, Checkpoint: &types.Checkpoint{CacheSizeBytes: 9_007_199_254_740_993},
		},
		{ContainerId: "container-2", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, RetryCount: 4},
	}
	setPendingContainerRequests(t, rdb, requests...)
	for _, request := range requests {
		currentWorker, err := repo.GetWorkerById(worker.Id)
		assert.Nil(t, err)
		assert.Nil(t, repo.ScheduleContainerRequest(currentWorker, request))
	}
	staleRequest, err := json.Marshal(&types.ContainerRequest{ContainerId: "expired-container"})
	assert.NoError(t, err)
	staleDelivery, err := json.Marshal(map[string]string{"request": string(staleRequest), "delivery_token": "expired-assignment:1"})
	assert.NoError(t, err)
	assert.NoError(t, rdb.HSet(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id), "expired-assignment", staleDelivery).Err())

	assert.Nil(t, repo.RemoveWorker(worker.Id))

	_, err = repo.GetWorkerById(worker.Id)
	_, workerNotFound := err.(*types.ErrWorkerNotFound)
	assert.True(t, workerNotFound)

	queueDepth, err := rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), queueDepth)

	backlog, err := rdb.ZRange(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 0, -1).Result()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(backlog))

	retriesByContainer := map[string]int{}
	requeuedRequests := make([]*types.ContainerRequest, 0, len(backlog))
	for _, raw := range backlog {
		var request types.ContainerRequest
		assert.Nil(t, json.Unmarshal([]byte(raw), &request))
		retriesByContainer[request.ContainerId] = request.RetryCount
		requeuedRequests = append(requeuedRequests, &request)
	}
	assert.Equal(t, 3, retriesByContainer["container-1"])
	assert.Equal(t, 5, retriesByContainer["container-2"])
	for _, request := range requeuedRequests {
		if request.ContainerId == "container-1" {
			assert.Equal(t, []string{}, request.Env)
			assert.Equal(t, []uint32{}, request.Ports)
			assert.NotNil(t, request.Checkpoint)
			assert.Equal(t, int64(9_007_199_254_740_993), request.Checkpoint.CacheSizeBytes)
		}
	}

	replacement := &types.Worker{
		Id:          "replacement-worker",
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    10_000,
		TotalMemory: 10_000,
		FreeCpu:     10_000,
		FreeMemory:  10_000,
	}
	assert.NoError(t, repo.AddWorker(replacement))
	for _, request := range requeuedRequests {
		assert.NoError(t, repo.ScheduleContainerRequest(replacement, request))
		assert.Equal(t, replacement.Id, rdb.HGet(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId), "worker_id").Val())
	}
}

func TestGetWorkerById(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:         "worker1",
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "",
	}

	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)
}

func TestToggleWorkerAvailable(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	newWorker := &types.Worker{
		Id:         "worker1",
		Status:     types.WorkerStatusPending,
		FreeCpu:    1000,
		FreeMemory: 1000,
		Gpu:        "",
	}

	// Create a pending worker
	err = repo.AddWorker(newWorker)
	assert.Nil(t, err)

	worker, err := repo.GetWorkerById(newWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, newWorker.Status, worker.Status)

	// Set it to be available
	err = repo.ToggleWorkerAvailable(worker.Id, "")
	assert.Nil(t, err)

	// Retrieve it again and check fields
	worker, err = repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, newWorker.FreeCpu, worker.FreeCpu)
	assert.Equal(t, newWorker.FreeMemory, worker.FreeMemory)
	assert.Equal(t, newWorker.Gpu, worker.Gpu)
	assert.Equal(t, types.WorkerStatusAvailable, worker.Status)
}

func TestToggleWorkerAvailablePreservesDisabledWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "disabled-worker", Status: types.WorkerStatusDisabled}
	assert.Nil(t, repo.AddWorker(worker))
	assert.Nil(t, repo.ToggleWorkerAvailable(worker.Id, ""))

	worker, err = repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusDisabled, worker.Status)
}

func TestWorkerStartupAcknowledgesPublishedImage(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   types.WorkerStatus
		cordoned bool
	}{
		{name: "pending", status: types.WorkerStatusPending},
		{name: "available", status: types.WorkerStatusAvailable},
		{name: "disabled", status: types.WorkerStatusDisabled},
		{name: "cordoned", status: types.WorkerStatusDisabled, cordoned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			initialStatus := test.status
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			repo := NewWorkerRedisRepository(rdb, types.WorkerConfig{ImageTag: "gateway-desired", CleanupPendingWorkerAgeLimit: time.Minute})
			worker := &types.Worker{Id: "agent-worker", WorkspaceId: "admin", PoolName: "pool", MachineId: "machine", Status: initialStatus, BuildVersion: "old", CordonRequested: test.cordoned}
			require.NoError(t, repo.AddWorker(worker))
			require.NoError(t, NewComputeRedisRepository(rdb).SaveAgentTokenState(context.Background(), &compute.AgentTokenState{
				WorkspaceID: worker.WorkspaceId, PoolName: worker.PoolName, MachineID: worker.MachineId,
				Schedulable: true, LastHeartbeatAt: time.Now(),
			}, time.Hour))
			slotKey := common.RedisKeys.ComputeAgentSlot(worker.WorkspaceId, worker.PoolName, worker.MachineId, worker.Id)
			require.NoError(t, rdb.Set(context.Background(), slotKey, `{"worker_id":"agent-worker","generation":"running-generation","worker_image":"registry/worker:running-image"}`, 0).Err())

			// Startup from an obsolete slot cannot acknowledge a different image.
			require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "obsolete-generation"), "does not match published slot")
			state, err := repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.Equal(t, "old", state.BuildVersion)
			require.Equal(t, initialStatus, state.Status)

			if initialStatus == types.WorkerStatusDisabled && !test.cordoned {
				require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "running-generation"), "awaiting controller readiness")
			} else {
				require.NoError(t, repo.ToggleWorkerAvailable(worker.Id, "running-generation"))
			}
			state, err = repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.Equal(t, "running-image", state.BuildVersion)
			if initialStatus == types.WorkerStatusDisabled {
				require.Equal(t, types.WorkerStatusDisabled, state.Status)
			} else {
				require.Equal(t, types.WorkerStatusAvailable, state.Status)
			}

			// A stale controller snapshot must not overwrite the acknowledged image.
			require.NoError(t, repo.AddWorker(worker))
			state, err = repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.Equal(t, "running-image", state.BuildVersion)

			_, err = repo.PrepareWorkerRollout(worker.Id, "next-generation")
			require.NoError(t, err)
			require.NoError(t, repo.ToggleWorkerAvailable(worker.Id, ""))
			require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "running-generation"), "does not match rollout generation")
			state, err = repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.Equal(t, "next-generation", state.RolloutGeneration)
			require.Equal(t, types.WorkerStatusDisabled, state.Status)
			require.Equal(t, "running-image", state.BuildVersion)

			require.NoError(t, rdb.Set(context.Background(), slotKey, `{"worker_id":"agent-worker","generation":"next-generation","worker_image":"registry/worker:next-image"}`, 0).Err())
			if initialStatus == types.WorkerStatusDisabled && !test.cordoned {
				require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "next-generation"), "awaiting controller readiness")
			} else {
				require.NoError(t, repo.ToggleWorkerAvailable(worker.Id, "next-generation"))
			}
			state, err = repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.Empty(t, state.RolloutGeneration)
			require.Equal(t, "next-image", state.BuildVersion)
			require.Equal(t, initialStatus == types.WorkerStatusDisabled, state.Status == types.WorkerStatusDisabled)
		})
	}
}

func TestWorkerStartupRequiresPublishedSlot(t *testing.T) {
	for _, status := range []types.WorkerStatus{types.WorkerStatusPending, types.WorkerStatusAvailable, types.WorkerStatusDisabled} {
		t.Run(string(status), func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{Id: "worker", WorkspaceId: "workspace", PoolName: "pool", MachineId: "machine", Status: status, BuildVersion: "old"}
			if status == types.WorkerStatusDisabled {
				worker.RolloutGeneration = "generation"
			}
			require.NoError(t, repo.AddWorker(worker))
			before, err := repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "generation"), "read worker startup slot", "startup cannot succeed without its published generation")
			after, err := repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.Equal(t, before, after)
			slotKey := common.RedisKeys.ComputeAgentSlot(worker.WorkspaceId, worker.PoolName, worker.MachineId, worker.Id)
			require.NoError(t, rdb.Set(context.Background(), slotKey, "invalid-json", 0).Err())
			require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "generation"), "decode worker startup slot")
			for _, value := range []string{
				`{"worker_id":"worker","generation":"obsolete","worker_image":"worker:new"}`,
				`{"worker_id":"other-worker","generation":"generation","worker_image":"worker:new"}`,
			} {
				require.NoError(t, rdb.Set(context.Background(), slotKey, value, 0).Err())
				require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "generation"), "does not match published slot")
			}
			require.NoError(t, rdb.Del(context.Background(), slotKey).Err())
			require.NoError(t, rdb.HSet(context.Background(), slotKey, "invalid", "type").Err())
			require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "generation"), "read worker startup slot")
			after, err = repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.Equal(t, before, after, "rejected startup must not alter readiness, rollout intent, or image reporting")
		})
	}
}

func TestWorkerRolloutAcknowledgmentPreservesMachineEligibility(t *testing.T) {
	for _, test := range []struct {
		name               string
		modify             func(*compute.AgentTokenState)
		missing            bool
		previouslyDisabled bool
		pending            bool
		available          bool
	}{
		{name: "healthy", available: true},
		{name: "pending machine unavailable", pending: true, modify: func(m *compute.AgentTokenState) { m.Schedulable = false }},
		{name: "availability withdrawn", modify: func(m *compute.AgentTokenState) { m.Schedulable = false }},
		{name: "machine cordoned", modify: func(m *compute.AgentTokenState) { m.Cordoned = true }},
		{name: "agent disconnected", modify: func(m *compute.AgentTokenState) { m.LastDisconnectAt = time.Now().Add(time.Second) }},
		{name: "heartbeat expired", modify: func(m *compute.AgentTokenState) {
			m.LastHeartbeatAt = time.Now().Add(-2 * compute.AgentHeartbeatTimeout)
		}},
		{name: "machine removed", missing: true},
		{name: "disabled before rollout", previouslyDisabled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{Id: "worker", WorkspaceId: "workspace", PoolName: "pool", MachineId: "machine", Status: types.WorkerStatusAvailable, BuildVersion: "old"}
			if test.previouslyDisabled {
				worker.Status = types.WorkerStatusDisabled
			} else if test.pending {
				worker.Status = types.WorkerStatusPending
			}
			require.NoError(t, repo.AddWorker(worker))
			if !test.pending {
				_, err = repo.PrepareWorkerRollout(worker.Id, "replacement")
				require.NoError(t, err)
			}
			require.NoError(t, rdb.Set(ctx, common.RedisKeys.ComputeAgentSlot(worker.WorkspaceId, worker.PoolName, worker.MachineId, worker.Id), `{"worker_id":"worker","generation":"replacement","worker_image":"worker:new"}`, 0).Err())
			machine := &compute.AgentTokenState{WorkspaceID: worker.WorkspaceId, PoolName: worker.PoolName, MachineID: worker.MachineId, Schedulable: true, LastHeartbeatAt: time.Now()}
			if test.modify != nil {
				test.modify(machine)
			}
			if !test.missing {
				require.NoError(t, NewComputeRedisRepository(rdb).SaveAgentTokenState(ctx, machine, time.Hour))
			}
			if test.available {
				require.NoError(t, repo.ToggleWorkerAvailable(worker.Id, "replacement"))
			} else {
				require.ErrorContains(t, repo.ToggleWorkerAvailable(worker.Id, "replacement"), "awaiting controller readiness")
			}
			state, err := repo.GetWorkerById(worker.Id)
			require.NoError(t, err)
			require.Equal(t, test.available, state.Status == types.WorkerStatusAvailable)
			require.Equal(t, "new", state.BuildVersion, "the actual image is acknowledged even while the machine remains disabled")
			require.Empty(t, state.RolloutGeneration)
		})
	}
}

func TestToggleWorkerAvailableReconcilesCapacityFromQueueAndContainerIndex(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{
		Id:            "worker-reconcile-capacity",
		Status:        types.WorkerStatusPending,
		TotalCpu:      4000,
		TotalMemory:   1024,
		TotalGpuCount: 2,
		FreeCpu:       0,
		FreeMemory:    0,
		FreeGpuCount:  0,
		Gpu:           "A10G",
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	queuedRequest := &types.ContainerRequest{
		ContainerId: "container-reconcile-queued",
		Cpu:         1000,
		Memory:      100,
	}
	queuedJSON, err := json.Marshal(queuedRequest)
	assert.Nil(t, err)
	err = rdb.RPush(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), queuedJSON).Err()
	assert.Nil(t, err)
	queuedState := &types.ContainerState{
		ContainerId: queuedRequest.ContainerId,
		Status:      types.ContainerStatusPending,
		WorkerId:    worker.Id,
		Cpu:         queuedRequest.Cpu,
		Memory:      queuedRequest.Memory,
	}
	queuedStateKey := common.RedisKeys.SchedulerContainerState(queuedState.ContainerId)
	err = rdb.HSet(context.TODO(), queuedStateKey, common.ToSlice(queuedState)).Err()
	assert.Nil(t, err)
	err = rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), queuedStateKey).Err()
	assert.Nil(t, err)

	runningState := &types.ContainerState{
		ContainerId: "container-reconcile-running",
		Status:      types.ContainerStatusRunning,
		WorkerId:    worker.Id,
		Cpu:         1000,
		Memory:      100,
		Gpu:         "A10G",
		GpuCount:    1,
	}
	runningStateKey := common.RedisKeys.SchedulerContainerState(runningState.ContainerId)
	err = rdb.HSet(context.TODO(), runningStateKey, common.ToSlice(runningState)).Err()
	assert.Nil(t, err)
	err = rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), runningStateKey).Err()
	assert.Nil(t, err)
	staleStateKey := common.RedisKeys.SchedulerContainerState("container-reconcile-missing")
	err = rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), staleStateKey).Err()
	assert.Nil(t, err)
	emptyOwnerStateKey := common.RedisKeys.SchedulerContainerState("container-reconcile-empty-owner")
	assert.NoError(t, rdb.HSet(context.TODO(), emptyOwnerStateKey, "status", string(types.ContainerStatusRunning), "worker_id", "", "cpu", 1000).Err())
	assert.NoError(t, rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), emptyOwnerStateKey).Err())

	err = repo.ToggleWorkerAvailable(worker.Id, "")
	assert.Nil(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusAvailable, updatedWorker.Status)
	assert.Equal(t, int64(2000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(774), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(1), updatedWorker.FreeGpuCount)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	staleIndexExists, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), staleStateKey).Result()
	assert.Nil(t, err)
	assert.False(t, staleIndexExists)
	assert.False(t, rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), emptyOwnerStateKey).Val())
}

func TestToggleWorkerAvailableCapsReconciledCapacityAtZero(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{
		Id:            "worker-reconcile-over-reserved",
		Status:        types.WorkerStatusPending,
		TotalCpu:      1000,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       1000,
		FreeMemory:    125,
		FreeGpuCount:  1,
		Gpu:           "A10G",
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	queuedRequest := &types.ContainerRequest{
		ContainerId: "container-reconcile-over-reserved",
		Cpu:         2000,
		Memory:      200,
		Gpu:         "A10G",
		GpuCount:    2,
	}
	queuedJSON, err := json.Marshal(queuedRequest)
	assert.Nil(t, err)
	err = rdb.RPush(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), queuedJSON).Err()
	assert.Nil(t, err)

	err = repo.ToggleWorkerAvailable(worker.Id, "")
	assert.Nil(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(0), updatedWorker.FreeGpuCount)
}

func TestStoppingContainerRemainsReservedUntilStateDeletion(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-stopping-capacity",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      100,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       100,
		FreeMemory:    125,
		FreeGpuCount:  1,
		Gpu:           "A10G",
	}
	assert.NoError(t, repo.AddWorker(worker))
	request := &types.ContainerRequest{
		ContainerId: "container-stopping-capacity",
		Cpu:         100,
		Memory:      100,
		Gpu:         worker.Gpu,
		GpuCount:    1,
	}
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	assert.NoError(t, rdb.HSet(context.TODO(), stateKey, common.ToSlice(&types.ContainerState{
		ContainerId: request.ContainerId,
		Status:      types.ContainerStatusStopping,
		WorkerId:    worker.Id,
		Cpu:         request.Cpu,
		Memory:      request.Memory,
		Gpu:         request.Gpu,
		GpuCount:    request.GpuCount,
	})).Err())
	assert.NoError(t, rdb.SAdd(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())

	heartbeat := *worker
	assert.NoError(t, repo.AddWorker(&heartbeat))
	assert.Equal(t, int64(0), heartbeat.FreeCpu)
	assert.Equal(t, int64(0), heartbeat.FreeMemory)
	assert.Equal(t, uint32(0), heartbeat.FreeGpuCount)
	assert.NoError(t, repo.UpdateWorkerCapacity(&heartbeat, request, types.AddCapacity))
	assert.Equal(t, uint32(0), heartbeat.FreeGpuCount)

	assert.NoError(t, rdb.Del(context.TODO(), stateKey).Err())
	assert.NoError(t, rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())
	assert.NoError(t, repo.UpdateWorkerCapacity(&heartbeat, request, types.AddCapacity))
	assert.Equal(t, int64(100), heartbeat.FreeCpu)
	assert.Equal(t, int64(125), heartbeat.FreeMemory)
	assert.Equal(t, uint32(1), heartbeat.FreeGpuCount)
	releasedVersion := heartbeat.ResourceVersion
	assert.NoError(t, repo.UpdateWorkerCapacity(&heartbeat, request, types.AddCapacity))
	assert.Equal(t, releasedVersion, heartbeat.ResourceVersion)
}

func TestStaleWorkerHeartbeatCannotRestoreScheduledCapacity(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-heartbeat-schedule-race",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      100,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       100,
		FreeMemory:    125,
		FreeGpuCount:  1,
		Gpu:           "A10G",
	}
	assert.NoError(t, repo.AddWorker(worker))
	request := &types.ContainerRequest{ContainerId: "container-heartbeat-schedule-race", Cpu: 100, Memory: 100, Gpu: worker.Gpu}
	setPendingContainerRequests(t, rdb, request)

	heartbeat := *worker
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	assert.NoError(t, repo.AddWorker(&heartbeat))

	updated, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), updated.FreeCpu)
	assert.Equal(t, int64(0), updated.FreeMemory)
	assert.Equal(t, uint32(0), updated.FreeGpuCount)
	assert.Equal(t, int64(1), updated.ResourceVersion)
	assert.Equal(t, int64(1), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
}

func TestUpdateWorkerCapacityReconcilesCompletedGPURequestExactlyOnce(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.Nil(t, err)

	worker := &types.Worker{
		Id:            "worker-capacity-release",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      500,
		TotalMemory:   1250,
		TotalGpuCount: 1,
		FreeCpu:       500,
		FreeMemory:    1250,
		Gpu:           "A10G",
		FreeGpuCount:  1,
	}
	request := &types.ContainerRequest{
		ContainerId: "container-capacity-release",
		Cpu:         500,
		Memory:      1000,
		Gpu:         "A10G",
		GpuCount:    1,
	}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	assert.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId)).Err())
	assert.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Err())
	assert.NoError(t, rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), common.RedisKeys.SchedulerContainerState(request.ContainerId)).Err())

	assert.NoError(t, repo.UpdateWorkerCapacity(worker, request, types.AddCapacity))
	version := worker.ResourceVersion
	assert.Equal(t, int64(500), worker.FreeCpu)
	assert.Equal(t, int64(1250), worker.FreeMemory)
	assert.Equal(t, uint32(1), worker.FreeGpuCount)
	assert.NoError(t, repo.UpdateWorkerCapacity(worker, request, types.AddCapacity))
	assert.Equal(t, version, worker.ResourceVersion)
}

func TestCapacityMemoryForRequest(t *testing.T) {
	assert.Equal(t, int64(0), capacityMemoryForRequest(&types.ContainerRequest{}))
	assert.Equal(t, int64(-1), capacityMemoryForRequest(&types.ContainerRequest{Memory: -1}))
	assert.Equal(t, int64(125), capacityMemoryForRequest(&types.ContainerRequest{Memory: 100}))
	assert.Equal(t, int64(2), capacityMemoryForRequest(&types.ContainerRequest{Memory: 1}))
}

func TestNativeVMCapacityReconciliationIncludesControlMemory(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.Nil(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "vm-capacity", Runtime: types.ContainerRuntimeMicroVM.String(), TotalMemory: 4096}
	state := &types.ContainerState{ContainerId: "vm-running", WorkerId: worker.Id, Status: types.ContainerStatusRunning, Memory: 512}
	key := common.RedisKeys.SchedulerContainerState(state.ContainerId)
	assert.Nil(t, rdb.HSet(context.Background(), key, common.ToSlice(state)).Err())
	assert.Nil(t, rdb.SAdd(context.Background(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), key).Err())
	assert.Nil(t, repo.(*WorkerRedisRepository).reconcileWorkerCapacity(context.Background(), worker))
	assert.Equal(t, int64(2816), worker.FreeMemory, "running VMs reserve their exact guest and VMM memory after queue consumption")
}

func TestUpdateWorkerCapacityAddDoesNotExceedTotalCapacity(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-capacity-cap",
		Status:        types.WorkerStatusAvailable,
		TotalCpu:      1000,
		TotalMemory:   125,
		TotalGpuCount: 1,
		FreeCpu:       900,
		FreeMemory:    100,
		FreeGpuCount:  0,
		Gpu:           "A10G",
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-capacity-cap",
		Cpu:         500,
		Memory:      100,
		Gpu:         "A10G",
		GpuCount:    2,
	}
	err = repo.UpdateWorkerCapacity(worker, request, types.AddCapacity)
	assert.Nil(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(125), updatedWorker.FreeMemory)
	assert.Equal(t, uint32(1), updatedWorker.FreeGpuCount)
}

func TestGetAllWorkers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	// Create a bunch of available workers
	nWorkers := 100
	for i := 0; i < nWorkers; i++ {
		err := repo.AddWorker(&types.Worker{
			Id:           fmt.Sprintf("worker-available-%d", i),
			Status:       types.WorkerStatusAvailable,
			FreeCpu:      1000,
			FreeMemory:   1000,
			Gpu:          "A10G",
			FreeGpuCount: 1,
		})
		assert.Nil(t, err)
	}

	// Create a bunch of pending workers
	for i := 0; i < nWorkers; i++ {
		err := repo.AddWorker(&types.Worker{
			Id:           fmt.Sprintf("worker-pending-%d", i),
			Status:       types.WorkerStatusPending,
			FreeCpu:      1000,
			FreeMemory:   1000,
			Gpu:          "A10G",
			FreeGpuCount: 1,
		})
		assert.Nil(t, err)
	}

	workers, err := repo.GetAllWorkers()
	assert.Nil(t, err)

	// Ensure we got back the correct total number of workers
	assert.Equal(t, nWorkers*2, len(workers))

	// Ensure we got back the correct number of each status type
	availableCount := 0
	pendingCount := 0
	for _, worker := range workers {
		switch worker.Status {
		case types.WorkerStatusAvailable:
			availableCount++
		case types.WorkerStatusPending:
			pendingCount++
		}
	}
	assert.Equal(t, nWorkers, availableCount)
	assert.Equal(t, nWorkers, pendingCount)
}

func TestGetAllWorkersInPoolUsesPoolIndex(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.Nil(t, repo.AddWorker(&types.Worker{Id: "worker-pool-a", PoolName: "pool-a", MachineId: "machine-a"}))
	assert.Nil(t, repo.AddWorker(&types.Worker{Id: "worker-pool-b", PoolName: "pool-b", MachineId: "machine-a"}))

	workers, err := repo.GetAllWorkersInPool("pool-a")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(workers))
	assert.Equal(t, "worker-pool-a", workers[0].Id)
}

func TestSetWorkerKeepAliveUpdatesMachineID(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-moving", Status: types.WorkerStatusAvailable, PoolName: "pool-a", MachineId: "machine-a"}
	assert.Nil(t, repo.AddWorker(worker))

	assert.Nil(t, repo.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{MachineId: "machine-b"}))

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, "machine-b", updatedWorker.MachineId)
}

func TestKeepAliveCPUTierProtocolIsReadBack(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.Nil(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	assert.Nil(t, repo.AddWorker(&types.Worker{Id: "tier-worker", Status: types.WorkerStatusAvailable, PoolName: "pool-a"}))
	assert.Nil(t, repo.SetWorkerKeepAlive("tier-worker", types.WorkerKeepAlive{CPUTierProtocol: types.CPUTierProtocolVersion}))

	workers, err := repo.GetAllWorkersInPool("pool-a")
	assert.Nil(t, err)
	assert.Len(t, workers, 1)
	assert.Equal(t, types.CPUTierProtocolVersion, workers[0].CPUTierProtocol)
}

func TestKeepAliveGPUVerificationIsReadBackAndWithdrawn(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.Nil(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "gpu-worker", Status: types.WorkerStatusAvailable, PoolName: "pool-a"}
	assert.Nil(t, repo.AddWorker(worker))

	verified := types.WorkerKeepAlive{VerifiedLocalGPURuntime: types.ContainerRuntimeGvisor.String(), NVIDIADriverVersion: "580.200.01"}
	assert.Nil(t, repo.SetWorkerKeepAlive(worker.Id, verified))
	assert.Nil(t, repo.AddWorker(worker))
	updated, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, verified.VerifiedLocalGPURuntime, updated.VerifiedLocalGPURuntime)
	assert.Equal(t, verified.NVIDIADriverVersion, updated.NVIDIADriverVersion)
	assert.Nil(t, repo.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{}))
	withoutVerification, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Empty(t, withoutVerification.VerifiedLocalGPURuntime)
	assert.Empty(t, withoutVerification.NVIDIADriverVersion)
	assert.Equal(t, updated.ResourceVersion+1, withoutVerification.ResourceVersion)
	err = repo.ScheduleContainerRequest(updated, &types.ContainerRequest{
		ContainerId: "gpu-request",
		Gpu:         "A10G",
		GpuCount:    1,
	})
	assert.True(t, errors.Is(err, ErrWorkerCapacityChanged), err)
}

func TestWorkerCheckpointCompatibilityKey(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.Nil(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "checkpoint-worker", Status: types.WorkerStatusAvailable}
	assert.Nil(t, repo.AddWorker(worker))

	for _, tc := range []struct {
		key, want string
		version   int64
	}{
		{"host-a", "host-a", 1},
		{"host-a", "host-a", 1},
		{"host-b", "host-b", 2},
		{"", "host-b", 2},
	} {
		assert.Nil(t, repo.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{CheckpointCompatibilityKey: tc.key}))
		updated, err := repo.GetWorkerById(worker.Id)
		assert.Nil(t, err)
		assert.Equal(t, tc.want, updated.CheckpointCompatibilityKey)
		assert.Equal(t, tc.version, updated.ResourceVersion)
	}

	// A stale controller registration must preserve the worker's latest profile.
	worker.CheckpointCompatibilityKey = "host-a"
	assert.Nil(t, repo.AddWorker(worker))
	updated, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, "host-b", updated.CheckpointCompatibilityKey)
	assert.Equal(t, int64(2), updated.ResourceVersion)
}

func TestWorkerSecondaryIndexesRemoveStaleMembers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	staleKey := common.RedisKeys.SchedulerWorkerState("worker-stale")
	poolIndexKey := common.RedisKeys.SchedulerWorkerPoolIndex("pool-a")

	assert.Nil(t, rdb.SAdd(context.TODO(), poolIndexKey, staleKey).Err())

	workers, err := repo.GetAllWorkersInPool("pool-a")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(workers))

	members, err := rdb.SMembers(context.TODO(), poolIndexKey).Result()
	assert.Nil(t, err)
	assert.NotContains(t, members, staleKey)
}

func TestWorkerSecondaryIndexesIgnoreWrongPoolMembers(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-wrong-pool", PoolName: "pool-a"}
	assert.Nil(t, repo.AddWorker(worker))

	stateKey := common.RedisKeys.SchedulerWorkerState(worker.Id)
	wrongPoolIndex := common.RedisKeys.SchedulerWorkerPoolIndex("pool-b")
	assert.Nil(t, rdb.SAdd(context.TODO(), wrongPoolIndex, stateKey).Err())

	workers, err := repo.GetAllWorkersInPool("pool-b")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(workers))

	inWrongPool, err := rdb.SIsMember(context.TODO(), wrongPoolIndex, stateKey).Result()
	assert.Nil(t, err)
	assert.False(t, inWrongPool)
}

func TestWorkerSecondaryIndexesUpdateAndRemove(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-moving", PoolName: "pool-a", MachineId: "machine-a"}
	assert.Nil(t, repo.AddWorker(worker))

	worker.PoolName = "pool-b"
	worker.MachineId = "machine-b"
	assert.Nil(t, repo.AddWorker(worker))

	stateKey := common.RedisKeys.SchedulerWorkerState(worker.Id)
	inOldPool, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerWorkerPoolIndex("pool-a"), stateKey).Result()
	assert.Nil(t, err)
	assert.False(t, inOldPool)
	inNewPool, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerWorkerPoolIndex("pool-b"), stateKey).Result()
	assert.Nil(t, err)
	assert.True(t, inNewPool)

	assert.Nil(t, repo.RemoveWorker(worker.Id))
	for _, indexKey := range []string{
		common.RedisKeys.SchedulerWorkerIndex(),
		common.RedisKeys.SchedulerWorkerPoolIndex("pool-b"),
	} {
		exists, err := rdb.SIsMember(context.TODO(), indexKey, stateKey).Result()
		assert.Nil(t, err)
		assert.False(t, exists)
	}
}

func TestScheduleContainerRequestRestoresCapacityWhenQueuePushFails(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-queue-error",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    1000,
		FreeMemory: 1000,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	err = rdb.Set(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), "wrong-type", 0).Err()
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-queue-error",
		Cpu:         100,
		Memory:      100,
	}
	setPendingContainerRequests(t, rdb, request)

	err = repo.ScheduleContainerRequest(worker, request)
	assert.Error(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1000), updatedWorker.FreeMemory)

	containerStateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	stateFields, err := rdb.HGetAll(context.TODO(), containerStateKey).Result()
	assert.Nil(t, err)
	assert.Equal(t, "", stateFields["worker_id"])
	assert.Equal(t, "", stateFields["machine_id"])
	indexed, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), containerStateKey).Result()
	assert.Nil(t, err)
	assert.False(t, indexed)
}

func TestScheduleContainerRequestRemovesQueuedRequestWhenMetadataWriteFails(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-metadata-error",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    1000,
		FreeMemory: 1000,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-metadata-error",
		Cpu:         100,
		Memory:      100,
	}
	err = rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId), "wrong-type", 0).Err()
	assert.Nil(t, err)

	err = repo.ScheduleContainerRequest(worker, request)
	assert.Error(t, err)

	queueDepth, err := rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), queueDepth)
	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1000), updatedWorker.FreeMemory)
	indexed, err := rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), common.RedisKeys.SchedulerContainerState(request.ContainerId)).Result()
	assert.Nil(t, err)
	assert.False(t, indexed)
}

func TestScheduleContainerRequestRejectsDisabledWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-disabled-schedule",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    1000,
		FreeMemory: 1000,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	selectedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)

	err = repo.UpdateWorkerStatus(worker.Id, types.WorkerStatusDisabled)
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: "container-disabled-schedule",
		Cpu:         100,
		Memory:      100,
	}
	err = repo.ScheduleContainerRequest(selectedWorker, request)
	assert.Error(t, err)

	queueDepth, err := rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), queueDepth)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, types.WorkerStatusDisabled, updatedWorker.Status)
	assert.Equal(t, int64(1000), updatedWorker.FreeCpu)
	assert.Equal(t, int64(1000), updatedWorker.FreeMemory)
}

func TestScheduleContainerRequestRejectsNegativeResources(t *testing.T) {
	for name, request := range map[string]*types.ContainerRequest{
		"cpu":    {ContainerId: "container-negative-cpu", Cpu: -1, Memory: 100},
		"memory": {ContainerId: "container-negative-memory", Cpu: 100, Memory: -1},
	} {
		t.Run(name, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			assert.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{Id: "worker-negative-" + name, Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
			assert.NoError(t, repo.AddWorker(worker))
			setPendingContainerRequests(t, rdb, request)

			assert.Error(t, repo.ScheduleContainerRequest(worker, request))
			updated, err := repo.GetWorkerById(worker.Id)
			assert.NoError(t, err)
			assert.Equal(t, int64(100), updated.FreeCpu)
			assert.Equal(t, int64(125), updated.FreeMemory)
			assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
		})
	}
}

func TestScheduleContainerRequestRejectsNonPendingState(t *testing.T) {
	for _, status := range []types.ContainerStatus{types.ContainerStatusStopping, ""} {
		t.Run(firstNonEmpty(string(status), "deleted"), func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			assert.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{
				Id:         "worker-cancelled-before-commit",
				Status:     types.WorkerStatusAvailable,
				FreeCpu:    100,
				FreeMemory: 125,
			}
			request := &types.ContainerRequest{
				ContainerId: "container-cancelled-before-commit",
				Cpu:         100,
				Memory:      100,
			}
			assert.NoError(t, repo.AddWorker(worker))
			if status != "" {
				assert.NoError(t, rdb.HSet(
					context.TODO(),
					common.RedisKeys.SchedulerContainerState(request.ContainerId),
					"status", string(status),
				).Err())
			}

			assert.Error(t, repo.ScheduleContainerRequest(worker, request))
			updated, err := repo.GetWorkerById(worker.Id)
			assert.NoError(t, err)
			assert.Equal(t, int64(100), updated.FreeCpu)
			assert.Equal(t, int64(125), updated.FreeMemory)
			assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
			assert.Equal(t, int64(0), rdb.SCard(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id)).Val())
		})
	}
}

func TestScheduleContainerRequestRejectsStaleWorkerReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-stale-reservation",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    100,
		FreeMemory: 125,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	firstWorkerCopy, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	secondWorkerCopy, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		ContainerId: "container-stale-reservation-first",
		Cpu:         100,
		Memory:      100,
	}
	secondRequest := &types.ContainerRequest{
		ContainerId: "container-stale-reservation-second",
		Cpu:         100,
		Memory:      100,
	}
	setPendingContainerRequests(t, rdb, firstRequest, secondRequest)

	err = repo.ScheduleContainerRequest(firstWorkerCopy, firstRequest)
	assert.Nil(t, err)

	err = repo.ScheduleContainerRequest(secondWorkerCopy, secondRequest)
	assert.True(t, errors.Is(err, ErrWorkerCapacityChanged), err)
	assert.True(t, errors.Is(err, ErrInsufficientEvictableCapacity), err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, int64(1), updatedWorker.ResourceVersion)

	queueDepth, err := rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Result()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), queueDepth)
}

func TestScheduleContainerRequestSharesWorkerMaintenanceLock(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-live-scheduling",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    100,
		FreeMemory: 125,
	}
	assert.NoError(t, repo.AddWorker(worker))

	maintenance := common.NewRedisLock(rdb)
	assert.NoError(t, maintenance.Acquire(context.Background(), common.RedisKeys.SchedulerWorkerLock(worker.Id), common.RedisLockOptions{TtlS: 10}))
	request := &types.ContainerRequest{
		ContainerId: "container-live-scheduling",
		Cpu:         100,
		Memory:      100,
	}
	setPendingContainerRequests(t, rdb, request)
	done := make(chan error, 1)
	go func() { done <- repo.ScheduleContainerRequest(worker, request) }()
	select {
	case err := <-done:
		t.Fatalf("schedule completed before worker lock was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	assert.NoError(t, maintenance.Release(common.RedisKeys.SchedulerWorkerLock(worker.Id)))
	assert.NoError(t, <-done)
}

func TestScheduleContainerRequestUsesCurrentCapacityForStaleWorkerReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-stale-reservation-current-capacity",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    200,
		FreeMemory: 250,
	}
	err = repo.AddWorker(worker)
	assert.Nil(t, err)

	firstWorkerCopy, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	secondWorkerCopy, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)

	firstRequest := &types.ContainerRequest{
		ContainerId: "container-stale-reservation-current-capacity-first",
		Cpu:         100,
		Memory:      100,
	}
	secondRequest := &types.ContainerRequest{
		ContainerId: "container-stale-reservation-current-capacity-second",
		Cpu:         100,
		Memory:      100,
	}
	setPendingContainerRequests(t, rdb, firstRequest, secondRequest)

	err = repo.ScheduleContainerRequest(firstWorkerCopy, firstRequest)
	assert.Nil(t, err)

	err = repo.ScheduleContainerRequest(secondWorkerCopy, secondRequest)
	assert.Nil(t, err)

	queuedFirstRequest, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.Nil(t, err)
	require.Len(t, queuedFirstRequest, 1)
	assert.Equal(t, firstRequest.ContainerId, queuedFirstRequest[0].ContainerId)

	queuedSecondRequest, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.Nil(t, err)
	require.Len(t, queuedSecondRequest, 1)
	assert.Equal(t, secondRequest.ContainerId, queuedSecondRequest[0].ContainerId)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, int64(2), updatedWorker.ResourceVersion)
}

func TestScheduleContainerRequestsReservesAndQueuesBatch(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:            "worker-batch",
		MachineId:     "machine-batch",
		Gpu:           "A10G",
		Status:        types.WorkerStatusAvailable,
		FreeCpu:       300,
		FreeMemory:    375,
		FreeGpuCount:  3,
		TotalGpuCount: 3,
	}
	assert.NoError(t, repo.AddWorker(worker))
	messages, _ := rdb.Subscribe(context.Background(), common.RedisKeys.SchedulerWorkerRequestChannel())

	requests := []*types.ContainerRequest{
		{ContainerId: "container-batch-1", Cpu: 100, Memory: 100, Gpu: worker.Gpu},
		{ContainerId: "container-batch-2", Cpu: 100, Memory: 100, Gpu: worker.Gpu},
		{ContainerId: "container-batch-3", Cpu: 100, Memory: 100, Gpu: worker.Gpu},
	}
	setPendingContainerRequests(t, rdb, requests...)
	assert.NoError(t, repo.ScheduleContainerRequests(worker, requests))
	select {
	case message := <-messages:
		assert.Equal(t, worker.Id, message.Payload)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker request notification")
	}

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), updatedWorker.FreeCpu)
	assert.Equal(t, int64(0), updatedWorker.FreeMemory)
	assert.Equal(t, int64(len(requests)), updatedWorker.ResourceVersion)

	actual, err := repo.GetNextContainerRequests(worker.Id, len(requests))
	assert.NoError(t, err)
	assert.Len(t, actual, len(requests))
	for i, expected := range requests {
		assert.Equal(t, expected.ContainerId, actual[i].ContainerId)
		assert.Equal(t, worker.MachineId, expected.MachineId)
		assert.Equal(t, worker.MachineId, actual[i].MachineId)

		state, err := rdb.HGetAll(context.TODO(), common.RedisKeys.SchedulerContainerState(expected.ContainerId)).Result()
		assert.NoError(t, err)
		assert.Equal(t, worker.Id, state["worker_id"])
		assert.Equal(t, worker.MachineId, state["machine_id"])
		assert.Equal(t, worker.Gpu, state["gpu"])
		assert.NotEmpty(t, state[schedulerAssignmentIDField])
	}
}

func TestContainerRequestDeliverySkipsExpiredState(t *testing.T) {
	for _, expired := range [][]int{{0}, {1}, {2}, {0, 1, 2}} {
		t.Run(fmt.Sprint(expired), func(t *testing.T) {
			ctx := context.Background()
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{Id: "worker-expired-delivery", Status: types.WorkerStatusAvailable,
				TotalCpu: 300, TotalMemory: 375, FreeCpu: 300, FreeMemory: 375}
			require.NoError(t, repo.AddWorker(worker))
			requests := []*types.ContainerRequest{
				{ContainerId: "delivery-0", Cpu: 100, Memory: 100},
				{ContainerId: "delivery-1", Cpu: 100, Memory: 100},
				{ContainerId: "delivery-2", Cpu: 100, Memory: 100},
			}
			setPendingContainerRequests(t, rdb, requests...)
			require.NoError(t, repo.ScheduleContainerRequests(worker, requests))
			expiredIDs := map[string]bool{}
			for _, index := range expired {
				id := requests[index].ContainerId
				expiredIDs[id] = true
				require.NoError(t, rdb.Del(ctx, common.RedisKeys.SchedulerContainerState(id)).Err())
			}
			var expected []string
			for _, request := range requests {
				if !expiredIDs[request.ContainerId] {
					expected = append(expected, request.ContainerId)
				}
			}

			for attempt := 0; attempt < 2; attempt++ {
				delivered, err := repo.GetNextContainerRequests(worker.Id, len(requests))
				require.NoError(t, err)
				var actual []string
				for _, request := range delivered {
					actual = append(actual, request.ContainerId)
					require.NotEmpty(t, request.DeliveryToken)
				}
				if attempt == 0 {
					require.Equal(t, expected, actual)
				} else {
					require.ElementsMatch(t, expected, actual)
				}
				require.Zero(t, rdb.LLen(ctx, common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
				require.Equal(t, int64(len(expected)), rdb.HLen(ctx, common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())
				for id := range expiredIDs {
					require.Zero(t, rdb.Exists(ctx, common.RedisKeys.SchedulerContainerState(id)).Val())
				}
				if attempt == 0 {
					// A reconnect must still recover every unacknowledged live request.
					require.NoError(t, repo.RecoverPendingContainerRequests(worker.Id))
				}
			}
			if len(expected) == 0 {
				// The controller's normal refresh now sees no outstanding reservation.
				require.NoError(t, repo.AddWorker(worker))
				worker, err = repo.GetWorkerById(worker.Id)
				require.NoError(t, err)
				require.Equal(t, worker.TotalCpu, worker.FreeCpu)
				fresh := &types.ContainerRequest{ContainerId: "delivery-fresh", Cpu: 100, Memory: 100}
				setPendingContainerRequests(t, rdb, fresh)
				require.NoError(t, repo.ScheduleContainerRequest(worker, fresh))
				delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
				require.NoError(t, err)
				require.Len(t, delivered, 1)
				require.Equal(t, fresh.ContainerId, delivered[0].ContainerId)
			}
		})
	}
}

func TestContainerRequestDeliveryPreservesLiveStateWithoutAssignment(t *testing.T) {
	ctx := context.Background()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	request := &types.ContainerRequest{ContainerId: "delivery-unassigned", Cpu: 100, Memory: 100}
	setPendingContainerRequests(t, rdb, request)
	raw, err := json.Marshal(request)
	require.NoError(t, err)
	queue := common.RedisKeys.SchedulerWorkerRequests("worker-unassigned")
	require.NoError(t, rdb.RPush(ctx, queue, raw).Err())

	_, err = repo.GetNextContainerRequests("worker-unassigned", 1)
	require.ErrorContains(t, err, "missing schedule assignment")
	require.Equal(t, int64(1), rdb.LLen(ctx, queue).Val())
	require.Equal(t, int64(1), rdb.Exists(ctx, common.RedisKeys.SchedulerContainerState(request.ContainerId)).Val())
	require.Zero(t, rdb.HLen(ctx, common.RedisKeys.SchedulerWorkerPendingRequests("worker-unassigned")).Val())
}

func TestContainerRequestRemainsRecoverableUntilWorkerAcknowledgesIt(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-pending-delivery",
		Status:      types.WorkerStatusAvailable,
		FreeCpu:     100,
		FreeMemory:  125,
		TotalCpu:    100,
		TotalMemory: 125,
	}
	request := &types.ContainerRequest{ContainerId: "container-pending-delivery", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))

	delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)
	assert.Len(t, delivered, 1)
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
	assert.Equal(t, int64(1), rdb.HLen(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())

	assert.NoError(t, repo.RemoveWorker(worker.Id))
	assert.Equal(t, int64(0), rdb.HLen(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())
	assert.Equal(t, int64(1), rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val())
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	assert.Empty(t, rdb.HGet(context.TODO(), stateKey, "worker_id").Val())
	assert.Empty(t, rdb.HGet(context.TODO(), stateKey, schedulerAssignmentIDField).Val())
	assert.False(t, rdb.SIsMember(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Val())
	backlog, err := rdb.ZRange(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 0, 0).Result()
	assert.NoError(t, err)
	assert.Len(t, backlog, 1)
	var requeued types.ContainerRequest
	assert.NoError(t, json.Unmarshal([]byte(backlog[0]), &requeued))
	replacement := &types.Worker{Id: "worker-pending-delivery-replacement", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	assert.NoError(t, repo.AddWorker(replacement))
	assert.NoError(t, repo.ScheduleContainerRequest(replacement, &requeued))
}

func TestAcknowledgedContainerRequestIsNotReplayedWhenWorkerIsRemoved(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:          "worker-acknowledged-delivery",
		Status:      types.WorkerStatusAvailable,
		FreeCpu:     100,
		FreeMemory:  125,
		TotalCpu:    100,
		TotalMemory: 125,
	}
	request := &types.ContainerRequest{ContainerId: "container-acknowledged-delivery", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)

	assert.NoError(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, delivered[0].DeliveryToken))
	assert.Equal(t, int64(0), rdb.HLen(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())
	assert.NoError(t, repo.RemoveWorker(worker.Id))
	assert.Equal(t, int64(0), rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val())
}

func TestContainerRequestAcknowledgementRejectsCancelledContainer(t *testing.T) {
	for _, status := range []string{string(types.ContainerStatusStopping), ""} {
		status := status
		t.Run(firstNonEmpty(status, "deleted"), func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			assert.NotNil(t, rdb)
			assert.NoError(t, err)

			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := &types.Worker{
				Id:          "worker-cancelled-delivery",
				Status:      types.WorkerStatusAvailable,
				FreeCpu:     100,
				FreeMemory:  125,
				TotalCpu:    100,
				TotalMemory: 125,
			}
			request := &types.ContainerRequest{ContainerId: "container-cancelled-delivery", Cpu: 100, Memory: 100}
			assert.NoError(t, repo.AddWorker(worker))
			setPendingContainerRequests(t, rdb, request)
			assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
			delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
			assert.NoError(t, err)
			assert.Len(t, delivered, 1)

			stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
			if status == "" {
				assert.NoError(t, rdb.Del(context.TODO(), stateKey).Err())
			} else {
				assert.NoError(t, rdb.HSet(context.TODO(), stateKey, "status", status).Err())
			}

			assert.Error(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, delivered[0].DeliveryToken))
			assert.Equal(t, int64(0), rdb.HLen(context.TODO(), common.RedisKeys.SchedulerWorkerPendingRequests(worker.Id)).Val())
			assert.Equal(t, int64(0), rdb.SCard(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id)).Val())
		})
	}
}

func TestRecoveredContainerRequestRejectsStaleAcknowledgement(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-reconnect", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-reconnect", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	first, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)

	assert.NoError(t, repo.RecoverPendingContainerRequests(worker.Id))
	second, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)
	assert.NotEqual(t, first[0].DeliveryToken, second[0].DeliveryToken)
	assert.Error(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, first[0].DeliveryToken))
	assert.NoError(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, second[0].DeliveryToken))
}

func TestSendFailureDoesNotRequeueAcknowledgedDelivery(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-ack-race", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-ack-race", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	delivered, err := repo.GetNextContainerRequests(worker.Id, 1)
	assert.NoError(t, err)
	assert.NoError(t, repo.AddContainerToWorker(worker.Id, request.ContainerId, delivered[0].DeliveryToken))
	assert.NoError(t, repo.RequeueContainerRequests(worker.Id, delivered))
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
}

func TestRequeueContainerRequestsPreservesOrder(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.NoError(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-requeue", Status: types.WorkerStatusAvailable, FreeCpu: 300, FreeMemory: 375}
	assert.NoError(t, repo.AddWorker(worker))
	requests := []*types.ContainerRequest{
		{ContainerId: "container-requeue-1", Cpu: 100, Memory: 100},
		{ContainerId: "container-requeue-2", Cpu: 100, Memory: 100},
		{ContainerId: "container-requeue-3", Cpu: 100, Memory: 100},
	}
	setPendingContainerRequests(t, rdb, requests...)
	assert.NoError(t, repo.ScheduleContainerRequests(worker, requests))
	delivered, err := repo.GetNextContainerRequests(worker.Id, len(requests))
	assert.NoError(t, err)
	assert.NoError(t, repo.RequeueContainerRequests(worker.Id, delivered))

	actual, err := repo.GetNextContainerRequests(worker.Id, len(requests))
	assert.NoError(t, err)
	assert.Len(t, actual, len(requests))
	for i := range requests {
		assert.Equal(t, requests[i].ContainerId, actual[i].ContainerId)
	}
}

func TestScheduleContainerRequestsRejectsBatchWithoutPartialReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id:         "worker-batch-capacity",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    100,
		FreeMemory: 125,
	}
	assert.NoError(t, repo.AddWorker(worker))
	duplicate := &types.ContainerRequest{ContainerId: "container-batch-duplicate", Cpu: 1, Memory: 1}
	assert.Error(t, repo.ScheduleContainerRequests(worker, []*types.ContainerRequest{duplicate, duplicate}))
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())

	err = repo.ScheduleContainerRequests(worker, []*types.ContainerRequest{
		{ContainerId: "container-batch-capacity-1", Cpu: 100, Memory: 100},
		{ContainerId: "container-batch-capacity-2", Cpu: 100, Memory: 100},
	})
	assert.Error(t, err)

	updatedWorker, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), updatedWorker.FreeCpu)
	assert.Equal(t, int64(125), updatedWorker.FreeMemory)
	assert.Equal(t, int64(0), updatedWorker.ResourceVersion)
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
}

func TestScheduleContainerRequestsValidatesCapacityFieldsBeforeWriting(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-invalid-resource-version", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-invalid-resource-version", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, rdb.HSet(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), "resource_version", "invalid").Err())

	payload, err := json.Marshal(request)
	assert.NoError(t, err)
	result, err := runScheduleScript(rdb, worker.Id, request, payload, "batch-invalid-resource-version",
		common.RedisKeys.SchedulerContainerState(request.ContainerId), "assignment-invalid-resource-version")
	assert.NoError(t, err)
	_, err = parseWorkerCapacityResult(worker.Id, result)
	assert.Error(t, err)
	assert.Equal(t, "100", rdb.HGet(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), "free_cpu").Val())
	assert.Equal(t, "125", rdb.HGet(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), "free_memory").Val())
	assert.Equal(t, int64(0), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
	assert.Empty(t, rdb.HGet(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId), "worker_id").Val())
}

func TestScheduleBatchMarkerSurvivesImmediateContainerFinalization(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)
	worker := &types.Worker{Id: "worker-immediate-finalize", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-immediate-finalize", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)

	payload, err := json.Marshal(request)
	assert.NoError(t, err)
	batchID := "batch-immediate-finalize"
	assignment := "assignment-immediate-finalize"
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	result, err := runScheduleScript(rdb, worker.Id, request, payload, batchID, stateKey, assignment)
	assert.NoError(t, err)
	_, err = parseWorkerCapacityResult(worker.Id, result)
	assert.NoError(t, err)
	retried, err := runScheduleScript(rdb, worker.Id, request, payload, batchID, stateKey, assignment)
	assert.NoError(t, err)
	_, err = parseWorkerCapacityResult(worker.Id, retried)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())

	assert.NoError(t, rdb.Del(context.TODO(), stateKey, common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Err())
	assert.NoError(t, rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())
	committed, err := repo.scheduledBatchCommitted(context.TODO(), worker.Id, batchID, []queuedContainerRequest{{
		request: request, payload: payload, stateKey: stateKey, assignment: assignment,
	}})
	assert.NoError(t, err)
	assert.True(t, committed)
}

func TestScheduleContainerRequestTreatsLostCommitReplyAsSuccess(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-lost-schedule-reply", Status: types.WorkerStatusAvailable, FreeCpu: 100, FreeMemory: 125}
	request := &types.ContainerRequest{ContainerId: "container-lost-schedule-reply", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	_, err = scheduleContainerRequestsScript.Load(context.TODO(), rdb).Result()
	assert.NoError(t, err)
	hook := &lostRedisScriptReplyHook{hash: scheduleContainerRequestsScript.Hash()}
	rdb.AddHook(hook)

	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	assert.True(t, hook.lost.Load())
	assert.Equal(t, int64(1), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
	updated, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), updated.FreeCpu)
	assert.Equal(t, int64(0), updated.FreeMemory)
	assert.Error(t, repo.ScheduleContainerRequest(updated, request))
	assert.Equal(t, int64(1), rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
}

func TestScheduleContainerRequestReconcilesWhenLostReplyHasNoCommitEvidence(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id: "worker-lost-schedule-evidence", Status: types.WorkerStatusAvailable,
		TotalCpu: 100, TotalMemory: 125, FreeCpu: 100, FreeMemory: 125,
	}
	request := &types.ContainerRequest{ContainerId: "container-lost-schedule-evidence", Cpu: 100, Memory: 100}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	_, err = scheduleContainerRequestsScript.Load(context.TODO(), rdb).Result()
	assert.NoError(t, err)
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	hook := &lostRedisScriptReplyHook{hash: scheduleContainerRequestsScript.Hash()}
	hook.after = func() {
		assert.NoError(t, rdb.HDel(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id), schedulerLastBatchIDField).Err())
		assert.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id), stateKey).Err())
		assert.NoError(t, rdb.SRem(context.TODO(), common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id), stateKey).Err())
	}
	rdb.AddHook(hook)

	assert.Error(t, repo.ScheduleContainerRequest(worker, request))
	assert.True(t, hook.lost.Load())
	updated, err := repo.GetWorkerById(worker.Id)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), updated.FreeCpu)
	assert.Equal(t, int64(125), updated.FreeMemory)
}

func TestDelayedCapacityReleaseCannotOvercreditRecreatedWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{
		Id: "worker-recreated", Status: types.WorkerStatusAvailable,
		TotalCpu: 100, TotalMemory: 125, TotalGpuCount: 1,
		FreeCpu: 100, FreeMemory: 125, FreeGpuCount: 1, Gpu: "A10G",
	}
	request := &types.ContainerRequest{ContainerId: "container-recreated", Cpu: 100, Memory: 100, Gpu: worker.Gpu}
	assert.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, request)
	assert.NoError(t, repo.ScheduleContainerRequest(worker, request))
	assert.NoError(t, repo.RemoveWorker(worker.Id))
	replacement := *worker
	replacement.Id = "worker-recreated-replacement"
	assert.NoError(t, repo.AddWorker(&replacement))
	assert.NoError(t, repo.ScheduleContainerRequests(&replacement, []*types.ContainerRequest{request}))
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	oldIndex := common.RedisKeys.SchedulerContainerWorkerIndex(worker.Id)
	assert.NoError(t, rdb.SAdd(context.TODO(), oldIndex, stateKey).Err())

	recreated := *worker
	recreated.FreeCpu, recreated.FreeMemory, recreated.FreeGpuCount = 100, 125, 1
	assert.NoError(t, repo.AddWorker(&recreated))
	current := &types.ContainerRequest{ContainerId: "container-recreated-current", Cpu: 100, Memory: 100, Gpu: worker.Gpu}
	setPendingContainerRequests(t, rdb, current)
	assert.NoError(t, repo.ScheduleContainerRequests(&recreated, []*types.ContainerRequest{current}))
	assert.NoError(t, repo.UpdateWorkerCapacity(&recreated, request, types.AddCapacity))
	assert.Equal(t, int64(0), recreated.FreeCpu)
	assert.Equal(t, int64(0), recreated.FreeMemory)
	assert.Equal(t, uint32(0), recreated.FreeGpuCount)
	assert.False(t, rdb.SIsMember(context.TODO(), oldIndex, stateKey).Val())
	version := recreated.ResourceVersion
	assert.NoError(t, repo.UpdateWorkerCapacity(&recreated, request, types.AddCapacity))
	assert.Equal(t, version, recreated.ResourceVersion)
}

func TestWorkerNetworkIPIndexMovesPreallocatedReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	networkPrefix := "node-a"
	ip := "192.168.0.2"

	err = repo.SetContainerIp(networkPrefix, "network-slot:slot-a", ip)
	assert.Nil(t, err)

	err = repo.MoveContainerIp(networkPrefix, "network-slot:slot-a", "container-a", ip)
	assert.Nil(t, err)
	// A gateway retry after losing the successful reply preserves the owner.
	assert.NoError(t, repo.MoveContainerIp(networkPrefix, "network-slot:slot-a", "container-a", ip))
	// The source can meanwhile acquire a different IP. A replay must leave it.
	assert.NoError(t, repo.SetContainerIp(networkPrefix, "network-slot:slot-a", "192.168.0.3"))
	assert.NoError(t, repo.MoveContainerIp(networkPrefix, "network-slot:slot-a", "container-a", ip))
	sourceIP, err := repo.GetContainerIp(networkPrefix, "network-slot:slot-a")
	assert.NoError(t, err)
	assert.Equal(t, "192.168.0.3", sourceIP)
	assert.Error(t, repo.MoveContainerIp(networkPrefix, "network-slot:slot-a", "another-container", ip))

	ips, err := repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.Contains(t, ips, ip)

	err = repo.RemoveContainerIp(networkPrefix, "network-slot:slot-a")
	assert.Nil(t, err)

	ips, err = repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.Contains(t, ips, ip)

	containerIP, err := repo.GetContainerIp(networkPrefix, "container-a")
	assert.Nil(t, err)
	assert.Equal(t, ip, containerIP)

	err = repo.SetContainerIp(networkPrefix, "container-b", ip)
	assert.Error(t, err)

	err = repo.RemoveContainerIp(networkPrefix, "container-a")
	assert.Nil(t, err)

	ips, err = repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.NotContains(t, ips, ip)

	err = repo.RemoveContainerIp(networkPrefix, "container-a")
	assert.Nil(t, err)
}

func TestGetContainerIpAssignments(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	networkPrefix := "node-assignments"

	err = repo.SetContainerIp(networkPrefix, "network-slot:slot-a", "192.168.0.2")
	assert.Nil(t, err)
	err = repo.SetContainerIp(networkPrefix, "container-a", "192.168.0.3")
	assert.Nil(t, err)
	err = rdb.SAdd(context.TODO(), common.RedisKeys.WorkerNetworkIpIndex(networkPrefix), "192.168.0.99").Err()
	assert.Nil(t, err)

	assignments, err := repo.GetContainerIpAssignments(networkPrefix)
	assert.Nil(t, err)
	assert.Equal(t, []types.ContainerIpAssignment{
		{ContainerID: "container-a", IPAddress: "192.168.0.3"},
		{ContainerID: "network-slot:slot-a", IPAddress: "192.168.0.2"},
	}, assignments)

	ips, err := repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.NotContains(t, ips, "192.168.0.99")
}

func TestRemoveContainerIpCleansLegacyIndexWithoutOwnerKey(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(t, rdb)
	assert.Nil(t, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)
	networkPrefix := "node-legacy"
	ip := "192.168.0.12"

	err = repo.SetContainerIp(networkPrefix, "container-a", ip)
	assert.Nil(t, err)

	err = rdb.Del(context.TODO(), common.RedisKeys.WorkerNetworkIpOwner(networkPrefix, ip)).Err()
	assert.Nil(t, err)

	err = repo.RemoveContainerIp(networkPrefix, "container-a")
	assert.Nil(t, err)

	ips, err := repo.GetContainerIps(networkPrefix)
	assert.Nil(t, err)
	assert.NotContains(t, ips, ip)

	err = repo.SetContainerIp(networkPrefix, "container-b", ip)
	assert.Nil(t, err)
}

func TestRemoveWorkerNetworkStateDeletesOnlyMachineScopedAllocations(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	assert.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewWorkerRedisRepositoryForTest(rdb)
	targetPrefix := common.WorkerNetworkPrefix("beta9", "machine-a")
	otherPrefix := common.WorkerNetworkPrefix("beta9", "machine-b")

	for i := 0; i < 24; i++ {
		containerID := fmt.Sprintf("network-slot:slot-%02d", i)
		ip := fmt.Sprintf("192.168.0.%d", i+2)
		assert.NoError(t, repo.SetContainerIp(targetPrefix, containerID, ip))
	}
	assert.NoError(t, repo.SetContainerIp(otherPrefix, "network-slot:other", "192.168.1.2"))

	ctx := context.Background()
	targetKeys, err := rdb.Keys(ctx, "worker:network:"+targetPrefix+"*")
	assert.NoError(t, err)
	assert.Equal(t, 50, len(targetKeys))
	routeRevisionKey := "scheduler:route:machine:{workspace-1}:pool:machine-a:rev"
	assert.NoError(t, rdb.Set(ctx, routeRevisionKey, "7", 0).Err())

	assert.NoError(t, repo.RemoveWorkerNetworkState(ctx, targetPrefix))
	assert.NoError(t, repo.RemoveWorkerNetworkState(ctx, targetPrefix))

	targetKeys, err = rdb.Keys(ctx, "worker:network:"+targetPrefix+"*")
	assert.NoError(t, err)
	assert.Empty(t, targetKeys)
	otherIP, err := repo.GetContainerIp(otherPrefix, "network-slot:other")
	assert.NoError(t, err)
	assert.Equal(t, "192.168.1.2", otherIP)
	routeRevision, err := rdb.Get(ctx, routeRevisionKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, "7", routeRevision)
}

func BenchmarkGetAllWorkers(b *testing.B) {
	rdb, err := NewRedisClientForTest()
	assert.NotNil(b, rdb)
	assert.Nil(b, err)

	repo := NewWorkerRedisRepositoryForTest(rdb)

	b.ResetTimer()

	maxDuration := time.Second
	for i := 0; i < b.N; i++ {
		start := time.Now()

		_, _ = repo.GetAllWorkers()

		duration := time.Since(start)
		b.Logf("GetAllWorkers, iteration %d took %v\n", i, duration)

		if duration > maxDuration {
			b.Fatalf("GetAllWorkers, iteration %d took more than %v\n", i, maxDuration)
		}
	}
}

func TestWorkerCreatedAtIsSetByTheFirstWriteOnly(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	now := time.Unix(1_800_000_000, 0)
	repo := NewWorkerRedisRepository(rdb, types.WorkerConfig{CleanupPendingWorkerAgeLimit: time.Hour},
		WithWorkerClock(func() time.Time { return now }))

	worker := &types.Worker{Id: "worker-created-at", Status: types.WorkerStatusPending, PoolName: "pool"}
	require.NoError(t, repo.AddWorker(worker))
	require.Equal(t, now.Unix(), worker.CreatedAt, "the caller's copy is stamped too")

	// A later write, whether it carries the old value, none, or another one,
	// does not make the worker look younger or older.
	now = now.Add(10 * time.Minute)
	for _, createdAt := range []int64{worker.CreatedAt, 0, now.Unix()} {
		update := &types.Worker{Id: worker.Id, Status: types.WorkerStatusPending, PoolName: "pool", CreatedAt: createdAt}
		require.NoError(t, repo.AddWorker(update))

		stored, err := repo.GetWorkerById(worker.Id)
		require.NoError(t, err)
		require.Equal(t, int64(1_800_000_000), stored.CreatedAt)
	}

	// Becoming available rewrites single fields and keeps it as well.
	require.NoError(t, repo.ToggleWorkerAvailable(worker.Id, ""))
	listed, err := repo.GetAllWorkersInPool("pool")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, int64(1_800_000_000), listed[0].CreatedAt)
}

func TestCordonWorkerIfIdle(t *testing.T) {
	newWorker := func(t *testing.T) (*WorkerRedisRepository, *types.Worker) {
		rdb, err := NewRedisClientForTest()
		require.NoError(t, err)
		repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)
		worker := &types.Worker{Id: "machine-1", Status: types.WorkerStatusAvailable,
			TotalCpu: 8000, FreeCpu: 8000, TotalMemory: 16000, FreeMemory: 16000,
			Gpu: "L40S", TotalGpuCount: 1, FreeGpuCount: 1}
		require.NoError(t, repo.AddWorker(worker))
		return repo, worker
	}
	status := func(t *testing.T, repo *WorkerRedisRepository, id string) (types.WorkerStatus, bool) {
		state, err := repo.GetWorkerById(id)
		require.NoError(t, err)
		return state.Status, state.CordonRequested
	}

	t.Run("an idle worker is cordoned", func(t *testing.T) {
		repo, worker := newWorker(t)
		cordoned, err := repo.CordonWorkerIfIdle(worker.Id)
		require.NoError(t, err)
		require.True(t, cordoned)
		workerStatus, requested := status(t, repo, worker.Id)
		require.Equal(t, types.WorkerStatusDisabled, workerStatus)
		require.True(t, requested)
	})

	t.Run("a worker with a queued request is left alone", func(t *testing.T) {
		repo, worker := newWorker(t)
		request := &types.ContainerRequest{ContainerId: "queued", Cpu: 1000, Memory: 1000, Gpu: "L40S", GpuCount: 1}
		setPendingContainerRequests(t, repo.rdb, request)
		require.NoError(t, repo.ScheduleContainerRequests(worker, []*types.ContainerRequest{request}))
		cordoned, err := repo.CordonWorkerIfIdle(worker.Id)
		require.NoError(t, err)
		require.False(t, cordoned)
		workerStatus, requested := status(t, repo, worker.Id)
		require.Equal(t, types.WorkerStatusAvailable, workerStatus)
		require.False(t, requested)
	})

	t.Run("a worker with a running container is left alone", func(t *testing.T) {
		repo, worker := newWorker(t)
		seedRunningContainer(t, repo.rdb, worker.Id, &types.ContainerState{ContainerId: "running", Cpu: 1000, Memory: 1000})
		cordoned, err := repo.CordonWorkerIfIdle(worker.Id)
		require.NoError(t, err)
		require.False(t, cordoned)
		workerStatus, _ := status(t, repo, worker.Id)
		require.Equal(t, types.WorkerStatusAvailable, workerStatus)
	})

	t.Run("a worker that is gone is an error", func(t *testing.T) {
		repo, _ := newWorker(t)
		cordoned, err := repo.CordonWorkerIfIdle("nobody")
		require.Error(t, err)
		require.False(t, cordoned)
	})
}

// A pool on spot capacity adds machines when it is asked for one, so its GPU
// counts as available while the pool has no worker.
func TestGpusOfSpotPoolsCountAsAvailable(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	onSpotNodes := types.WorkerPoolConfig{GPUType: "A10G"}
	onSpotNodes.JobSpec.NodeSelector = map[string]string{"zone": "us-east-1a", "karpenter.sh/capacity-type": "spot"}
	onOwnNodes := types.WorkerPoolConfig{GPUType: "T4"}
	onOwnNodes.JobSpec.NodeSelector = map[string]string{"karpenter.sh/capacity-type": "on-demand"}

	repo := NewWorkerRedisRepository(rdb, types.WorkerConfig{Pools: map[string]types.WorkerPoolConfig{
		"on-spot-nodes":   onSpotNodes,
		"more-spot-nodes": onSpotNodes,
		"says-so-itself":  {GPUType: "L40S", Preemptable: true},
		"on-own-nodes":    onOwnNodes,
		"cpu":             {Preemptable: true},
	}})

	require.ElementsMatch(t, []string{"A10G", "L40S"}, repo.GetPreemptibleGpus())

	available, err := repo.GetGpuAvailability()
	require.NoError(t, err)
	require.True(t, available["A10G"])
	require.True(t, available["L40S"])
	require.False(t, available["T4"], "a pool that is not on spot capacity has what its workers have")
}

// workerWithRequests adds a worker that is up, with the requests on its queue
// and room for more.
func workerWithRequests(t *testing.T, rdb *common.RedisClient, repo WorkerRepository, id string, requests ...*types.ContainerRequest) *types.Worker {
	t.Helper()

	worker := &types.Worker{
		Id: id, Status: types.WorkerStatusAvailable, PoolName: "default",
		FreeCpu: 10_000, FreeMemory: 10_000, TotalCpu: 10_000, TotalMemory: 10_000,
	}
	require.NoError(t, repo.AddWorker(worker))
	setPendingContainerRequests(t, rdb, requests...)
	require.NoError(t, repo.ScheduleContainerRequests(worker, requests))
	return worker
}

// lostWorkerWithRequests returns a worker whose state has expired with one
// request still on its queue and one it had taken off its queue but not
// claimed. (For one it had claimed, see claimedOnWorker.)
func lostWorkerWithRequests(t *testing.T, rdb *common.RedisClient, repo WorkerRepository) *types.Worker {
	t.Helper()

	worker := workerWithRequests(t, rdb, repo, "lost-worker",
		&types.ContainerRequest{ContainerId: "taken", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		&types.ContainerRequest{ContainerId: "queued", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
	)
	taken, err := repo.GetNextContainerRequests(worker.Id, 1)
	require.NoError(t, err)
	require.Len(t, taken, 1)

	// The worker stops sending heartbeats and its state expires.
	require.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id)).Err())
	return worker
}

// backlogRequests returns the requests that wait in a lane of the backlog, in
// the order they wait in.
func backlogRequests(t *testing.T, rdb *common.RedisClient, lane string) []types.ContainerRequest {
	t.Helper()

	members, err := rdb.ZRange(context.TODO(), lane, 0, -1).Result()
	require.NoError(t, err)
	requests := make([]types.ContainerRequest, len(members))
	for i, raw := range members {
		require.NoError(t, json.Unmarshal([]byte(raw), &requests[i]))
	}
	return requests
}

// laneContainerIDs names the containers whose requests wait in a lane.
func laneContainerIDs(t *testing.T, rdb *common.RedisClient, lane string) []string {
	t.Helper()

	requests := backlogRequests(t, rdb, lane)
	ids := make([]string, len(requests))
	for i, request := range requests {
		ids[i] = request.ContainerId
	}
	return ids
}

// backlogContainerIDs names those in the lane of requests that wait for
// capacity of their own.
func backlogContainerIDs(t *testing.T, rdb *common.RedisClient) []string {
	t.Helper()
	return laneContainerIDs(t, rdb, common.RedisKeys.SchedulerContainerRequests())
}

func requireNothingLeftOnWorker(t *testing.T, rdb *common.RedisClient, workerID string) {
	t.Helper()

	ctx := context.TODO()
	require.Zero(t, rdb.LLen(ctx, common.RedisKeys.SchedulerWorkerRequests(workerID)).Val())
	require.Zero(t, rdb.HLen(ctx, common.RedisKeys.SchedulerWorkerPendingRequests(workerID)).Val())
	for _, containerID := range []string{"taken", "queued"} {
		require.Empty(t, rdb.HGet(ctx, common.RedisKeys.SchedulerContainerState(containerID), "worker_id").Val())
	}
}

func TestListingWorkersRequeuesTheRequestsOfALostWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := lostWorkerWithRequests(t, rdb, repo)

	workers, err := repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)

	require.ElementsMatch(t, []string{"taken", "queued"}, backlogContainerIDs(t, rdb))
	requireNothingLeftOnWorker(t, rdb, worker.Id)

	// The worker is forgotten, so a second read has nothing to do.
	indexed, err := rdb.SMembers(context.TODO(), common.RedisKeys.SchedulerWorkerIndex()).Result()
	require.NoError(t, err)
	require.Empty(t, indexed)
	_, err = repo.GetAllWorkers()
	require.NoError(t, err)
	require.Len(t, backlogContainerIDs(t, rdb), 2)
}

func TestListingAPoolRequeuesTheRequestsOfALostWorker(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := lostWorkerWithRequests(t, rdb, repo)

	workers, err := repo.GetAllWorkersInPool("default")
	require.NoError(t, err)
	require.Empty(t, workers)

	require.ElementsMatch(t, []string{"taken", "queued"}, backlogContainerIDs(t, rdb))
	requireNothingLeftOnWorker(t, rdb, worker.Id)
}

func TestRemovingALostWorkerRequeuesItsRequests(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)
	worker := lostWorkerWithRequests(t, rdb, repo)

	// The pool cleaner removes a worker whose Job outlived its state. There
	// is no state left to delete, and the requests still have to move.
	err = repo.RemoveWorker(worker.Id)
	var notFound *types.ErrWorkerNotFound
	require.True(t, errors.As(err, &notFound), "got %v", err)

	require.ElementsMatch(t, []string{"taken", "queued"}, backlogContainerIDs(t, rdb))
	requireNothingLeftOnWorker(t, rdb, worker.Id)
}

func TestALostWorkerCannotStartARequestThatWasRequeued(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)

	request := &types.ContainerRequest{ContainerId: "taken", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100}
	worker := workerWithRequests(t, rdb, repo, "slow-worker", request)
	taken, err := repo.GetNextContainerRequests(worker.Id, 1)
	require.NoError(t, err)
	require.Len(t, taken, 1)

	require.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id)).Err())
	_, err = repo.GetAllWorkers()
	require.NoError(t, err)
	require.Equal(t, []string{"taken"}, backlogContainerIDs(t, rdb))

	// The worker was only slow. It must not start a container that is
	// already on its way to another worker.
	require.Error(t, repo.AddContainerToWorker(worker.Id, "taken", taken[0].DeliveryToken))
}

// A worker claims a request before it starts the container, and the claim
// takes the request off the worker for good: from then on there is nothing
// to return to the backlog. If the worker is lost before the container has
// started, the container's state says PENDING until its lease runs out, half
// an hour later, and for all that time its owner counts a container that is
// starting. So when the worker is forgotten or removed, the container is
// ended as a stop request would end it, and its owner starts another.

// claimedOnWorker is a worker with four containers on its index:
//
//	claimed  the worker claimed it and has not started it
//	running  the worker started it
//	queued   the worker has not taken it off its queue
//	moved    another worker's by now, and still listed here
type claimedOnWorker struct {
	rdb    *common.RedisClient
	repo   *WorkerRedisRepository
	worker *types.Worker
}

func newClaimedOnWorker(t *testing.T) *claimedOnWorker {
	t.Helper()
	ctx := context.TODO()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	f := &claimedOnWorker{rdb: rdb, repo: NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)}

	f.worker = workerWithRequests(t, rdb, f.repo, "worker",
		&types.ContainerRequest{ContainerId: "claimed", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		&types.ContainerRequest{ContainerId: "running", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
	)
	taken, err := f.repo.GetNextContainerRequests(f.worker.Id, 2)
	require.NoError(t, err)
	require.Len(t, taken, 2)
	for _, request := range taken {
		require.NoError(t, f.repo.AddContainerToWorker(f.worker.Id, request.ContainerId, request.DeliveryToken))
	}
	require.NoError(t, rdb.HSet(ctx, common.RedisKeys.SchedulerContainerState("running"), "status", string(types.ContainerStatusRunning)).Err())

	queued := &types.ContainerRequest{ContainerId: "queued", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100}
	setPendingContainerRequests(t, rdb, queued)
	require.NoError(t, f.repo.ScheduleContainerRequests(f.worker, []*types.ContainerRequest{queued}))

	moved := common.RedisKeys.SchedulerContainerState("moved")
	require.NoError(t, rdb.HSet(ctx, moved, "container_id", "moved", "status", string(types.ContainerStatusPending), "worker_id", "another-worker").Err())
	require.NoError(t, rdb.SAdd(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(f.worker.Id), moved).Err())

	// Each state has the lease of a container that is starting.
	for _, id := range []string{"claimed", "running", "queued", "moved"} {
		require.NoError(t, rdb.Expire(ctx, common.RedisKeys.SchedulerContainerState(id), time.Duration(types.ContainerStateTtlSWhilePending)*time.Second).Err())
	}
	return f
}

func (f *claimedOnWorker) state(t *testing.T, containerID string) (status types.ContainerStatus, workerID string, lease time.Duration) {
	t.Helper()
	ctx := context.TODO()
	key := common.RedisKeys.SchedulerContainerState(containerID)
	fields, err := f.rdb.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	return types.ContainerStatus(fields["status"]), fields["worker_id"], f.rdb.TTL(ctx, key).Val()
}

// requireClaimedContainerEnded checks what became of each container once the
// worker was forgotten or removed.
func (f *claimedOnWorker) requireClaimedContainerEnded(t *testing.T) {
	t.Helper()
	pendingLease := time.Duration(types.ContainerStateTtlSWhilePending) * time.Second
	stoppingLease := time.Duration(types.ContainerStateTtlSWhileStopping) * time.Second

	status, owner, lease := f.state(t, "claimed")
	require.Equal(t, types.ContainerStatusStopping, status, "its owner no longer counts it as starting")
	require.Equal(t, f.worker.Id, owner)
	require.Equal(t, stoppingLease, lease, "and it is gone once a stopping container's time is up")
	// The index of container states says when each expires.
	requireLease(t, f.rdb, "claimed", types.ContainerStateTtlSWhileStopping)

	// A container that had started lives and dies by its own lease.
	status, owner, lease = f.state(t, "running")
	require.Equal(t, types.ContainerStatusRunning, status)
	require.Equal(t, f.worker.Id, owner)
	require.Equal(t, pendingLease, lease)

	// A request the worker never claimed goes back to the backlog.
	status, owner, _ = f.state(t, "queued")
	require.Equal(t, types.ContainerStatusPending, status)
	require.Empty(t, owner)
	require.Equal(t, []string{"queued"}, backlogContainerIDs(t, f.rdb), "what was claimed cannot be placed again: its request is gone")

	// Another worker's container is not this worker's to end.
	status, owner, lease = f.state(t, "moved")
	require.Equal(t, types.ContainerStatusPending, status)
	require.Equal(t, "another-worker", owner)
	require.Equal(t, pendingLease, lease)
}

func TestAContainerClaimedByAWorkerThatIsForgottenIsEnded(t *testing.T) {
	f := newClaimedOnWorker(t)

	// The worker stops sending keepalives and its state expires.
	require.NoError(t, f.rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(f.worker.Id)).Err())
	workers, err := f.repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)

	f.requireClaimedContainerEnded(t)

	// Looking again changes nothing.
	_, err = f.repo.GetAllWorkers()
	require.NoError(t, err)
	f.requireClaimedContainerEnded(t)
}

func TestAContainerClaimedByAWorkerThatIsRemovedIsEnded(t *testing.T) {
	f := newClaimedOnWorker(t)

	require.NoError(t, f.repo.RemoveWorker(f.worker.Id))

	f.requireClaimedContainerEnded(t)
}

// A worker is forgotten when it is on the index and has no state. Between
// the read that found no state and the forgetting, the worker can have been
// put on record again: a machine's worker is, by its pool, when its record
// has expired. That worker is not lost.
func TestAWorkerThatIsOnRecordAgainIsNotForgotten(t *testing.T) {
	f := newClaimedOnWorker(t)
	ctx := context.TODO()
	stateKey := common.RedisKeys.SchedulerWorkerState(f.worker.Id)

	f.repo.forgetLostWorker(ctx, stateKey, common.RedisKeys.SchedulerWorkerIndex())

	workers, err := f.repo.GetAllWorkers()
	require.NoError(t, err)
	require.Len(t, workers, 1, "the worker is still on the index")
	require.EqualValues(t, 1, f.rdb.LLen(ctx, common.RedisKeys.SchedulerWorkerRequests(f.worker.Id)).Val(), "with the request that is queued on it")
	require.Empty(t, backlogContainerIDs(t, f.rdb))
	status, _, _ := f.state(t, "claimed")
	require.Equal(t, types.ContainerStatusPending, status)
}

// Whoever writes a worker's record holds the worker's lock. A read that
// finds the worker without a state while the lock is held leaves it for the
// next read to look at.
func TestAWorkerIsNotForgottenWhileItsRecordIsBeingWritten(t *testing.T) {
	f := newClaimedOnWorker(t)
	ctx := context.TODO()
	require.NoError(t, f.rdb.Del(ctx, common.RedisKeys.SchedulerWorkerState(f.worker.Id)).Err())

	writer := common.NewRedisLock(f.rdb)
	lockKey := common.RedisKeys.SchedulerWorkerLock(f.worker.Id)
	require.NoError(t, writer.Acquire(ctx, lockKey, schedulerWorkerLockOptions))

	workers, err := f.repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)
	require.Empty(t, backlogContainerIDs(t, f.rdb))
	indexed, err := f.rdb.SMembers(ctx, common.RedisKeys.SchedulerWorkerIndex()).Result()
	require.NoError(t, err)
	require.Len(t, indexed, 1)

	// The writer is done and wrote nothing: the worker is lost after all.
	require.NoError(t, writer.Release(lockKey))
	_, err = f.repo.GetAllWorkers()
	require.NoError(t, err)
	f.requireClaimedContainerEnded(t)
	indexed, err = f.rdb.SMembers(ctx, common.RedisKeys.SchedulerWorkerIndex()).Result()
	require.NoError(t, err)
	require.Empty(t, indexed)
}

// The backlog has two lanes. Work that only fills spare capacity waits in a
// lane of its own, which the scheduler takes from after the other, so that it
// never holds up a request that is waiting for capacity of its own. A request
// that comes back from a lost worker belongs in the lane it was taken from.

func TestRequestsOfALostWorkerReturnToTheLaneTheyCameFrom(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)

	requests := []*types.ContainerRequest{
		{ContainerId: "serverless", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		{ContainerId: "replica", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, Evictable: true},
		{ContainerId: "spare-only", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, OpportunisticOnly: true},
		{ContainerId: "spot", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, Evictable: true, OpportunisticOnly: true},
	}
	for _, request := range requests {
		require.Equal(t, request.ContainerId != "serverless", BacklogLane(request) == common.RedisKeys.SchedulerBackgroundRequests())
	}
	worker := workerWithRequests(t, rdb, repo, "lost-worker", requests...)
	// The worker had taken some of them and started none.
	taken, err := repo.GetNextContainerRequests(worker.Id, 2)
	require.NoError(t, err)
	require.Len(t, taken, 2)

	// Something else waits in each lane already.
	for _, waiting := range []*types.ContainerRequest{{ContainerId: "waiting"}, {ContainerId: "waiting-replica", Evictable: true}} {
		member, err := json.Marshal(waiting)
		require.NoError(t, err)
		require.NoError(t, rdb.ZAdd(context.TODO(), BacklogLane(waiting), redis.Z{Score: 1, Member: member}).Err())
	}

	// The worker stops sending keepalives and its state expires.
	require.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id)).Err())
	workers, err := repo.GetAllWorkers()
	require.NoError(t, err)
	require.Empty(t, workers)

	require.Equal(t, []string{"waiting", "serverless"}, laneContainerIDs(t, rdb, common.RedisKeys.SchedulerContainerRequests()))
	require.ElementsMatch(t, []string{"waiting-replica", "replica", "spare-only", "spot"}, laneContainerIDs(t, rdb, common.RedisKeys.SchedulerBackgroundRequests()))
	require.EqualValues(t, 6, vmetrics.GetDefaultSet().GetOrCreateGauge("scheduler_backlog_depth", nil).Get(),
		"the backlog's depth is what waits in both lanes")
}

// A worker's requests are moved all together or not at all, and nothing is
// moved into a lane that is not a backlog.
func TestRequestsOfALostWorkerStayOnItWhenALaneCannotTakeThem(t *testing.T) {
	for _, broken := range []string{common.RedisKeys.SchedulerContainerRequests(), common.RedisKeys.SchedulerBackgroundRequests()} {
		t.Run(broken, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			repo := NewWorkerRedisRepositoryForTest(rdb)
			worker := workerWithRequests(t, rdb, repo, "lost-worker",
				&types.ContainerRequest{ContainerId: "serverless", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
				&types.ContainerRequest{ContainerId: "replica", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, Evictable: true},
			)
			require.NoError(t, rdb.Set(context.TODO(), broken, "not a sorted set", 0).Err())

			require.NoError(t, rdb.Del(context.TODO(), common.RedisKeys.SchedulerWorkerState(worker.Id)).Err())
			_, err = repo.GetAllWorkers()
			require.NoError(t, err)

			require.EqualValues(t, 2, rdb.LLen(context.TODO(), common.RedisKeys.SchedulerWorkerRequests(worker.Id)).Val())
			indexed, err := rdb.SMembers(context.TODO(), common.RedisKeys.SchedulerWorkerIndex()).Result()
			require.NoError(t, err)
			require.Len(t, indexed, 1, "the worker stays on the index, so the next read tries again")
		})
	}
}

// A request is stamped with the machine it is placed on. When that machine's
// worker is lost, the request goes back to the backlog free to run anywhere:
// only a machine its caller chose still binds it.
func TestARequeuedRequestIsNotBoundToTheMachineItWasPlacedOn(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{
		Id: "worker-1", MachineId: "machine-1", Status: types.WorkerStatusAvailable, PoolName: "spot-l40s",
		FreeCpu: 10_000, FreeMemory: 10_000, TotalCpu: 10_000, TotalMemory: 10_000,
	}
	require.NoError(t, repo.AddWorker(worker))
	requests := []*types.ContainerRequest{
		{ContainerId: "taken", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		{ContainerId: "queued", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100},
		{ContainerId: "pinned", WorkspaceId: "workspace", StubId: "stub", Cpu: 100, Memory: 100, MachineId: "machine-1"},
	}
	setPendingContainerRequests(t, rdb, requests...)
	require.NoError(t, repo.ScheduleContainerRequests(worker, requests))

	// On the worker, each request names the machine it runs on.
	taken, err := repo.GetNextContainerRequests(worker.Id, 1)
	require.NoError(t, err)
	require.Len(t, taken, 1)
	require.Equal(t, "taken", taken[0].ContainerId)
	require.Equal(t, "machine-1", taken[0].MachineId)
	require.True(t, taken[0].MachineAssigned)

	// The machine is reclaimed.
	require.NoError(t, repo.RemoveWorker(worker.Id))

	requeued := map[string]types.ContainerRequest{}
	for _, request := range backlogRequests(t, rdb, common.RedisKeys.SchedulerContainerRequests()) {
		requeued[request.ContainerId] = request
	}
	require.Len(t, requeued, 3)
	for _, id := range []string{"taken", "queued"} {
		require.Empty(t, requeued[id].MachineId, "%s is still bound to the machine that is gone", id)
		require.False(t, requeued[id].MachineAssigned)
	}
	require.Equal(t, "machine-1", requeued["pinned"].MachineId, "its caller chose the machine")
	require.False(t, requeued["pinned"].MachineAssigned)
}

// A worker's state expires once the worker has sent no keepalive for
// types.WorkerStateTtlS. That is how a worker that died is noticed, and what
// returns the requests queued on it to the backlog (see forgetLostWorker). A
// pool controller saves the state too, each time it reconciles its pool, and
// that must not keep the state of a worker that died alive.

const workerKeepAliveLifetime = time.Duration(types.WorkerStateTtlS) * time.Second

func TestSavingAWorkerRenewsItsStateOnlyWhileItIsStarting(t *testing.T) {
	ctx := context.TODO()
	const left = 30 * time.Second

	// lifetimeAfterSaving returns how long the state of a worker has left
	// once its controller has saved it again. The worker is added with the
	// given status; became, if set, is what happens to it before then.
	lifetimeAfterSaving := func(t *testing.T, status types.WorkerStatus, became func(repo *WorkerRedisRepository, id string) error) (*WorkerRedisRepository, time.Duration) {
		rdb, err := NewRedisClientForTest()
		require.NoError(t, err)
		repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)
		worker := &types.Worker{Id: "worker-1", MachineId: "machine-1", Status: status, PoolName: "joined-a10g"}
		require.NoError(t, repo.AddWorker(worker))
		key := common.RedisKeys.SchedulerWorkerState(worker.Id)
		require.Equal(t, repo.config.CleanupPendingWorkerAgeLimit, rdb.TTL(ctx, key).Val(), "a new worker has the pending lifetime to start in")
		if became != nil {
			require.NoError(t, became(repo, worker.Id))
		}

		// Most of its lifetime has passed when its pool is reconciled.
		require.NoError(t, rdb.Expire(ctx, key, left).Err())
		current, err := repo.GetWorkerById(worker.Id)
		require.NoError(t, err)
		require.NoError(t, repo.AddWorker(current))
		return repo, rdb.TTL(ctx, key).Val()
	}

	t.Run("a worker that is starting gets its full lifetime again", func(t *testing.T) {
		repo, lifetime := lifetimeAfterSaving(t, types.WorkerStatusPending, nil)
		require.Equal(t, repo.config.CleanupPendingWorkerAgeLimit, lifetime)
	})

	t.Run("so does one that is being replaced, which is silent until its replacement starts", func(t *testing.T) {
		repo, lifetime := lifetimeAfterSaving(t, types.WorkerStatusAvailable, func(repo *WorkerRedisRepository, id string) error {
			_, err := repo.PrepareWorkerRollout(id, "generation-2")
			return err
		})
		require.Equal(t, repo.config.CleanupPendingWorkerAgeLimit, lifetime)
	})

	t.Run("a worker that is up lives by its own keepalives", func(t *testing.T) {
		_, lifetime := lifetimeAfterSaving(t, types.WorkerStatusAvailable, nil)
		require.Equal(t, left, lifetime)
	})

	t.Run("and so does one that is cordoned", func(t *testing.T) {
		_, lifetime := lifetimeAfterSaving(t, types.WorkerStatusAvailable, func(repo *WorkerRedisRepository, id string) error {
			return repo.SetWorkerCordon(id, true)
		})
		require.Equal(t, left, lifetime)
	})
}

// expireBeforeSave makes a worker's state expire after it has been read and
// before the transaction that saves it.
type expireBeforeSave struct {
	rdb *common.RedisClient
	key string
}

func (h *expireBeforeSave) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *expireBeforeSave) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (h *expireBeforeSave) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() == "hset" && len(cmd.Args()) > 1 && cmd.Args()[1] == h.key {
				if err := h.rdb.Del(ctx, h.key).Err(); err != nil {
					return err
				}
				break
			}
		}
		return next(ctx, cmds)
	}
}

// A state that is saved without a lifetime has to have one already. If it
// expired a moment before, the save would make a state that never expires:
// a worker that died would stay on record, taking requests, for good.
func TestSavingAWorkerWhoseStateJustExpiredDoesNotLeaveItOnRecordForGood(t *testing.T) {
	ctx := context.TODO()
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	repo := NewWorkerRedisRepositoryForTest(rdb)

	worker := &types.Worker{Id: "worker-1", MachineId: "machine-1", Status: types.WorkerStatusAvailable, PoolName: "joined-a10g"}
	require.NoError(t, repo.AddWorker(worker))
	current, err := repo.GetWorkerById(worker.Id)
	require.NoError(t, err)

	key := common.RedisKeys.SchedulerWorkerState(worker.Id)
	rdb.AddHook(&expireBeforeSave{rdb: rdb, key: key})
	require.NoError(t, repo.AddWorker(current))

	require.Equal(t, workerKeepAliveLifetime, rdb.TTL(ctx, key).Val(), "it has one keepalive's time to show that it is alive")
}

// A worker that is starting sends no keepalive yet. The pool that waits for
// it gives its state its full lifetime again; one that is up lives by its
// own.
func TestKeepPendingWorker(t *testing.T) {
	ctx := context.TODO()
	newWorker := func(t *testing.T, status types.WorkerStatus) (*WorkerRedisRepository, string) {
		rdb, err := NewRedisClientForTest()
		require.NoError(t, err)
		repo := NewWorkerRedisRepositoryForTest(rdb).(*WorkerRedisRepository)
		require.NoError(t, repo.AddWorker(&types.Worker{Id: "worker-1", MachineId: "machine-1", Status: status, PoolName: "spot-l40s"}))
		// Most of its lifetime has passed.
		key := common.RedisKeys.SchedulerWorkerState("worker-1")
		require.NoError(t, repo.rdb.Expire(ctx, key, 30*time.Second).Err())
		return repo, key
	}

	t.Run("a pending worker gets its full lifetime again", func(t *testing.T) {
		repo, key := newWorker(t, types.WorkerStatusPending)
		kept, err := repo.KeepPendingWorker("worker-1")
		require.NoError(t, err)
		require.True(t, kept)
		require.Equal(t, repo.config.CleanupPendingWorkerAgeLimit, repo.rdb.TTL(ctx, key).Val())
	})

	t.Run("a worker that is up lives by its own heartbeat", func(t *testing.T) {
		repo, key := newWorker(t, types.WorkerStatusAvailable)
		kept, err := repo.KeepPendingWorker("worker-1")
		require.NoError(t, err)
		require.True(t, kept)
		require.Equal(t, 30*time.Second, repo.rdb.TTL(ctx, key).Val())
	})

	t.Run("a worker that is gone is not made again", func(t *testing.T) {
		repo, _ := newWorker(t, types.WorkerStatusPending)
		kept, err := repo.KeepPendingWorker("nobody")
		require.NoError(t, err)
		require.False(t, kept)
		_, err = repo.GetWorkerById("nobody")
		var notFound *types.ErrWorkerNotFound
		require.ErrorAs(t, err, &notFound)
	})
}

// Three kinds of work are done for a pool by one scheduler replica at a
// time: sizing it, reporting its state and cleaning up after its workers.
// Each has a lock, held for as long as the work runs.

// poolLocksFixture is a Redis with two scheduler replicas' view of its pool
// locks, and a count of the round trips made to it while counting is on.
type poolLocksFixture struct {
	server        *miniredis.Miniredis
	first, second *WorkerPoolRedisRepository
	roundTrips    *redisRoundTrips
}

func newPoolLocksForTest(t *testing.T) *poolLocksFixture {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	roundTrips := &redisRoundTrips{}
	rdb.AddHook(roundTrips)
	replica := func() *WorkerPoolRedisRepository {
		return NewWorkerPoolRedisRepositoryForTest(rdb).(*WorkerPoolRedisRepository)
	}
	return &poolLocksFixture{server: server, first: replica(), second: replica(), roundTrips: roundTrips}
}

func TestEachKindOfPoolWorkHasItsOwnLock(t *testing.T) {
	type lock func(*WorkerPoolRedisRepository, string, func()) error
	sizer := lock((*WorkerPoolRedisRepository).WithWorkerPoolSizerLock)
	state := lock((*WorkerPoolRedisRepository).WithWorkerPoolStateLock)
	cleaner := lock((*WorkerPoolRedisRepository).WithWorkerPoolCleanerLock)

	tests := []struct {
		name string
		lock lock
		key  string
		ttl  time.Duration
		// attempts is how often a replica asks for a lock that is held
		// before it leaves the work to the holder.
		attempts int64
		others   []lock
	}{
		{"sizing", sizer, common.RedisKeys.WorkerPoolSizerLock("gpu"), 30 * time.Second, 1, []lock{state, cleaner}},
		{"reporting state", state, common.RedisKeys.WorkerPoolStateLock("gpu"), 10 * time.Second, 1, []lock{sizer, cleaner}},
		{"cleaning", cleaner, common.RedisKeys.WorkerPoolCleanerLock("gpu"), 10 * time.Second, 4, []lock{sizer, state}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newPoolLocksForTest(t)
			server := f.server
			ran := func(lock lock, pool string) (bool, error) {
				ran := false
				err := lock(f.second, pool, func() { ran = true })
				return ran, err
			}

			err := test.lock(f.first, "gpu", func() {
				require.True(t, server.Exists(test.key))
				require.Equal(t, test.ttl, server.TTL(test.key))

				// Another replica leaves the work to this one.
				f.roundTrips.enabled.Store(true)
				did, err := ran(test.lock, "gpu")
				f.roundTrips.enabled.Store(false)
				require.ErrorIs(t, err, redislock.ErrNotObtained)
				require.False(t, did)
				require.Equal(t, test.attempts, f.roundTrips.calls.Load())

				// The pool's other work is not held up, nor is another pool.
				for _, other := range test.others {
					did, err := ran(other, "gpu")
					require.NoError(t, err)
					require.True(t, did)
				}
				did, err = ran(test.lock, "cpu")
				require.NoError(t, err)
				require.True(t, did)
			})
			require.NoError(t, err)

			// The lock is given up when the work is done.
			require.False(t, server.Exists(test.key))
			did, err := ran(test.lock, "gpu")
			require.NoError(t, err)
			require.True(t, did)
		})
	}
}

// A pass can take longer than its lock's TTL: cleaning lists every Job of
// the pool and each Job's pods. The lock is renewed while the pass runs, so
// that a second replica does not start the same work beside it.
func TestAPoolLockOutlastsItsTTLWhileItsWorkRuns(t *testing.T) {
	f := newPoolLocksForTest(t)
	server, first, second := f.server, f.first, f.second
	const key = "pool-lock"
	// A short TTL, so that the test need not wait long for a renewal.
	options := common.RedisLockOptions{TtlS: 1}

	working, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- first.withPoolLock(key, options, func() {
			close(working)
			<-finish
		})
	}()
	<-working

	// The work goes on for three times the lock's TTL.
	for range 3 {
		server.FastForward(900 * time.Millisecond)
		require.Eventually(t, func() bool { return server.TTL(key) > 900*time.Millisecond },
			2*time.Second, 10*time.Millisecond, "the lock was not renewed")
	}
	ran := false
	err := second.withPoolLock(key, options, func() { ran = true })
	require.ErrorIs(t, err, redislock.ErrNotObtained)
	require.False(t, ran)

	close(finish)
	require.NoError(t, <-done)
	require.NoError(t, second.withPoolLock(key, options, func() { ran = true }))
	require.True(t, ran)
}

// The lock that is held is the one the previous release takes and gives up,
// so that replicas of both releases keep each other out while a release is
// rolled out.
func TestAPoolLockKeepsOutAReplicaOfThePreviousRelease(t *testing.T) {
	f := newPoolLocksForTest(t)
	server, replica := f.server, f.first
	previous := common.NewRedisLock(replica.rdb)
	ctx := context.Background()

	for _, lock := range []struct {
		key     string
		options common.RedisLockOptions
		with    func(string, func()) error
	}{
		{common.RedisKeys.WorkerPoolStateLock("gpu"), common.RedisLockOptions{TtlS: 10, Retries: 0}, replica.WithWorkerPoolStateLock},
		{common.RedisKeys.WorkerPoolCleanerLock("gpu"), common.RedisLockOptions{TtlS: 10, Retries: 3}, replica.WithWorkerPoolCleanerLock},
	} {
		// While this release works, the previous one cannot take the lock.
		require.NoError(t, lock.with("gpu", func() {
			require.ErrorIs(t, previous.Acquire(ctx, lock.key, lock.options), redislock.ErrNotObtained)
		}))

		// While the previous one holds it, this release does not work.
		require.NoError(t, previous.Acquire(ctx, lock.key, lock.options))
		ran := false
		require.ErrorIs(t, lock.with("gpu", func() { ran = true }), redislock.ErrNotObtained)
		require.False(t, ran)
		require.NoError(t, previous.Release(lock.key))
		require.False(t, server.Exists(lock.key))
	}
}

func TestOnlyOneReplicaSizesAPoolAtATime(t *testing.T) {
	f := newPoolLocksForTest(t)
	first, second := f.first, f.second

	// The first replica is in the middle of sizing: buying a machine, say.
	sizing, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- first.WithWorkerPoolSizerLock("spot-l40s", func() {
			close(sizing)
			<-finish
		})
	}()
	<-sizing

	ran := false
	err := second.WithWorkerPoolSizerLock("spot-l40s", func() { ran = true })
	require.True(t, errors.Is(err, redislock.ErrNotObtained), "got %v", err)
	require.False(t, ran)

	// Another pool is not held up.
	require.NoError(t, second.WithWorkerPoolSizerLock("spot-a10g", func() { ran = true }))
	require.True(t, ran)

	// Once the first is done, the next tick of either may size the pool.
	close(finish)
	require.NoError(t, <-done)
	ran = false
	require.NoError(t, second.WithWorkerPoolSizerLock("spot-l40s", func() { ran = true }))
	require.True(t, ran)
}

func TestProviderOwnerIsChosenOnceAndSharedByEveryGateway(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	ctx := context.Background()

	owner, err := NewComputeRedisRepository(rdb).ProviderOwner(ctx)
	require.NoError(t, err)
	require.Regexp(t, `^beam-[0-9a-f]{12}$`, owner)

	again, err := NewComputeRedisRepository(rdb).ProviderOwner(ctx)
	require.NoError(t, err)
	require.Equal(t, owner, again)

	elsewhere, err := NewRedisClientForTest()
	require.NoError(t, err)
	other, err := NewComputeRedisRepository(elsewhere).ProviderOwner(ctx)
	require.NoError(t, err)
	require.NotEqual(t, owner, other, "a deployment with its own records has its own name")
}
