package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/tj/assert"
)

func TestRetryIncrementsRetryCount(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Timestamp:   time.Now(),
	}

	newSchedulingAttempt(scheduler, request, nil).retry("test")
	assert.Equal(t, 1, request.RetryCount)

	time.Sleep(requestProcessingInterval + 10*time.Millisecond)
	requeuedRequest, err := scheduler.requestBacklog.Pop()
	assert.Nil(t, err)
	assert.Equal(t, request.ContainerId, requeuedRequest.ContainerId)
	assert.Equal(t, 1, requeuedRequest.RetryCount)
}

func TestPrivatePoolMissFallsBackToRegularAvailableWorker(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	workspaceID := "workspace-private-fallback"

	privateController := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "private-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		requiresSelector: true,
		cannotAddWorker:  true,
	}
	state := &compute.PoolState{Selector: "private-cpu", Mode: string(types.PoolModePrivate)}
	scheduler.workerPoolManager.SetPoolAt(agentPoolControllerKey(workspaceID, state), "private-cpu", types.WorkerPoolConfig{
		Mode:                 types.PoolModePrivate,
		RequiresPoolSelector: true,
	}, privateController)

	privateWorker := &types.Worker{
		Id:                   "private-worker",
		Status:               types.WorkerStatusAvailable,
		FreeCpu:              100,
		FreeMemory:           2000,
		PoolName:             "private-cpu",
		RequiresPoolSelector: true,
	}
	regularWorker := &types.Worker{
		Id:         "regular-worker",
		Status:     types.WorkerStatusAvailable,
		FreeCpu:    2000,
		FreeMemory: 2000,
		PoolName:   "beta9-cpu",
	}
	assert.Nil(t, scheduler.workerRepo.AddWorker(privateWorker))
	assert.Nil(t, scheduler.workerRepo.AddWorker(regularWorker))

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		WorkspaceId:  workspaceID,
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "private-cpu",
		Timestamp:    time.Now(),
		Workspace:    testWorkspaceWithStorage(),
	}
	withoutStorage := request.Clone()
	withoutStorage.Workspace = types.Workspace{}
	withoutStorage.Env = []string{"BETA9_TOKEN=user-token"}
	withoutStorage = withoutStorage.PrivateWorkerRequest()
	privateController.cannotAddWorker = false
	fallback, poolName, ok := newSchedulingAttempt(scheduler, withoutStorage, nil).privatePoolFallbackRequest()
	assert.True(t, ok)
	assert.Equal(t, "private-cpu", poolName)
	assert.Empty(t, fallback.PoolSelector)
	assert.True(t, fallback.RuntimeTokenRequired)
	assert.Empty(t, fallback.Env)
	privateController.cannotAddWorker = true

	setPendingSchedulerRequests(t, scheduler, request)
	scheduler.processRequestBatch([]*types.ContainerRequest{request}, []*types.Worker{privateWorker, regularWorker})
	// The request went back to the backlog without its pool. The next pass
	// places it.
	assert.Equal(t, 1, scheduler.ProcessBacklog())

	queued, err := nextContainerRequest(scheduler.workerRepo, regularWorker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queued)
	assert.Equal(t, request.ContainerId, queued.ContainerId)
	assert.Equal(t, "", queued.PoolSelector)

	privateAfter, err := scheduler.workerRepo.GetWorkerById(privateWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(100), privateAfter.FreeCpu)
	assert.Equal(t, 0, privateController.AddWorkerCallCount())
}
func TestPrivatePoolMissWithDurableDiskDoesNotFallback(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	workspaceID := "workspace-private-durable"

	privateController := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "private-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		requiresSelector: true,
		cannotAddWorker:  true,
	}
	state := &compute.PoolState{Selector: "private-cpu", Mode: string(types.PoolModePrivate)}
	scheduler.workerPoolManager.SetPoolAt(agentPoolControllerKey(workspaceID, state), "private-cpu", types.WorkerPoolConfig{
		Mode:                 types.PoolModePrivate,
		RequiresPoolSelector: true,
	}, privateController)

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		WorkspaceId:  workspaceID,
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "private-cpu",
		Timestamp:    time.Now(),
		Mounts: []types.Mount{
			{
				MountType:   types.StorageModeDurableDisk,
				DurableDisk: &types.DurableDiskMountConfig{Name: "pg-data"},
			},
		},
	}

	fallback, poolName, ok := newSchedulingAttempt(scheduler, request, nil).privatePoolFallbackRequest()
	assert.False(t, ok)
	assert.Nil(t, fallback)
	assert.Equal(t, "", poolName)
}

func TestPrivatePoolMissWithoutRegularCapacityKeepsPrivateSelector(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()
	workspaceID := "workspace-private-no-capacity"

	privateController := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "private-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		requiresSelector: true,
		cannotAddWorker:  true,
	}
	state := &compute.PoolState{Selector: "private-cpu", Mode: string(types.PoolModePrivate)}
	scheduler.workerPoolManager.SetPoolAt(agentPoolControllerKey(workspaceID, state), "private-cpu", types.WorkerPoolConfig{
		Mode:                 types.PoolModePrivate,
		RequiresPoolSelector: true,
	}, privateController)

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		WorkspaceId:  workspaceID,
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "private-cpu",
		Timestamp:    time.Now(),
		Workspace:    testWorkspaceWithStorage(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	scheduler.processRequestBatch([]*types.ContainerRequest{request}, nil)

	time.Sleep(provisioningWorkerRequeueDelay + requestProcessingInterval)
	requeued, err := scheduler.requestBacklog.Pop()
	assert.Nil(t, err)
	assert.Equal(t, request.ContainerId, requeued.ContainerId)
	assert.Equal(t, "private-cpu", requeued.PoolSelector)
	assert.Equal(t, 0, privateController.AddWorkerCallCount())
	assert.Empty(t, request.GrowAttempts)
}

func TestPrivatePoolMissWithExhaustedRegularAgentKeepsPrivateSelector(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()
	workspaceID := "workspace-private-exhausted-regular-agent"

	privateController := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "private-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		requiresSelector: true,
		cannotAddWorker:  true,
	}
	privateState := &compute.PoolState{Selector: "private-cpu", Mode: string(types.PoolModePrivate)}
	scheduler.workerPoolManager.SetPoolAt(agentPoolControllerKey(workspaceID, privateState), "private-cpu", types.WorkerPoolConfig{
		Mode:                 types.PoolModePrivate,
		RequiresPoolSelector: true,
	}, privateController)

	regularController := &LocalWorkerPoolControllerForTest{
		ctx:             context.Background(),
		name:            "regular-agent-cpu",
		config:          scheduler.config,
		workerRepo:      scheduler.workerRepo,
		cannotAddWorker: true,
	}
	scheduler.workerPoolManager.SetPool(regularController.name, types.WorkerPoolConfig{
		Mode: types.PoolModeExternal,
	}, regularController)

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		WorkspaceId:  workspaceID,
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "private-cpu",
		Timestamp:    time.Now(),
		Workspace:    testWorkspaceWithStorage(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	scheduler.processRequestBatch([]*types.ContainerRequest{request}, nil)

	time.Sleep(provisioningWorkerRequeueDelay + requestProcessingInterval)
	requeued, err := scheduler.requestBacklog.Pop()
	assert.Nil(t, err)
	assert.Equal(t, request.ContainerId, requeued.ContainerId)
	assert.Equal(t, "private-cpu", requeued.PoolSelector)
	assert.Equal(t, 0, privateController.AddWorkerCallCount())
	assert.Equal(t, 0, regularController.AddWorkerCallCount())
	assert.Empty(t, request.GrowAttempts)
}
