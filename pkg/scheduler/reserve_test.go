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

func TestRetrySoonIncrementsRetryCount(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	request := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Timestamp:   time.Now(),
	}

	newSchedulingAttempt(scheduler, request, nil).retrySoon("test")
	assert.Equal(t, 1, request.RetryCount)

	time.Sleep(requestProcessingInterval + 10*time.Millisecond)
	requeuedRequest, err := scheduler.requestBacklog.Pop()
	assert.Nil(t, err)
	assert.Equal(t, request.ContainerId, requeuedRequest.ContainerId)
	assert.Equal(t, 1, requeuedRequest.RetryCount)
}

func TestProvisioningFailureBackoffSkipsImmediateAddWorkerRetry(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	started := make(chan struct{}, 2)
	controller := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "beta9-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: started,
		addWorkerErr:     types.NewProviderNotImplemented(),
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)

	firstRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, firstRequest, nil).provisionWorker()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected first provisioning attempt")
	}

	deadline := time.After(time.Second)
	for scheduler.workerProvisioningBackoff.canAttempt("beta9-cpu") {
		select {
		case <-deadline:
			t.Fatal("expected provisioning failure to back off add-worker attempts")
		case <-time.After(time.Millisecond):
		}
	}
	assert.Equal(t, 1, controller.AddWorkerCallCount())

	secondRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, secondRequest, nil).provisionWorker()
	time.Sleep(2 * requestProcessingInterval)

	assert.Equal(t, 1, controller.AddWorkerCallCount())
}

func TestProvisioningAttemptDoesNotFailOverWithinReservation(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	primaryStarted := make(chan struct{}, 1)
	secondaryStarted := make(chan struct{}, 1)
	primary := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "primary",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: primaryStarted,
		addWorkerErr:     types.NewProviderNotImplemented(),
	}
	secondary := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "secondary",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: secondaryStarted,
	}

	scheduler.workerPoolManager.SetPool("primary", types.WorkerPoolConfig{Priority: 200}, primary)
	scheduler.workerPoolManager.SetPool("secondary", types.WorkerPoolConfig{Priority: 100}, secondary)

	request := &types.ContainerRequest{
		ContainerId: uuid.New().String(),
		Cpu:         100,
		Memory:      100,
		Timestamp:   time.Now(),
	}
	newSchedulingAttempt(scheduler, request, nil).provisionWorker()

	select {
	case <-primaryStarted:
	case <-time.After(time.Second):
		t.Fatal("expected primary provisioning attempt")
	}

	select {
	case <-secondaryStarted:
		t.Fatal("did not expect same reservation to provision a second pool")
	case <-time.After(2 * requestProcessingInterval):
	}

	assert.Equal(t, 1, primary.AddWorkerCallCount())
	assert.Equal(t, 0, secondary.AddWorkerCallCount())
}

func TestProvisionedWorkerRuntimeMismatchDoesNotAmplifyJobs(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	started := make(chan struct{}, 2)
	done := make(chan struct{}, 1)
	controller := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "gvisor-pool",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		containerRuntime: types.ContainerRuntimeGvisor.String(),
		addWorkerStarted: started,
		addWorkerDone:    done,
	}
	scheduler.workerPoolManager.SetPool("gvisor-pool", types.WorkerPoolConfig{
		ContainerRuntime: types.ContainerRuntimeGvisor.String(),
	}, controller)

	request := &types.ContainerRequest{
		ContainerId:       uuid.NewString(),
		Cpu:               100,
		Memory:            100,
		PoolSelector:      "gvisor-pool",
		Timestamp:         time.Now(),
		CheckpointEnabled: true,
		Checkpoint: &types.Checkpoint{
			CheckpointId: "checkpoint-1",
			Status:       string(types.CheckpointStatusAvailable),
			Runtime:      types.ContainerRuntimeGvisor.String(),
		},
	}
	setPendingSchedulerRequests(t, scheduler, request)

	newSchedulingAttempt(scheduler, request, nil).run()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected the initial worker provisioning attempt")
	}

	requeued := popBacklogRequest(t, scheduler.requestBacklog)
	assert.Equal(t, 1, requeued.ProvisioningAttempts)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected the initial worker provisioning attempt to complete")
	}

	workers, err := scheduler.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Len(t, workers, 1)
	workers[0].Runtime = types.ContainerRuntimeRunc.String()
	assert.Nil(t, scheduler.workerRepo.AddWorker(workers[0]))

	newSchedulingAttempt(scheduler, requeued, workers).run()
	time.Sleep(2 * requestProcessingInterval)
	assert.Equal(t, 1, controller.AddWorkerCallCount(), "one request must not create another worker while its first worker is pending")
}

func TestProvisioningLimitStopsFurtherWorkerCreation(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	controller := &LocalWorkerPoolControllerForTest{
		ctx:        context.Background(),
		name:       "beta9-cpu",
		config:     scheduler.config,
		workerRepo: scheduler.workerRepo,
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)
	request := &types.ContainerRequest{
		ContainerId:          uuid.NewString(),
		Cpu:                  100,
		Memory:               100,
		PoolSelector:         "beta9-cpu",
		Timestamp:            time.Now(),
		ProvisioningAttempts: maxWorkerProvisioningAttempts,
	}
	setPendingSchedulerRequests(t, scheduler, request)

	newSchedulingAttempt(scheduler, request, nil).provisionWorker()
	assert.Equal(t, 0, controller.AddWorkerCallCount())
	assert.False(t, newSchedulingAttempt(scheduler, request, nil).runnable())
}

func TestProvisioningLimitSurvivesSchedulerHandoffs(t *testing.T) {
	first, err := NewSchedulerForTest()
	assert.Nil(t, err)
	replicas := []*Scheduler{first, schedulerReplicaForTest(first), schedulerReplicaForTest(first)}

	started := make(chan struct{}, len(replicas))
	done := make(chan struct{}, len(replicas))
	unblock := make(chan struct{})
	controller := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "beta9-cpu",
		config:           first.config,
		workerRepo:       first.workerRepo,
		addWorkerStarted: started,
		addWorkerDone:    done,
		unblockAddWorker: unblock,
	}
	first.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)

	request := &types.ContainerRequest{
		ContainerId:  uuid.NewString(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	setPendingSchedulerRequests(t, first, request)

	for attempt, scheduler := range replicas {
		newSchedulingAttempt(scheduler, request, nil).provisionWorker()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("provisioning attempt %d did not start", attempt+1)
		}
		request = popBacklogRequest(t, first.requestBacklog)
		assert.Equal(t, attempt+1, request.ProvisioningAttempts)
	}

	newSchedulingAttempt(first, request, nil).provisionWorker()
	assert.Equal(t, maxWorkerProvisioningAttempts, controller.AddWorkerCallCount())
	assert.False(t, newSchedulingAttempt(first, request, nil).runnable())

	close(unblock)
	for range replicas {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("provisioning attempt did not finish")
		}
	}
}

func popBacklogRequest(t *testing.T, backlog *RequestBacklog) *types.ContainerRequest {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		request, err := backlog.Pop()
		if err == nil {
			return request
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("expected a request in the scheduler backlog")
	return nil
}

func TestWorkerProvisioningBackoffDoesNotBlockExistingPoolCapacity(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	controller := &LocalWorkerPoolControllerForTest{
		ctx:          context.Background(),
		name:         "beta9-cpu",
		config:       scheduler.config,
		workerRepo:   scheduler.workerRepo,
		addWorkerErr: types.NewProviderNotImplemented(),
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)

	provisionRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, provisionRequest, nil).provisionWorker()

	deadline := time.After(time.Second)
	for scheduler.workerProvisioningBackoff.canAttempt("beta9-cpu") {
		select {
		case <-deadline:
			t.Fatal("expected add-worker attempt to be backed off")
		case <-time.After(time.Millisecond):
		}
	}

	worker := &types.Worker{
		Id:          uuid.New().String(),
		Status:      types.WorkerStatusAvailable,
		TotalCpu:    100,
		FreeCpu:     100,
		TotalMemory: 125,
		FreeMemory:  125,
		PoolName:    "beta9-cpu",
	}
	assert.Nil(t, scheduler.workerRepo.AddWorker(worker))

	scheduleRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	setPendingSchedulerRequests(t, scheduler, scheduleRequest)
	newSchedulingAttempt(scheduler, scheduleRequest, []*types.Worker{worker}).run()

	queued, err := scheduler.workerRepo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queued)
	assert.Equal(t, scheduleRequest.ContainerId, queued.ContainerId)
}

func TestPrivatePoolMissFallsBackToRegularAvailableWorker(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	workspaceID := "workspace-private-fallback"

	privateController := &capacityCheckingWorkerPoolControllerForTest{
		LocalWorkerPoolControllerForTest: &LocalWorkerPoolControllerForTest{
			ctx:              context.Background(),
			name:             "private-cpu",
			config:           scheduler.config,
			workerRepo:       scheduler.workerRepo,
			requiresSelector: true,
		},
		hasCapacity: false,
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
	privateController.hasCapacity = true
	fallback, poolName, ok := newSchedulingAttempt(scheduler, withoutStorage, nil).privatePoolFallbackRequest()
	assert.True(t, ok)
	assert.Equal(t, "private-cpu", poolName)
	assert.Empty(t, fallback.PoolSelector)
	assert.True(t, fallback.RuntimeTokenRequired)
	assert.Empty(t, fallback.Env)
	privateController.hasCapacity = false

	setPendingSchedulerRequests(t, scheduler, request)
	newSchedulingAttempt(scheduler, request, []*types.Worker{privateWorker, regularWorker}).run()

	queued, err := scheduler.workerRepo.GetNextContainerRequest(regularWorker.Id)
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

	privateController := &capacityCheckingWorkerPoolControllerForTest{
		LocalWorkerPoolControllerForTest: &LocalWorkerPoolControllerForTest{
			ctx:              context.Background(),
			name:             "private-cpu",
			config:           scheduler.config,
			workerRepo:       scheduler.workerRepo,
			requiresSelector: true,
		},
		hasCapacity: false,
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

	started := make(chan struct{}, 1)
	privateController := &capacityCheckingWorkerPoolControllerForTest{
		LocalWorkerPoolControllerForTest: &LocalWorkerPoolControllerForTest{
			ctx:              context.Background(),
			name:             "private-cpu",
			config:           scheduler.config,
			workerRepo:       scheduler.workerRepo,
			addWorkerStarted: started,
			addWorkerErr:     types.NewProviderNotImplemented(),
			requiresSelector: true,
		},
		hasCapacity: false,
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
	newSchedulingAttempt(scheduler, request, nil).run()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected private provisioning attempt")
	}

	time.Sleep(provisioningWorkerRequeueDelay + requestProcessingInterval)
	requeued, err := scheduler.requestBacklog.Pop()
	assert.Nil(t, err)
	assert.Equal(t, request.ContainerId, requeued.ContainerId)
	assert.Equal(t, "private-cpu", requeued.PoolSelector)
}

type capacityCheckingWorkerPoolControllerForTest struct {
	*LocalWorkerPoolControllerForTest
	hasCapacity bool
	capacityErr error
}

func (wpc *capacityCheckingWorkerPoolControllerForTest) HasWorkerCapacity(cpu int64, memory int64, gpuCount uint32) (bool, error) {
	return wpc.hasCapacity, wpc.capacityErr
}
