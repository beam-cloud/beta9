package scheduler

import (
	"context"
	"testing"
	"time"

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
	scheduler.workerPoolManager = NewWorkerPoolManager(false)

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
	newSchedulingAttempt(scheduler, scheduleRequest, []*types.Worker{worker}).run()

	queued, err := scheduler.workerRepo.GetNextContainerRequest(worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queued)
	assert.Equal(t, scheduleRequest.ContainerId, queued.ContainerId)
}

func TestPrivatePoolMissFallsBackToRegularAvailableWorker(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	scheduler.workerPoolManager.SetPool("private-cpu", types.WorkerPoolConfig{
		Mode:                 types.PoolModePrivate,
		RequiresPoolSelector: true,
	}, &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "private-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerErr:     &AgentPoolCapacityError{PoolName: "private-cpu"},
		requiresSelector: true,
	})

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
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "private-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, request, []*types.Worker{privateWorker, regularWorker}).run()

	queued, err := scheduler.workerRepo.GetNextContainerRequest(regularWorker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queued)
	assert.Equal(t, request.ContainerId, queued.ContainerId)
	assert.Equal(t, "", queued.PoolSelector)

	privateAfter, err := scheduler.workerRepo.GetWorkerById(privateWorker.Id)
	assert.Nil(t, err)
	assert.Equal(t, int64(100), privateAfter.FreeCpu)
}

func TestPrivatePoolMissWithoutRegularCapacityKeepsPrivateSelector(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager(false)

	started := make(chan struct{}, 1)
	privateController := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "private-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: started,
		addWorkerErr:     types.NewProviderNotImplemented(),
		requiresSelector: true,
	}
	scheduler.workerPoolManager.SetPool("private-cpu", types.WorkerPoolConfig{
		Mode:                 types.PoolModePrivate,
		RequiresPoolSelector: true,
	}, privateController)

	request := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          1000,
		Memory:       1000,
		PoolSelector: "private-cpu",
		Timestamp:    time.Now(),
	}
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
