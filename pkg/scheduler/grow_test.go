package scheduler

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/clock"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/tj/assert"
)

// growthForTest returns the pool the scheduler would ask for a worker for the
// request, and the size it would ask for.
func growthForTest(t *testing.T, s *Scheduler, request *types.ContainerRequest) growth {
	t.Helper()
	route, err := s.growthRouteFor(request)
	require.NoError(t, err)
	choice := s.chooseGrowth(route, request)
	require.NotNil(t, choice.pool, "no pool can add a worker for the request")
	return choice
}

// growFixture is a scheduler on a fake clock whose route for a CPU request is
// the pools a, b and c, in that order.
type growFixture struct {
	*Scheduler
	clock *clock.Fake
	pools map[string]*LocalWorkerPoolControllerForTest
}

func newGrowFixture(t *testing.T) *growFixture {
	t.Helper()
	s, err := NewSchedulerForTest()
	require.NoError(t, err)

	fixture := &growFixture{Scheduler: s, clock: clock.NewFake(time.Now()), pools: map[string]*LocalWorkerPoolControllerForTest{}}
	s.clock = fixture.clock
	s.growBackoff = newGrowBackoff(fixture.clock)
	s.workerPoolManager = NewWorkerPoolManager()
	for i, name := range []string{"a", "b", "c"} {
		controller := &LocalWorkerPoolControllerForTest{ctx: context.Background(), name: name, config: s.config, workerRepo: s.workerRepo}
		fixture.pools[name] = controller
		s.workerPoolManager.SetPool(name, types.WorkerPoolConfig{Priority: int32(30 - 10*i)}, controller)
	}
	return fixture
}

func (f *growFixture) choose(t *testing.T, request *types.ContainerRequest) growth {
	t.Helper()
	route, err := f.growthRouteFor(request)
	require.NoError(t, err)
	return f.chooseGrowth(route, request)
}

func TestChooseGrowth(t *testing.T) {
	const noPool = ""
	asked := func(attempts map[string]int) *types.ContainerRequest {
		return &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, GrowAttempts: attempts}
	}

	for _, test := range []struct {
		name      string
		setup     func(f *growFixture)
		request   *types.ContainerRequest
		want      string
		exhausted bool
	}{
		{
			name:    "the first pool in the route",
			request: asked(nil),
			want:    "a",
		},
		{
			name:    "a pool that cannot add a worker is passed over",
			setup:   func(f *growFixture) { f.pools["a"].cannotAddWorker = true },
			request: asked(nil),
			want:    "b",
		},
		{
			name:    "a pool that cannot be asked is passed over",
			setup:   func(f *growFixture) { f.pools["a"].canAddWorkerErr = errors.New("redis is down") },
			request: asked(nil),
			want:    "b",
		},
		{
			name:    "a pool that just failed to add a worker is left alone",
			setup:   func(f *growFixture) { f.growBackoff.failed("a") },
			request: asked(nil),
			want:    "b",
		},
		{
			name: "and is asked again once the delay has passed",
			setup: func(f *growFixture) {
				f.growBackoff.failed("a")
				f.clock.Advance(growRetryDelay)
			},
			request: asked(nil),
			want:    "a",
		},
		{
			name:    "a pool the request has asked three times is not asked again",
			request: asked(map[string]int{"a": maxGrowAttempts}),
			want:    "b",
		},
		{
			name:    "attempts on one pool do not count against another",
			request: asked(map[string]int{"a": maxGrowAttempts - 1, "b": maxGrowAttempts, "c": maxGrowAttempts}),
			want:    "a",
		},
		{
			name: "no pool can add a worker now",
			setup: func(f *growFixture) {
				for _, pool := range f.pools {
					pool.cannotAddWorker = true
				}
			},
			request: asked(nil),
			want:    noPool,
		},
		{
			name: "every pool is being left alone",
			setup: func(f *growFixture) {
				for name := range f.pools {
					f.growBackoff.failed(name)
				}
			},
			request: asked(nil),
			want:    noPool,
		},
		{
			name:      "the request has asked every pool three times",
			request:   asked(map[string]int{"a": maxGrowAttempts, "b": maxGrowAttempts, "c": maxGrowAttempts}),
			want:      noPool,
			exhausted: true,
		},
		{
			name:    "a pool that cannot grow now keeps the request waiting when the others are used up",
			setup:   func(f *growFixture) { f.pools["b"].cannotAddWorker = true },
			request: asked(map[string]int{"a": maxGrowAttempts, "c": maxGrowAttempts}),
			want:    noPool,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newGrowFixture(t)
			if test.setup != nil {
				test.setup(f)
			}
			before := test.request.Clone()

			choice := f.choose(t, test.request)

			got := noPool
			if choice.pool != nil {
				got = choice.pool.Name
			}
			require.Equal(t, test.want, got)
			require.Equal(t, test.exhausted, choice.exhausted)
			// Choosing is a decision only: nothing is asked for and nothing
			// is counted.
			require.Equal(t, before.GrowAttempts, test.request.GrowAttempts)
			for name, pool := range f.pools {
				require.Zero(t, pool.AddWorkerCallCount(), "pool %s", name)
			}
		})
	}
}

// A request counts the workers it asked each pool for, so pools that keep
// failing use up their own attempts and not those of the pools after them.
func TestGrowCountsAttemptsPerPool(t *testing.T) {
	f := newGrowFixture(t)
	f.spawn = func(fn func()) { fn() }
	f.pools["a"].addWorkerErr = errors.New("cluster refused the job")
	f.pools["b"].addWorkerErr = errors.New("cluster refused the job")
	f.pools["c"].cannotAddWorker = true

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)

	for round := 1; round <= maxGrowAttempts; round++ {
		newSchedulingAttempt(f.Scheduler, request, nil).grow() // a fails
		newSchedulingAttempt(f.Scheduler, request, nil).grow() // a is left alone, b fails
		newSchedulingAttempt(f.Scheduler, request, nil).grow() // both left alone, c cannot grow
		require.Equal(t, map[string]int{"a": round, "b": round}, request.GrowAttempts)
		require.True(t, newSchedulingAttempt(f.Scheduler, request, nil).runnable(), "round %d", round)
		f.clock.Advance(growRetryDelay)
	}

	// a and b are used up. c was never asked for a worker, so it costs
	// nothing and the request keeps waiting for it.
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.True(t, newSchedulingAttempt(f.Scheduler, request, nil).runnable())
	require.Zero(t, f.pools["c"].AddWorkerCallCount())

	f.pools["c"].cannotAddWorker = false
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 1, f.pools["c"].AddWorkerCallCount())
	require.Equal(t, map[string]int{"a": maxGrowAttempts, "b": maxGrowAttempts, "c": 1}, request.GrowAttempts)
}

func TestWorkerSizeFor(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.config.Worker.DefaultWorkerCPURequest = 2000
	s.config.Worker.DefaultWorkerMemoryRequest = 2048

	sized := types.WorkerPoolJobSpecPoolSizingConfig{DefaultWorkerCPU: "8000m", DefaultWorkerMemory: "16Gi", DefaultWorkerGpuCount: "4"}
	small := &types.ContainerRequest{Cpu: 500, Memory: 512}
	large := &types.ContainerRequest{Cpu: 32000, Memory: 64 * 1024, Gpu: "A10G", GpuCount: 8}
	memory := capacityMemoryForScheduling

	for _, test := range []struct {
		name    string
		pool    types.WorkerPoolConfig
		request *types.ContainerRequest
		want    workerSize
	}{
		{"kubernetes pool: at least the default worker", types.WorkerPoolConfig{}, small,
			workerSize{cpu: 2000, memory: 2048}},
		{"kubernetes pool: at least the pool's own default", types.WorkerPoolConfig{GPUType: "A10G", PoolSizing: sized}, small,
			workerSize{cpu: 8000, memory: 16 * 1024, gpuCount: 4}},
		{"kubernetes pool: a larger request gets a worker its size", types.WorkerPoolConfig{GPUType: "A10G", PoolSizing: sized}, large,
			workerSize{cpu: 32000, memory: memory(large), gpuCount: 8}},
		{"agent pool: what the request needs", types.WorkerPoolConfig{Mode: types.PoolModeExternal, PoolSizing: sized}, small,
			workerSize{cpu: 500, memory: memory(small)}},
		{"private pool: what the request needs", types.WorkerPoolConfig{Mode: types.PoolModePrivate, GPUType: "A10G", PoolSizing: sized}, large,
			workerSize{cpu: 32000, memory: memory(large), gpuCount: 8}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, s.workerSizeFor(&WorkerPool{Name: "pool", Config: test.pool}, test.request))
		})
	}
}

func TestPoolThatFailedToAddAWorkerIsLeftAloneForAMoment(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	started := make(chan struct{}, 2)
	controller := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "beta9-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: started,
		addWorkerErr:     errors.New("provisioning failed"),
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)

	firstRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, firstRequest, nil).grow()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected first provisioning attempt")
	}

	deadline := time.After(time.Second)
	for !scheduler.growBackoff.leftAlone("beta9-cpu") {
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
	newSchedulingAttempt(scheduler, secondRequest, nil).grow()
	time.Sleep(2 * requestProcessingInterval)

	assert.Equal(t, 1, controller.AddWorkerCallCount())
}

func TestGrowPassesOverAPoolThatCannotAddAWorker(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	agent := &LocalWorkerPoolControllerForTest{
		ctx:             context.Background(),
		name:            "agent-cpu",
		config:          scheduler.config,
		workerRepo:      scheduler.workerRepo,
		cannotAddWorker: true,
	}
	dynamicStarted := make(chan struct{}, 1)
	dynamic := &LocalWorkerPoolControllerForTest{
		ctx:              context.Background(),
		name:             "dynamic-cpu",
		config:           scheduler.config,
		workerRepo:       scheduler.workerRepo,
		addWorkerStarted: dynamicStarted,
	}
	scheduler.workerPoolManager.SetPool(agent.name, types.WorkerPoolConfig{
		Mode:     types.PoolModeExternal,
		Priority: 10,
	}, agent)
	scheduler.workerPoolManager.SetPool(dynamic.name, types.WorkerPoolConfig{
		Mode:     types.PoolModeLocal,
		Priority: 0,
	}, dynamic)

	request := &types.ContainerRequest{
		ContainerId: uuid.NewString(),
		Cpu:         8000,
		Memory:      16 * 1024,
		Timestamp:   time.Now(),
		// With storage the agent pool is in the route, ahead of the dynamic one.
		Workspace: testWorkspaceWithStorage(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	newSchedulingAttempt(scheduler, request, nil).grow()

	select {
	case <-dynamicStarted:
	case <-time.After(time.Second):
		t.Fatal("expected the dynamic pool to provision a worker")
	}
	assert.Equal(t, 0, agent.AddWorkerCallCount())
	assert.Equal(t, 1, dynamic.AddWorkerCallCount())
	// Passing over the agent pool cost the request nothing.
	assert.Equal(t, map[string]int{dynamic.name: 1}, request.GrowAttempts)
}

func TestGrowDoesNotSpendAnAttemptWhenAPoolCannotBeAsked(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)
	scheduler.workerPoolManager = NewWorkerPoolManager()

	agent := &LocalWorkerPoolControllerForTest{
		ctx:             context.Background(),
		name:            "agent-cpu",
		config:          scheduler.config,
		workerRepo:      scheduler.workerRepo,
		canAddWorkerErr: context.DeadlineExceeded,
	}
	scheduler.workerPoolManager.SetPool(agent.name, types.WorkerPoolConfig{
		Mode: types.PoolModeExternal,
	}, agent)

	request := &types.ContainerRequest{
		ContainerId: uuid.NewString(),
		Cpu:         8000,
		Memory:      16 * 1024,
		Timestamp:   time.Now(),
		Workspace:   testWorkspaceWithStorage(),
	}
	setPendingSchedulerRequests(t, scheduler, request)
	newSchedulingAttempt(scheduler, request, nil).grow()

	assert.Equal(t, 0, agent.AddWorkerCallCount())
	assert.Empty(t, request.GrowAttempts)
	time.Sleep(provisioningWorkerRequeueDelay + 10*time.Millisecond)
	requeued, err := scheduler.requestBacklog.Pop()
	assert.Nil(t, err)
	assert.Equal(t, request.ContainerId, requeued.ContainerId)
}

func TestEveryReplicaPassesOverAPoolThatCannotAddAWorker(t *testing.T) {
	first, err := NewSchedulerForTest()
	assert.Nil(t, err)
	first.workerPoolManager = NewWorkerPoolManager()
	replicas := []*Scheduler{first, schedulerReplicaForTest(first), schedulerReplicaForTest(first)}

	dynamic := &LocalWorkerPoolControllerForTest{
		ctx:        context.Background(),
		name:       "dynamic-cpu",
		config:     first.config,
		workerRepo: first.workerRepo,
	}
	agent := &LocalWorkerPoolControllerForTest{
		ctx:             context.Background(),
		name:            "agent-cpu",
		config:          first.config,
		workerRepo:      first.workerRepo,
		cannotAddWorker: true,
	}
	first.workerPoolManager.SetPool(agent.name, types.WorkerPoolConfig{
		Mode:     types.PoolModeExternal,
		Priority: 10,
	}, agent)
	first.workerPoolManager.SetPool(dynamic.name, types.WorkerPoolConfig{Mode: types.PoolModeLocal}, dynamic)

	request := &types.ContainerRequest{Cpu: 8000, Memory: 16 * 1024, Workspace: testWorkspaceWithStorage()}
	for _, scheduler := range replicas {
		controllers, err := controllersFor(scheduler, request)
		assert.Nil(t, err)
		assert.Equal(t, []string{agent.name, dynamic.name}, controllerNames(controllers))
		assert.Equal(t, dynamic.name, growthForTest(t, scheduler, request).pool.Name)
	}
	assert.Equal(t, 0, agent.AddWorkerCallCount())
}

func TestGrowAsksOnePoolPerPass(t *testing.T) {
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
		addWorkerErr:     errors.New("provisioning failed"),
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
	newSchedulingAttempt(scheduler, request, nil).grow()

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

func TestRequestDoesNotAddASecondWorkerWhileItsFirstIsPending(t *testing.T) {
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

	scheduler.processRequestBatch([]*types.ContainerRequest{request}, nil)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("expected the initial worker provisioning attempt")
	}

	requeued := popBacklogRequest(t, scheduler.requestBacklog)
	assert.Equal(t, map[string]int{"gvisor-pool": 1}, requeued.GrowAttempts)
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

	scheduler.processRequestBatch([]*types.ContainerRequest{requeued}, workers)
	time.Sleep(2 * requestProcessingInterval)
	assert.Equal(t, 1, controller.AddWorkerCallCount(), "one request must not create another worker while its first worker is pending")
}

func TestRequestThatHasAskedEveryPoolThreeTimesFails(t *testing.T) {
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
		ContainerId:  uuid.NewString(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
		GrowAttempts: map[string]int{"beta9-cpu": maxGrowAttempts},
	}
	setPendingSchedulerRequests(t, scheduler, request)

	newSchedulingAttempt(scheduler, request, nil).grow()
	assert.Equal(t, 0, controller.AddWorkerCallCount())
	assert.False(t, newSchedulingAttempt(scheduler, request, nil).runnable())
}

// blockedProvisioning is a pool whose AddWorker parks until unblock closes,
// reporting each call on started.
func blockedProvisioning(t *testing.T, scheduler *Scheduler, n int) (*LocalWorkerPoolControllerForTest, chan struct{}) {
	t.Helper()
	unblock := make(chan struct{})
	controller := &LocalWorkerPoolControllerForTest{
		ctx: context.Background(), name: "beta9-cpu", config: scheduler.config, workerRepo: scheduler.workerRepo,
		addWorkerStarted: make(chan struct{}, n), addWorkerDone: make(chan struct{}, n), unblockAddWorker: unblock,
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)
	return controller, unblock
}

func awaitN(t *testing.T, ch chan struct{}, n int, what string) {
	t.Helper()
	for i := range n {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: %d/%d", what, i, n)
		}
	}
}

// Replicas share one lock per container: while one provisions, or until its
// pending worker's reservation expires, the others do not add a second worker.
// Distinct containers never contend.
func TestGrowLockIsPerContainerAcrossReplicas(t *testing.T) {
	first, err := NewSchedulerForTest()
	assert.Nil(t, err)
	second := schedulerReplicaForTest(first)
	controller, unblock := blockedProvisioning(t, first, 3)
	request := func() *types.ContainerRequest {
		return &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 100, Memory: 100, PoolSelector: "beta9-cpu", Timestamp: time.Now()}
	}
	same, otherA, otherB := request(), request(), request()
	setPendingSchedulerRequests(t, first, same, otherA, otherB)

	newSchedulingAttempt(first, same, nil).grow()
	newSchedulingAttempt(second, same, nil).grow()
	newSchedulingAttempt(second, otherA, nil).grow()
	newSchedulingAttempt(first, otherB, nil).grow()
	awaitN(t, controller.addWorkerStarted, 3, "distinct containers started provisioning")
	time.Sleep(2 * requestProcessingInterval)
	assert.Equal(t, 3, controller.AddWorkerCallCount(), "the duplicate attempt waits")

	close(unblock)
	awaitN(t, controller.addWorkerDone, 3, "provisioning finished")
	newSchedulingAttempt(second, same, nil).grow()
	time.Sleep(2 * requestProcessingInterval)
	assert.Equal(t, 3, controller.AddWorkerCallCount(), "the lock outlives a successful attempt")
	workers, err := first.workerRepo.GetAllWorkers()
	assert.Nil(t, err)
	assert.Len(t, workers, 3)
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

func TestPoolLeftAloneAfterAFailureStillPlacesOnItsWorkers(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.Nil(t, err)

	controller := &LocalWorkerPoolControllerForTest{
		ctx:          context.Background(),
		name:         "beta9-cpu",
		config:       scheduler.config,
		workerRepo:   scheduler.workerRepo,
		addWorkerErr: errors.New("provisioning failed"),
	}
	scheduler.workerPoolManager.SetPool("beta9-cpu", types.WorkerPoolConfig{}, controller)

	provisionRequest := &types.ContainerRequest{
		ContainerId:  uuid.New().String(),
		Cpu:          100,
		Memory:       100,
		PoolSelector: "beta9-cpu",
		Timestamp:    time.Now(),
	}
	newSchedulingAttempt(scheduler, provisionRequest, nil).grow()

	deadline := time.After(time.Second)
	for !scheduler.growBackoff.leftAlone("beta9-cpu") {
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
	scheduler.processRequestBatch([]*types.ContainerRequest{scheduleRequest}, []*types.Worker{worker})

	queued, err := nextContainerRequest(scheduler.workerRepo, worker.Id)
	assert.Nil(t, err)
	assert.NotNil(t, queued)
	assert.Equal(t, scheduleRequest.ContainerId, queued.ContainerId)
}

// A request that asked a pool for a worker holds a lock until it would stop
// waiting for that worker. Finding the lock held is the normal case for every
// other scheduling pass and is not an error.
func TestGrowDoesNotReportAHeldLockAsAnError(t *testing.T) {
	f := newGrowFixture(t)
	replica := schedulerReplicaForTest(f.Scheduler)
	replica.growBackoff = newGrowBackoff(f.clock)

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)

	var logged bytes.Buffer
	original := log.Logger
	log.Logger = zerolog.New(&logged)
	t.Cleanup(func() { log.Logger = original })

	f.spawn = func(fn func()) { fn() }
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	newSchedulingAttempt(replica, request.Clone(), nil).grow()

	require.Equal(t, 1, f.pools["a"].AddWorkerCallCount())
	require.NotContains(t, logged.String(), `"level":"error"`)
}

func TestGrowLockLastsAsLongAsTheRequestWaitsForTheWorker(t *testing.T) {
	pool := func(stalledAfter time.Duration) *WorkerPool {
		return &WorkerPool{Name: "pool", Config: types.WorkerPoolConfig{StalledAfter: types.Duration(stalledAfter)}}
	}
	require.Equal(t, pendingWorkerReservationTTL, growLockTTL(pool(0)))
	require.Equal(t, time.Minute, growLockTTL(pool(time.Minute)), "a stalled worker is given up on sooner")
	require.Equal(t, pendingWorkerReservationTTL, growLockTTL(pool(time.Hour)))
}

// waitingFixture is a grow fixture whose request has asked pool a for a
// worker and waits for it. The worker has room for four such requests.
type waitingFixture struct {
	*growFixture
	request *types.ContainerRequest
	worker  *types.Worker
}

func newWaitingFixture(t *testing.T) *waitingFixture {
	t.Helper()
	f := newGrowFixture(t)
	f.provisioning = newProvisioningTracker(f.clock)
	f.spawn = func(fn func()) { fn() }
	f.config.Worker.DefaultWorkerCPURequest = 4000
	f.config.Worker.DefaultWorkerMemoryRequest = 8192

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 1, f.pools["a"].AddWorkerCallCount())

	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	require.Len(t, workers, 1)
	return &waitingFixture{growFixture: f, request: request, worker: workers[0]}
}

// waits reports whether the request goes on waiting for a worker, in a pass
// that plans with the workers there are now.
func (f *waitingFixture) waits(t *testing.T) bool {
	t.Helper()
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	return f.provisioning.reserveCapacity(f.Scheduler, workers, f.request)
}

// A request waits for the worker its pool added until the worker starts or
// is removed. A machine that was bought and that its provider took back, or
// failed to start, is removed like that; the request then asks for another
// worker at once instead of waiting out the time a worker has to start.
func TestRequestStopsWaitingForAWorkerThatWasRemoved(t *testing.T) {
	f := newWaitingFixture(t)
	f.clock.Advance(time.Minute)
	require.True(t, f.waits(t), "the worker is starting")

	require.NoError(t, f.workerRepo.RemoveWorker(f.worker.Id))
	require.False(t, f.waits(t), "the worker is gone")

	newSchedulingAttempt(f.Scheduler, f.request, nil).grow()
	require.Equal(t, 2, f.pools["a"].AddWorkerCallCount(), "another worker is asked for at once")
	require.True(t, f.waits(t), "and waited for")
}

// The pass that asks for a worker, and those that had read the workers just
// before it was added, plan without it.
func TestRequestWaitsForAWorkerThatWasJustAdded(t *testing.T) {
	f := newWaitingFixture(t)
	before := []*types.Worker{}

	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, before, f.request))
	f.clock.Advance(workerListLag - time.Millisecond)
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, before, f.request))

	f.clock.Advance(time.Millisecond)
	require.False(t, f.provisioning.reserveCapacity(f.Scheduler, before, f.request), "a list that old would have it")
}

// A pool can take its time to add a worker: a machine is bought before the
// worker exists. Until the pool answers there is no worker to find missing.
func TestRequestWaitsWhileItsPoolIsStillAddingTheWorker(t *testing.T) {
	f := newGrowFixture(t)
	f.provisioning = newProvisioningTracker(f.clock)
	pool := f.pools["a"]
	pool.addWorkerStarted, pool.addWorkerDone = make(chan struct{}, 1), make(chan struct{}, 1)
	pool.unblockAddWorker = make(chan struct{})

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	awaitN(t, pool.addWorkerStarted, 1, "the pool was asked")

	f.clock.Advance(time.Minute)
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, nil, request))

	close(pool.unblockAddWorker)
	awaitN(t, pool.addWorkerDone, 1, "the pool answered")
}

// A worker that is removed takes every request that waited for it along,
// whichever of them finds out.
func TestEveryRequestThatWaitedForARemovedWorkerAsksAgain(t *testing.T) {
	f := newWaitingFixture(t)
	f.clock.Advance(time.Minute)

	// A second request finds room on the worker that is starting.
	second := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	workers, err := f.workerRepo.GetAllWorkers()
	require.NoError(t, err)
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, workers, second))

	require.NoError(t, f.workerRepo.RemoveWorker(f.worker.Id))
	require.False(t, f.provisioning.reserveCapacity(f.Scheduler, nil, second), "the second request finds the worker gone")

	// The first request, which holds the lock on asking, is free to ask.
	newSchedulingAttempt(f.Scheduler, f.request, nil).grow()
	require.Equal(t, 2, f.pools["a"].AddWorkerCallCount())
}

// A worker that was already starting when a request found it is waited for
// in the same way, and given up on in the same way.
func TestRequestStopsWaitingForAStartingWorkerItFoundThatWasRemoved(t *testing.T) {
	f := newGrowFixture(t)
	f.provisioning = newProvisioningTracker(f.clock)
	starting := &types.Worker{
		Id: "starting", PoolName: "a", Status: types.WorkerStatusPending,
		TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8192, FreeMemory: 8192, CreatedAt: f.clock.Now().Unix(),
	}
	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}

	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, []*types.Worker{starting}, request))
	f.clock.Advance(workerListLag)
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, []*types.Worker{starting}, request))
	require.False(t, f.provisioning.reserveCapacity(f.Scheduler, nil, request))
}

// A pool of machines that joined by hand answers with the machine's worker,
// which may be running already. The request waits for room on it as long as
// it would wait for a worker to start, and then asks again.
func TestRequestWaitsForARunningWorkerItsPoolAnsweredWith(t *testing.T) {
	f := newGrowFixture(t)
	f.provisioning = newProvisioningTracker(f.clock)
	f.spawn = func(fn func()) { fn() }
	f.pools["a"].workerStatus = types.WorkerStatusAvailable

	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)
	waits := func() bool {
		workers, err := f.workerRepo.GetAllWorkers()
		require.NoError(t, err)
		return f.provisioning.reserveCapacity(f.Scheduler, workers, request)
	}

	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	f.clock.Advance(pendingWorkerReservationTTL - time.Second)
	require.True(t, waits(), "the pool has no other worker to answer with")
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 1, f.pools["a"].AddWorkerCallCount())

	f.clock.Advance(time.Second)
	require.False(t, waits(), "the wait is over")
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 2, f.pools["a"].AddWorkerCallCount(), "the pool is asked again")
}

// A second request that is answered with the worker another one waits for
// joins that wait; neither is forgotten.
func TestRequestsAnsweredWithTheSameWorkerWaitTogether(t *testing.T) {
	f := newGrowFixture(t)
	f.provisioning = newProvisioningTracker(f.clock)

	machine := &types.Worker{
		Id: "machine", PoolName: "a", Status: types.WorkerStatusAvailable,
		TotalCpu: 8000, FreeCpu: 8000, TotalMemory: 16384, FreeMemory: 16384,
	}
	first := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024}
	second := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024}
	pool, ok := f.workerPoolManager.GetPool("a")
	require.True(t, ok)
	for _, request := range []*types.ContainerRequest{first, second} {
		reservation := f.provisioning.addReservation(f.Scheduler, request, pool, workerSize{cpu: 1000, memory: 1024})
		f.provisioning.handoff(f.Scheduler, reservation, machine)
	}

	workers := []*types.Worker{machine}
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, workers, first))
	require.True(t, f.provisioning.reserveCapacity(f.Scheduler, workers, second))

	f.clock.Advance(pendingWorkerReservationTTL)
	require.Empty(t, f.provisioning.requestReservations, "the wait ends for both")
	require.Empty(t, f.provisioning.reservations)
}

// A request that asked a pool for a worker holds a lock that keeps it from
// asking again while it waits. The lock is released as soon as the wait is
// over, whatever ended it.
func TestGrowLockIsReleasedWhenTheRequestStopsWaiting(t *testing.T) {
	for _, test := range []struct {
		name string
		end  func(t *testing.T, f *waitingFixture)
	}{
		{"the worker was removed", func(t *testing.T, f *waitingFixture) {
			require.NoError(t, f.workerRepo.RemoveWorker(f.worker.Id))
			require.False(t, f.waits(t))
		}},
		{"the worker stalled", func(t *testing.T, f *waitingFixture) {
			f.workerPoolManager.SetPool("a", types.WorkerPoolConfig{Priority: 30, StalledAfter: types.Duration(time.Minute)}, f.pools["a"])
			require.False(t, f.waits(t))
		}},
		{"the worker took too long to start", func(t *testing.T, f *waitingFixture) {
			f.clock.Advance(pendingWorkerReservationTTL)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newWaitingFixture(t)
			lock := common.RedisKeys.SchedulerWorkerProvisioningLock(f.request.ContainerId)
			held := func() bool { return f.requestBacklog.rdb.Exists(context.Background(), lock).Val() == 1 }

			f.clock.Advance(time.Minute)
			require.True(t, f.waits(t))
			require.True(t, held(), "the request waits")

			test.end(t, f)
			require.False(t, held(), "the request stopped waiting")
		})
	}
}

// A pool that failed to add a worker is left alone for a while. A request
// with no other pool to ask is planned again as soon as any request that
// waits: a container that finishes meanwhile leaves room for it.
func TestRequestWhosePoolsAreLeftAloneIsPlannedAgainSoon(t *testing.T) {
	f := newGrowFixture(t)
	f.requestBacklog.now = f.clock.Now
	for name := range f.pools {
		f.growBackoff.failed(name)
	}
	request := &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, request)

	newSchedulingAttempt(f.Scheduler, request, nil).grow()

	f.clock.Advance(provisioningWorkerRequeueDelay)
	waiting, err := f.requestBacklog.PopN(1)
	require.NoError(t, err)
	require.Len(t, waiting, 1)
}

// flakyPools fails the next reads of a pool's state.
type flakyPools struct {
	repo.ComputeRepository
	failures int
}

func (r *flakyPools) GetPoolState(ctx context.Context, workspaceID, name string) (*compute.PoolState, error) {
	if r.failures > 0 {
		r.failures--
		return nil, errors.New("redis timed out")
	}
	return r.ComputeRepository.GetPoolState(ctx, workspaceID, name)
}

// A request fails for want of a pool only when no pool accepts it. Failing
// to read the pool it names says nothing about the request, which is tried
// again.
func TestGrowTriesAgainWhenThePoolCannotBeLookedUp(t *testing.T) {
	f := newGrowFixture(t)
	pools := &flakyPools{ComputeRepository: f.computeRepo}
	f.computeRepo = pools

	request := &types.ContainerRequest{
		ContainerId: uuid.NewString(), WorkspaceId: "workspace", PoolSelector: "a",
		Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now(),
	}
	setPendingSchedulerRequests(t, f.Scheduler, request)

	pools.failures = 1
	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.True(t, newSchedulingAttempt(f.Scheduler, request, nil).runnable(), "the request is still to be scheduled")
	require.Zero(t, f.pools["a"].AddWorkerCallCount())

	newSchedulingAttempt(f.Scheduler, request, nil).grow()
	require.Equal(t, 1, f.pools["a"].AddWorkerCallCount())

	nowhere := &types.ContainerRequest{ContainerId: uuid.NewString(), PoolSelector: "no-such-pool", Cpu: 1000, Memory: 1024, Timestamp: f.clock.Now()}
	setPendingSchedulerRequests(t, f.Scheduler, nowhere)
	newSchedulingAttempt(f.Scheduler, nowhere, nil).grow()
	require.False(t, newSchedulingAttempt(f.Scheduler, nowhere, nil).runnable(), "no pool accepts it")
}
