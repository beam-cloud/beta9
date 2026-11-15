package scheduler

import (
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWorkerStalled(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	created := func(ago time.Duration) int64 { return now.Add(-ago).Unix() }

	for _, test := range []struct {
		name         string
		worker       types.Worker
		stalledAfter time.Duration
		want         bool
	}{
		{"pending for less than stalledAfter", types.Worker{Status: types.WorkerStatusPending, CreatedAt: created(119 * time.Second)}, 2 * time.Minute, false},
		{"pending for stalledAfter", types.Worker{Status: types.WorkerStatusPending, CreatedAt: created(2 * time.Minute)}, 2 * time.Minute, true},
		{"pending for longer", types.Worker{Status: types.WorkerStatusPending, CreatedAt: created(time.Hour)}, 2 * time.Minute, true},
		{"the pool has no stalledAfter", types.Worker{Status: types.WorkerStatusPending, CreatedAt: created(time.Hour)}, 0, false},
		{"a worker that started is not stalled, however old", types.Worker{Status: types.WorkerStatusAvailable, CreatedAt: created(time.Hour)}, 2 * time.Minute, false},
		{"a worker from before workers recorded their age", types.Worker{Status: types.WorkerStatusPending}, 2 * time.Minute, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, workerStalled(&test.worker, test.stalledAfter, now))
		})
	}
}

func TestStallCheckReusesItsAnswerForAnInterval(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	looks := 0
	answer := true
	look := func() (bool, error) { looks++; return answer, nil }

	var check stallCheck
	stalled, err := check.stalledAt(now, look)
	require.NoError(t, err)
	require.True(t, stalled)

	// The pool's workers started, but the answer is not looked up again yet.
	answer = false
	stalled, _ = check.stalledAt(now.Add(stallCheckInterval-time.Millisecond), look)
	require.True(t, stalled)
	require.Equal(t, 1, looks)

	stalled, _ = check.stalledAt(now.Add(stallCheckInterval), look)
	require.False(t, stalled)
	require.Equal(t, 2, looks)
}

func TestStallCheckDoesNotRememberAFailedLook(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	var check stallCheck

	_, err := check.stalledAt(now, func() (bool, error) { return false, errors.New("redis is down") })
	require.Error(t, err)

	stalled, err := check.stalledAt(now, func() (bool, error) { return true, nil })
	require.NoError(t, err)
	require.True(t, stalled)
}

// A request stops waiting for a worker only when the worker's own pool says
// so: the pool's stalledAfter, read from the pool the worker belongs to.
func TestSchedulerStopsWaitingForStalledWorkersOnly(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	s.workerPoolManager.SetPool("patient", types.WorkerPoolConfig{}, &LocalWorkerPoolControllerForTest{name: "patient"})
	s.workerPoolManager.SetPool("impatient", types.WorkerPoolConfig{StalledAfter: types.Duration(time.Minute)}, &LocalWorkerPoolControllerForTest{name: "impatient"})

	now := s.now()
	old := now.Add(-time.Hour).Unix()
	workers := []*types.Worker{
		{Id: "patient-old", PoolName: "patient", Status: types.WorkerStatusPending, CreatedAt: old},
		{Id: "impatient-old", PoolName: "impatient", Status: types.WorkerStatusPending, CreatedAt: old},
		{Id: "impatient-new", PoolName: "impatient", Status: types.WorkerStatusPending, CreatedAt: now.Unix()},
		{Id: "impatient-running", PoolName: "impatient", Status: types.WorkerStatusAvailable, CreatedAt: old},
		{Id: "unknown-pool", PoolName: "deleted", Status: types.WorkerStatusPending, CreatedAt: old},
	}

	kept := []string{}
	for _, worker := range s.filterStalledWorkers(workers) {
		kept = append(kept, worker.Id)
	}
	require.Equal(t, []string{"patient-old", "impatient-new", "impatient-running", "unknown-pool"}, kept)
	require.Len(t, workers, 5, "the caller's list is left as it was")
	require.Equal(t, "impatient-old", workers[1].Id)
}

// A workspace's own pool may have the name of a shared pool. Its workers
// follow their own pool's stalledAfter, not the shared pool's.
func TestWorkerOfAPrivatePoolIsNotStalledByASharedPoolOfTheSameName(t *testing.T) {
	s, err := NewSchedulerForTest()
	require.NoError(t, err)
	s.workerPoolManager = NewWorkerPoolManager()
	s.workerPoolManager.SetPool("gpu", types.WorkerPoolConfig{StalledAfter: types.Duration(time.Minute)}, &LocalWorkerPoolControllerForTest{name: "gpu"})
	require.NoError(t, s.EnsureAgentPool("tenant", &compute.PoolState{Name: "gpu", Selector: "gpu", Mode: string(types.PoolModePrivate)}))

	now := s.now()
	old := now.Add(-time.Hour).Unix()
	shared := &types.Worker{Id: "shared-1", Status: types.WorkerStatusPending, PoolName: "gpu", CreatedAt: old}
	private := &types.Worker{Id: "private-1", Status: types.WorkerStatusPending, PoolName: "gpu", PoolSelector: "gpu", WorkspaceId: "tenant", CreatedAt: old}

	require.True(t, s.stalled(shared, now))
	require.False(t, s.stalled(private, now))
}

// A worker that has been starting for too long is not capacity to wait for:
// a request that finds only that worker goes on to ask a pool for another.
func TestRequestDoesNotWaitForAStalledWorker(t *testing.T) {
	now := time.Now()
	pending := func(age time.Duration) *types.Worker {
		return &types.Worker{
			Id: "worker", PoolName: "impatient", Status: types.WorkerStatusPending,
			TotalCpu: 4000, FreeCpu: 4000, TotalMemory: 8192, FreeMemory: 8192,
			CreatedAt: now.Add(-age).Unix(),
		}
	}
	waits := func(t *testing.T, s *Scheduler, request *types.ContainerRequest, worker *types.Worker) bool {
		t.Helper()
		return s.provisioning.reserveCapacity(s, []*types.Worker{worker}, request)
	}
	fixture := func(t *testing.T) (*Scheduler, *types.ContainerRequest) {
		t.Helper()
		s, err := NewSchedulerForTest()
		require.NoError(t, err)
		s.workerPoolManager = NewWorkerPoolManager()
		s.workerPoolManager.SetPool("impatient", types.WorkerPoolConfig{StalledAfter: types.Duration(time.Minute)}, &LocalWorkerPoolControllerForTest{name: "impatient"})
		return s, &types.ContainerRequest{ContainerId: uuid.NewString(), Cpu: 1000, Memory: 1024, Timestamp: now}
	}

	t.Run("a worker that is starting is waited for", func(t *testing.T) {
		s, request := fixture(t)
		require.True(t, waits(t, s, request, pending(10*time.Second)))
	})

	t.Run("a stalled worker is not", func(t *testing.T) {
		s, request := fixture(t)
		require.False(t, waits(t, s, request, pending(time.Hour)))
	})

	t.Run("a request stops waiting when its worker stalls", func(t *testing.T) {
		s, request := fixture(t)
		worker := pending(50 * time.Second)
		require.True(t, waits(t, s, request, worker))

		worker.CreatedAt -= 10
		require.False(t, waits(t, s, request, worker))
	})
}
