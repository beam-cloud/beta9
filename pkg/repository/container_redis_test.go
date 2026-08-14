package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/redis/go-redis/v9"
)

type redisKeyCommandBarrier struct {
	key     string
	reached chan struct{}
	once    sync.Once
}

func newRedisKeyCommandBarrier(key string) *redisKeyCommandBarrier {
	return &redisKeyCommandBarrier{key: key, reached: make(chan struct{})}
}

func (b *redisKeyCommandBarrier) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (b *redisKeyCommandBarrier) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		b.observe(cmd)
		return err
	}
}

func (b *redisKeyCommandBarrier) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			b.observe(cmd)
		}
		return err
	}
}

func (b *redisKeyCommandBarrier) observe(cmd redis.Cmder) {
	for _, arg := range cmd.Args()[1:] {
		if key, ok := arg.(string); ok && key == b.key {
			b.once.Do(func() { close(b.reached) })
			return
		}
	}
}

func assertContainerStateTTL(t *testing.T, rdb *common.RedisClient, containerID string, want time.Duration) {
	t.Helper()
	ttl, err := rdb.TTL(context.Background(), common.RedisKeys.SchedulerContainerState(containerID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl != want && ttl != want-time.Second {
		t.Fatalf("container state TTL = %s, want %s (allowing one-second boundary rounding)", ttl, want)
	}
}

func TestCreateContainerStateWithConcurrencyLimitSkipsConcurrencyLockWithoutQuota(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	request := &types.ContainerRequest{
		ContainerId: "sandbox-test-stub-00000000",
		StubId:      "test-stub",
		WorkspaceId: "test-workspace",
		MachineId:   "machine-1",
		Cpu:         100,
		Memory:      128,
		Stub: types.StubWithRelated{
			Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)},
		},
	}

	lock, err := redislock.Obtain(
		context.Background(),
		rdb,
		common.RedisKeys.WorkspaceConcurrencyLimitLock(request.WorkspaceId),
		time.Second,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(context.Background())

	startedAt := time.Now()
	err = repo.CreateContainerStateWithConcurrencyLimit(nil, request)
	if err != nil {
		t.Fatalf("expected nil quota to bypass concurrency lock, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("nil quota path waited on concurrency lock for %s", elapsed)
	}

	state, err := repo.GetContainerState(request.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != types.ContainerStatusPending {
		t.Fatalf("expected pending state, got %s", state.Status)
	}
	if state.MachineId != request.MachineId {
		t.Fatalf("expected pending state on machine %q, got %q", request.MachineId, state.MachineId)
	}
}

func TestSetContainerStateCommitsIndexesWithState(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	state := &types.ContainerState{
		ContainerId: "pod-test-stub-indexed",
		StubId:      "test-stub",
		WorkspaceId: "test-workspace",
		Status:      types.ContainerStatusPending,
		ScheduledAt: time.Now().Unix(),
		Cpu:         100,
		Memory:      128,
	}

	if err := repo.SetContainerState(state.ContainerId, state); err != nil {
		t.Fatal(err)
	}

	stateKey := common.RedisKeys.SchedulerContainerState(state.ContainerId)
	stubIndexKey := common.RedisKeys.SchedulerContainerIndex(state.StubId)
	workspaceIndexKey := common.RedisKeys.SchedulerContainerWorkspaceIndex(state.WorkspaceId)

	if ok, err := rdb.SIsMember(context.Background(), stubIndexKey, stateKey).Result(); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected state key to be present in stub index")
	}

	if ok, err := rdb.SIsMember(context.Background(), workspaceIndexKey, stateKey).Result(); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected state key to be present in workspace index")
	}
	if score, err := rdb.ZScore(context.Background(), common.RedisKeys.SchedulerContainerStateIndex(), stateKey).Result(); err != nil {
		t.Fatal(err)
	} else if score <= float64(time.Now().Unix()) {
		t.Fatal("expected state key to have a future expiry in the global index")
	}

	byStub, err := repo.GetActiveContainersByStubId(state.StubId)
	if err != nil {
		t.Fatal(err)
	}
	if len(byStub) != 1 || byStub[0].ContainerId != state.ContainerId {
		t.Fatalf("expected stub index to return container %q, got %+v", state.ContainerId, byStub)
	}

	byWorkspace, err := repo.GetActiveContainersByWorkspaceId(state.WorkspaceId)
	if err != nil {
		t.Fatal(err)
	}
	if len(byWorkspace) != 1 || byWorkspace[0].ContainerId != state.ContainerId {
		t.Fatalf("expected workspace index to return container %q, got %+v", state.ContainerId, byWorkspace)
	}
}

func TestCreateContainerStateAdmitsOneDuplicate(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	repo := NewContainerRedisRepositoryForTest(rdb)
	request := testContainerRequest("sandbox-duplicate", "workspace", 100)

	results := make(chan error, 2)
	for range 2 {
		go func() { results <- repo.CreateContainerStateWithConcurrencyLimit(nil, request.Clone()) }()
	}

	var admitted, duplicate int
	for range 2 {
		err := <-results
		var alreadyScheduled *types.ContainerAlreadyScheduledError
		switch {
		case err == nil:
			admitted++
		case errors.As(err, &alreadyScheduled):
			duplicate++
		default:
			t.Fatal(err)
		}
	}
	if admitted != 1 || duplicate != 1 {
		t.Fatalf("admitted=%d duplicate=%d, want 1 each", admitted, duplicate)
	}
}

func TestCreateContainerStateDoesNotOverwriteStopping(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	repo := NewContainerRedisRepositoryForTest(rdb)
	request := testContainerRequest("sandbox-stopping-duplicate", "workspace", 100)
	if err := repo.CreateContainerStateWithConcurrencyLimit(nil, request); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateContainerStatus(request.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhileStopping); err != nil {
		t.Fatal(err)
	}

	err = repo.CreateContainerStateWithConcurrencyLimit(nil, request.Clone())
	var duplicate *types.ContainerAlreadyScheduledError
	if !errors.As(err, &duplicate) {
		t.Fatalf("expected stopping duplicate to be rejected, got %v", err)
	}
	state, err := repo.GetContainerState(request.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != types.ContainerStatusStopping {
		t.Fatalf("status = %s, want stopping", state.Status)
	}
}

func TestContainerFailureRetentionAndCooldown(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	for containerID, exitCode := range map[string]int{
		"container-success": int(types.ContainerExitCodeSuccess),
		"container-failure": int(types.ContainerExitCodeUnknownError),
	} {
		if err := repo.SetContainerExitCode(containerID, exitCode); err != nil {
			t.Fatal(err)
		}
	}

	assertTTL := func(containerID string, want time.Duration) {
		t.Helper()
		got, err := rdb.TTL(context.Background(), common.RedisKeys.SchedulerContainerExitCode(containerID)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s exit code TTL = %s, want %s", containerID, got, want)
		}
	}

	assertTTL("container-success", types.ContainerExitCodeTTL)
	assertTTL("container-failure", types.ContainerFailureHistoryTTL)

	if err := repo.SetContainerFailureCooldown([]string{"container-failure"}); err != nil {
		t.Fatal(err)
	}
	assertTTL("container-failure", types.ContainerFailureCooldown)

	if err := repo.SetContainerFailureCooldown([]string{"container-failure"}); err != nil {
		t.Fatal(err)
	}
	assertTTL("container-failure", types.ContainerFailureCooldown)
}

func TestCreateContainerStateWithConcurrencyLimitUsesAtomicReservationAfterInit(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 100, CPUMillicoreLimit: 100_000}
	initRequest := testContainerRequest("sandbox-test-stub-init", "test-workspace", 100)
	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, initRequest); err != nil {
		t.Fatal(err)
	}

	lock, err := redislock.Obtain(
		context.Background(),
		rdb,
		common.RedisKeys.WorkspaceConcurrencyLimitLock(initRequest.WorkspaceId),
		time.Second,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(context.Background())

	request := testContainerRequest("sandbox-test-stub-after-init", initRequest.WorkspaceId, 100)
	startedAt := time.Now()
	err = repo.CreateContainerStateWithConcurrencyLimit(quota, request)
	if err != nil {
		t.Fatalf("expected initialized quota path to avoid workspace lock, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("initialized quota path waited on workspace lock for %s", elapsed)
	}
}

func TestCreateContainerStateWithConcurrencyLimitAllowsOnlyQuotaUnderParallelLoad(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 10_000}

	var successCount int64
	var throttleCount int64
	var wg sync.WaitGroup
	for i := 0; i < 120; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			request := testContainerRequest(fmt.Sprintf("sandbox-test-stub-parallel-%03d", i), "test-workspace", 100)
			err := repo.CreateContainerStateWithConcurrencyLimit(quota, request)
			if err == nil {
				atomic.AddInt64(&successCount, 1)
				return
			}

			var throttled *types.ThrottledByConcurrencyLimitError
			if errors.As(err, &throttled) {
				atomic.AddInt64(&throttleCount, 1)
				return
			}

			t.Errorf("unexpected error: %v", err)
		}(i)
	}
	wg.Wait()

	if successCount != 100 {
		t.Fatalf("expected 100 successful reservations, got %d", successCount)
	}
	if throttleCount != 20 {
		t.Fatalf("expected 20 throttled reservations, got %d", throttleCount)
	}
}

func TestCheckContainerConcurrencyLimitRejectsWithoutReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-check-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-check-second", "test-workspace", 1)

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	err = repo.CheckContainerConcurrencyLimit(quota, secondRequest)
	var throttled *types.ThrottledByConcurrencyLimitError
	if !errors.As(err, &throttled) {
		t.Fatalf("expected preflight to throttle second request, got %v", err)
	}

	if _, err := repo.GetContainerState(secondRequest.ContainerId); err == nil {
		t.Fatal("expected preflight not to create container state")
	}

	reservationExists, err := rdb.Exists(
		context.Background(),
		common.RedisKeys.WorkspaceConcurrencyLimitReservation(secondRequest.WorkspaceId, secondRequest.ContainerId),
	).Result()
	if err != nil {
		t.Fatal(err)
	}
	if reservationExists != 0 {
		t.Fatal("expected preflight not to create a concurrency reservation")
	}
}

func TestCheckContainerConcurrencyLimitDoesNotReserveCapacity(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-check-capacity-first", "test-workspace", 40)
	secondRequest := testContainerRequest("sandbox-test-stub-check-capacity-second", "test-workspace", 60)

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	if err := repo.CheckContainerConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected preflight to allow second request, got %v", err)
	}

	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(firstRequest.WorkspaceId)
	usedCPU, err := rdb.HGet(context.Background(), usageKey, "cpu").Int64()
	if err != nil {
		t.Fatal(err)
	}
	if usedCPU != firstRequest.Cpu {
		t.Fatalf("expected preflight not to reserve CPU, got %d", usedCPU)
	}

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected authoritative reservation to admit second request, got %v", err)
	}
}

func TestUpdateContainerStatusStoppingReleasesConcurrencyReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-release-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-release-second", "test-workspace", 100)

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	err = repo.CreateContainerStateWithConcurrencyLimit(quota, secondRequest)
	var throttled *types.ThrottledByConcurrencyLimitError
	if !errors.As(err, &throttled) {
		t.Fatalf("expected second request to be throttled, got %v", err)
	}

	err = repo.UpdateContainerStatus(firstRequest.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending)
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected quota to be released after STOPPING status, got %v", err)
	}

	indexed, err := rdb.SIsMember(
		context.Background(),
		common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(firstRequest.WorkspaceId),
		firstRequest.ContainerId,
	).Result()
	if err != nil {
		t.Fatal(err)
	}
	if indexed {
		t.Fatal("expected STOPPING release to remove reservation index entry")
	}
}

func TestUpdateContainerStatusEnforcesMonotonicTransitionsAndLease(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	for _, test := range []struct {
		name            string
		storedStatus    types.ContainerStatus
		storedTTL       int64
		requestedStatus types.ContainerStatus
		requestedTTL    int64
		wantStatus      types.ContainerStatus
		wantTTL         int64
	}{
		{name: "pending stays pending", storedStatus: types.ContainerStatusPending, storedTTL: 401, requestedStatus: types.ContainerStatusPending, requestedTTL: 402, wantStatus: types.ContainerStatusPending, wantTTL: 402},
		{name: "pending becomes running", storedStatus: types.ContainerStatusPending, storedTTL: 403, requestedStatus: types.ContainerStatusRunning, requestedTTL: 404, wantStatus: types.ContainerStatusRunning, wantTTL: 404},
		{name: "pending becomes stopping", storedStatus: types.ContainerStatusPending, storedTTL: 405, requestedStatus: types.ContainerStatusStopping, requestedTTL: 406, wantStatus: types.ContainerStatusStopping, wantTTL: 406},
		{name: "running stays running", storedStatus: types.ContainerStatusRunning, storedTTL: 407, requestedStatus: types.ContainerStatusRunning, requestedTTL: 408, wantStatus: types.ContainerStatusRunning, wantTTL: 408},
		{name: "running becomes stopping", storedStatus: types.ContainerStatusRunning, storedTTL: 409, requestedStatus: types.ContainerStatusStopping, requestedTTL: 410, wantStatus: types.ContainerStatusStopping, wantTTL: 410},
		{name: "running rejects pending", storedStatus: types.ContainerStatusRunning, storedTTL: 411, requestedStatus: types.ContainerStatusPending, requestedTTL: 412, wantStatus: types.ContainerStatusRunning, wantTTL: 411},
		{name: "stopping stays stopping", storedStatus: types.ContainerStatusStopping, storedTTL: 413, requestedStatus: types.ContainerStatusStopping, requestedTTL: 414, wantStatus: types.ContainerStatusStopping, wantTTL: 414},
		{name: "stopping rejects pending", storedStatus: types.ContainerStatusStopping, storedTTL: 415, requestedStatus: types.ContainerStatusPending, requestedTTL: 416, wantStatus: types.ContainerStatusStopping, wantTTL: 415},
		{name: "stopping rejects running", storedStatus: types.ContainerStatusStopping, storedTTL: 417, requestedStatus: types.ContainerStatusRunning, requestedTTL: 418, wantStatus: types.ContainerStatusStopping, wantTTL: 417},
	} {
		t.Run(test.name, func(t *testing.T) {
			containerID := "sandbox-transition-" + strings.ReplaceAll(test.name, " ", "-")
			request := testContainerRequest(containerID, "test-workspace", 100)
			if err := repo.CreateContainerStateWithConcurrencyLimit(nil, request); err != nil {
				t.Fatal(err)
			}
			if test.storedStatus != types.ContainerStatusPending {
				if err := repo.UpdateContainerStatus(request.ContainerId, test.storedStatus, test.storedTTL); err != nil {
					t.Fatal(err)
				}
			} else if err := rdb.Expire(
				context.Background(),
				common.RedisKeys.SchedulerContainerState(request.ContainerId),
				time.Duration(test.storedTTL)*time.Second,
			).Err(); err != nil {
				t.Fatal(err)
			}

			if err := repo.UpdateContainerStatus(request.ContainerId, test.requestedStatus, test.requestedTTL); err != nil {
				t.Fatal(err)
			}

			state, err := repo.GetContainerState(request.ContainerId)
			if err != nil {
				t.Fatal(err)
			}
			if state.Status != test.wantStatus {
				t.Fatalf("status = %s, want %s", state.Status, test.wantStatus)
			}
			assertContainerStateTTL(t, rdb, request.ContainerId, time.Duration(test.wantTTL)*time.Second)
		})
	}
}

func TestStoppingWinsConcurrentHeartbeatUpdates(t *testing.T) {
	for _, heartbeatStatus := range []types.ContainerStatus{
		types.ContainerStatusPending,
		types.ContainerStatusRunning,
	} {
		t.Run(string(heartbeatStatus), func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			if err != nil {
				t.Fatal(err)
			}
			repo := NewContainerRedisRepositoryForTest(rdb)

			for i := range 8 {
				request := testContainerRequest(fmt.Sprintf("sandbox-stop-heartbeat-%s-%02d", heartbeatStatus, i), "test-workspace", 100)
				if err := repo.CreateContainerStateWithConcurrencyLimit(nil, request); err != nil {
					t.Fatal(err)
				}

				start := make(chan struct{})
				stopResult := make(chan error, 1)
				heartbeatResult := make(chan error, 1)
				go func() {
					<-start
					marked, err := repo.MarkPendingContainerStoppingIfUnassigned(
						request.ContainerId,
						types.ContainerStateTtlSWhileStopping,
					)
					if err == nil && !marked {
						err = repo.UpdateContainerStatus(
							request.ContainerId,
							types.ContainerStatusStopping,
							types.ContainerStateTtlSWhileStopping,
						)
					}
					stopResult <- err
				}()
				go func() {
					<-start
					heartbeatResult <- repo.UpdateContainerStatus(
						request.ContainerId,
						heartbeatStatus,
						types.ContainerStateTtlS,
					)
				}()
				close(start)

				if err := <-stopResult; err != nil {
					t.Fatal(err)
				}
				if err := <-heartbeatResult; err != nil {
					t.Fatal(err)
				}

				state, err := repo.GetContainerState(request.ContainerId)
				if err != nil {
					t.Fatal(err)
				}
				if state.Status != types.ContainerStatusStopping {
					t.Fatalf("status = %s, want STOPPING", state.Status)
				}
				assertContainerStateTTL(t, rdb, request.ContainerId, time.Duration(types.ContainerStateTtlSWhileStopping)*time.Second)
			}
		})
	}
}

func TestReserveContainerConcurrencyForPendingPreservesStateAndLease(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	request := testContainerRequest("sandbox-private-fallback", "test-workspace", 100)
	if err := repo.CreateContainerStateWithConcurrencyLimit(nil, request); err != nil {
		t.Fatal(err)
	}
	stateBefore, err := repo.GetContainerState(request.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	const leaseSeconds = int64(777)
	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	if err := rdb.Expire(context.Background(), stateKey, time.Duration(leaseSeconds)*time.Second).Err(); err != nil {
		t.Fatal(err)
	}

	quota := &types.ConcurrencyLimit{CPUMillicoreLimit: 1_000}
	if err := repo.ReserveContainerConcurrencyForPending(quota, request); err != nil {
		t.Fatal(err)
	}

	stateAfter, err := repo.GetContainerState(request.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	if *stateAfter != *stateBefore {
		t.Fatalf("fallback reservation rewrote state:\nbefore: %+v\nafter:  %+v", stateBefore, stateAfter)
	}
	assertContainerStateTTL(t, rdb, request.ContainerId, time.Duration(leaseSeconds)*time.Second)
	reserved, err := rdb.Exists(
		context.Background(),
		common.RedisKeys.WorkspaceConcurrencyLimitReservation(request.WorkspaceId, request.ContainerId),
	).Result()
	if err != nil {
		t.Fatal(err)
	}
	if reserved != 1 {
		t.Fatal("expected managed fallback concurrency reservation")
	}
}

func TestReserveContainerConcurrencyForPendingCannotRaceStopOrDelete(t *testing.T) {
	for _, test := range []struct {
		name              string
		counterState      string
		stop              func(ContainerRepository, string) error
		wantStateNotFound bool
	}{
		{
			name:         "workspace initialization does not block stopping",
			counterState: "",
			stop: func(repo ContainerRepository, containerID string) error {
				return repo.UpdateContainerStatus(containerID, types.ContainerStatusStopping, types.ContainerStateTtlSWhileStopping)
			},
		},
		{
			name:              "workspace repair does not block delete",
			counterState:      concurrencyCounterRepairing,
			wantStateNotFound: true,
			stop: func(repo ContainerRepository, containerID string) error {
				return repo.DeleteContainerState(containerID)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			rdb, err := common.NewRedisClient(types.RedisConfig{
				Addrs: []string{server.Addr()},
				Mode:  types.RedisModeSingle,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = rdb.Close() })

			repo := NewContainerRedisRepositoryForTest(rdb)
			quota := &types.ConcurrencyLimit{CPUMillicoreLimit: 1_000_000}
			request := testContainerRequest("sandbox-fallback-slow-accounting", "test-workspace", 100)
			if err := repo.CreateContainerStateWithConcurrencyLimit(nil, request); err != nil {
				t.Fatal(err)
			}
			if test.counterState != "" {
				if err := rdb.HSet(context.Background(), common.RedisKeys.WorkspaceConcurrencyLimitUsage(request.WorkspaceId),
					"gpu_count", 0,
					"cpu", 0,
					"initialized", test.counterState,
					"updated_at", time.Now().Unix(),
				).Err(); err != nil {
					t.Fatal(err)
				}
			}

			workspaceLock, err := redislock.Obtain(
				context.Background(),
				rdb,
				common.RedisKeys.WorkspaceConcurrencyLimitLock(request.WorkspaceId),
				time.Minute,
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			var finishOnce sync.Once
			finishWorkspaceAccounting := func() {
				finishOnce.Do(func() {
					if err := rdb.HSet(context.Background(), common.RedisKeys.WorkspaceConcurrencyLimitUsage(request.WorkspaceId),
						"gpu_count", 0,
						"cpu", 0,
						"initialized", concurrencyCounterInitialized,
						"updated_at", time.Now().Unix(),
					).Err(); err != nil {
						t.Errorf("finish workspace accounting: %v", err)
					}
					if err := workspaceLock.Release(context.Background()); err != nil {
						t.Errorf("release workspace lock: %v", err)
					}
				})
			}
			defer finishWorkspaceAccounting()

			workspaceLockAttempted := newRedisKeyCommandBarrier(
				common.RedisKeys.WorkspaceConcurrencyLimitLock(request.WorkspaceId),
			)
			rdb.AddHook(workspaceLockAttempted)
			reserveResult := make(chan error, 1)
			go func() {
				reserveResult <- repo.ReserveContainerConcurrencyForPending(quota, request)
			}()
			select {
			case <-workspaceLockAttempted.reached:
			case <-time.After(time.Second):
				t.Fatal("fallback reservation did not attempt workspace accounting lock")
			}
			select {
			case err := <-reserveResult:
				t.Fatalf("fallback did not remain blocked in workspace accounting: %v", err)
			default:
			}

			stopResult := make(chan error, 1)
			go func() { stopResult <- test.stop(repo, request.ContainerId) }()
			select {
			case err := <-stopResult:
				if err != nil {
					t.Fatalf("stop/delete failed while workspace accounting was blocked: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("slow workspace accounting held the per-container lifecycle lock")
			}

			finishWorkspaceAccounting()
			reserveErr := <-reserveResult
			if reserveErr == nil {
				t.Fatal("fallback reserved concurrency after stop/delete completed")
			}
			var notFound *types.ErrContainerStateNotFound
			if !errors.As(reserveErr, &notFound) && !strings.Contains(reserveErr.Error(), "no longer pending") {
				t.Fatalf("unexpected fallback result after stop/delete: %v", reserveErr)
			}

			state, stateErr := repo.GetContainerState(request.ContainerId)
			if test.wantStateNotFound {
				if !errors.As(stateErr, &notFound) {
					t.Fatalf("deleted state was recreated: state=%+v err=%v", state, stateErr)
				}
			} else {
				if stateErr != nil {
					t.Fatal(stateErr)
				}
				if state.Status != types.ContainerStatusStopping {
					t.Fatalf("status = %s, want STOPPING", state.Status)
				}
				assertContainerStateTTL(t, rdb, request.ContainerId, time.Duration(types.ContainerStateTtlSWhileStopping)*time.Second)
			}

			reserved, err := rdb.Exists(
				context.Background(),
				common.RedisKeys.WorkspaceConcurrencyLimitReservation(request.WorkspaceId, request.ContainerId),
			).Result()
			if err != nil {
				t.Fatal(err)
			}
			if reserved != 0 {
				t.Fatal("stop/delete left a managed fallback concurrency reservation")
			}
		})
	}
}

func TestUpdateContainerStatusDoesNotRecreateMissingState(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	containerID := "container-missing-update"
	err = repo.UpdateContainerStatus(containerID, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending)
	var notFound *types.ErrContainerStateNotFound
	if !errors.As(err, &notFound) {
		t.Fatalf("expected missing state error, got %v", err)
	}

	exists, err := rdb.Exists(context.Background(), common.RedisKeys.SchedulerContainerState(containerID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Fatal("missing state was recreated by status update")
	}
}

func TestMarkPendingContainerStoppingIfUnassigned(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	for _, test := range []struct {
		name     string
		state    *types.ContainerState
		expected bool
	}{
		{
			name: "unassigned pending container",
			state: &types.ContainerState{
				ContainerId: "pending-unassigned",
				Status:      types.ContainerStatusPending,
			},
			expected: true,
		},
		{
			name: "assigned pending container",
			state: &types.ContainerState{
				ContainerId: "pending-assigned",
				Status:      types.ContainerStatusPending,
				WorkerId:    "worker-1",
			},
		},
		{
			name: "running container",
			state: &types.ContainerState{
				ContainerId: "running",
				Status:      types.ContainerStatusRunning,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := repo.SetContainerState(test.state.ContainerId, test.state); err != nil {
				t.Fatal(err)
			}

			marked, err := repo.MarkPendingContainerStoppingIfUnassigned(test.state.ContainerId, types.ContainerStateTtlSWhileStopping)
			if err != nil {
				t.Fatal(err)
			}
			if marked != test.expected {
				t.Fatalf("expected marked=%t, got %t", test.expected, marked)
			}

			state, err := repo.GetContainerState(test.state.ContainerId)
			if err != nil {
				t.Fatal(err)
			}
			expectedStatus := test.state.Status
			if test.expected {
				expectedStatus = types.ContainerStatusStopping
			}
			if state.Status != expectedStatus {
				t.Fatalf("expected status %s, got %s", expectedStatus, state.Status)
			}
			if test.expected {
				ttl, err := rdb.TTL(context.Background(), common.RedisKeys.SchedulerContainerState(test.state.ContainerId)).Result()
				if err != nil {
					t.Fatal(err)
				}
				if ttl != time.Duration(types.ContainerStateTtlSWhileStopping)*time.Second {
					t.Fatalf("expected STOPPING TTL %ds, got %s", types.ContainerStateTtlSWhileStopping, ttl)
				}
			}
		})
	}
}

func TestBackendRoutesAreIndexedByMachine(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	revisionKey := common.RedisKeys.SchedulerBackendRouteMachineRevision("workspace-one", "pool-one", "machine-one")
	pubsubCtx, cancelPubsub := context.WithCancel(ctx)
	defer cancelPubsub()
	messages, errs := rdb.Subscribe(pubsubCtx, revisionKey)

	route := types.BackendRoute{
		RouteID:     "route-one",
		WorkspaceID: "workspace-one",
		PoolName:    "pool-one",
		MachineID:   "machine-one",
		WorkerID:    "worker-one",
		ContainerID: "container-one",
		Kind:        types.BackendRouteKindContainer,
		Port:        8001,
		Transport:   types.BackendRouteTransportTSNet,
		ProxyTarget: "machine-one.tailnet:29443",
		State:       types.BackendRouteStateReady,
	}
	if err := repo.SetBackendRoute(ctx, route); err != nil {
		t.Fatal(err)
	}
	if rev := rdb.Get(ctx, revisionKey).Val(); rev != "1" {
		t.Fatalf("route machine revision after create = %q, want 1", rev)
	}
	select {
	case message := <-messages:
		if message.Channel != revisionKey {
			t.Fatalf("route machine event channel = %q, want %q", message.Channel, revisionKey)
		}
		if message.Payload != common.KeyOperationSet {
			t.Fatalf("route machine event payload = %q, want %q", message.Payload, common.KeyOperationSet)
		}
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for route machine event")
	}

	if err := repo.SetBackendRoute(ctx, types.BackendRoute{
		RouteID:     "route-two",
		WorkspaceID: "workspace-one",
		PoolName:    "pool-one",
		MachineID:   "machine-two",
		ContainerID: "container-two",
		Kind:        types.BackendRouteKindContainer,
		Port:        8001,
	}); err != nil {
		t.Fatal(err)
	}

	routes, err := repo.ListBackendRoutesByMachine(ctx, "workspace-one", "pool-one", "machine-one")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0].RouteID != route.RouteID {
		t.Fatalf("routes = %#v, want only %s", routes, route.RouteID)
	}

	if err := repo.DeleteBackendRoutesByContainerID(ctx, route.ContainerID); err != nil {
		t.Fatal(err)
	}
	if rev := rdb.Get(ctx, revisionKey).Val(); rev != "2" {
		t.Fatalf("route machine revision after delete = %q, want 2", rev)
	}
	routes, err = repo.ListBackendRoutesByMachine(ctx, "workspace-one", "pool-one", "machine-one")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 0 {
		t.Fatalf("routes after delete = %#v, want empty", routes)
	}
}

func TestReadyWorkerRouteSurvivesOneHundredConcurrentRegistrations(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	ready := testReadyWorkerRoute("machine-one", "worker-one")
	if err := repo.SetBackendRoute(ctx, ready); err != nil {
		t.Fatal(err)
	}

	opening := ready
	opening.WorkspaceID = "buyer-workspace"
	opening.ProxyTarget = ""
	opening.State = types.BackendRouteStateOpening
	opening.UpdatedAt = 0

	const starts = 100
	start := make(chan struct{})
	errs := make(chan error, starts)
	var wg sync.WaitGroup
	for range starts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- repo.SetBackendRoute(ctx, opening)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent route registration failed: %v", err)
		}
	}

	got, err := repo.GetBackendRoute(ctx, ready.RouteID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != types.BackendRouteStateReady {
		t.Fatalf("route state = %q, want ready", got.State)
	}
	if got.ProxyTarget != ready.ProxyTarget {
		t.Fatalf("proxy target = %q, want %q", got.ProxyTarget, ready.ProxyTarget)
	}
	if got.UpdatedAt != ready.UpdatedAt {
		t.Fatalf("updated at = %d, want %d", got.UpdatedAt, ready.UpdatedAt)
	}
}

func TestReadyWorkerRoutesRemainIndependentDuringSameHostBurst(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	const (
		workers         = 4
		startsPerWorker = 25
	)
	routes := make([]types.BackendRoute, 0, workers)
	for workerIndex := range workers {
		route := testReadyWorkerRoute("shared-machine", fmt.Sprintf("worker-%d", workerIndex))
		if err := repo.SetBackendRoute(ctx, route); err != nil {
			t.Fatal(err)
		}
		routes = append(routes, route)
	}

	start := make(chan struct{})
	errs := make(chan error, workers*startsPerWorker)
	var wg sync.WaitGroup
	for _, ready := range routes {
		opening := ready
		opening.ProxyTarget = ""
		opening.State = types.BackendRouteStateOpening
		opening.UpdatedAt = 0
		for range startsPerWorker {
			wg.Add(1)
			go func(route types.BackendRoute) {
				defer wg.Done()
				<-start
				errs <- repo.SetBackendRoute(ctx, route)
			}(opening)
		}
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent route registration failed: %v", err)
		}
	}
	for _, ready := range routes {
		got, err := repo.GetBackendRoute(ctx, ready.RouteID)
		if err != nil {
			t.Fatal(err)
		}
		if got.State != types.BackendRouteStateReady || got.ProxyTarget != ready.ProxyTarget {
			t.Fatalf("route %s = state %q target %q, want ready target %q", got.RouteID, got.State, got.ProxyTarget, ready.ProxyTarget)
		}
	}
}

func TestChangedWorkerRouteReopens(t *testing.T) {
	tests := []struct {
		name   string
		change func(*types.BackendRoute)
	}{
		{
			name: "local target",
			change: func(route *types.BackendRoute) {
				route.LocalTarget = "127.0.0.1:32001"
			},
		},
		{
			name: "transport",
			change: func(route *types.BackendRoute) {
				route.Transport = types.BackendRouteTransportLocalDirect
			},
		},
		{
			name: "pool",
			change: func(route *types.BackendRoute) {
				route.PoolName = "pool-two"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			if err != nil {
				t.Fatal(err)
			}

			repo := NewContainerRedisRepositoryForTest(rdb)
			ctx := context.Background()
			ready := testReadyWorkerRoute("machine-one", "worker-one")
			if err := repo.SetBackendRoute(ctx, ready); err != nil {
				t.Fatal(err)
			}

			opening := ready
			opening.ProxyTarget = ""
			opening.State = types.BackendRouteStateOpening
			opening.UpdatedAt = 0
			test.change(&opening)
			if err := repo.SetBackendRoute(ctx, opening); err != nil {
				t.Fatal(err)
			}

			got, err := repo.GetBackendRoute(ctx, ready.RouteID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != types.BackendRouteStateOpening {
				t.Fatalf("route state = %q, want opening", got.State)
			}
			if got.ProxyTarget != "" {
				t.Fatalf("proxy target = %q, want empty", got.ProxyTarget)
			}
		})
	}
}

func TestBackendRoutesAreIndexedByMachineID(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	revisionKey := common.RedisKeys.SchedulerBackendRouteMachineIDRevision("machine-one")
	pubsubCtx, cancelPubsub := context.WithCancel(ctx)
	defer cancelPubsub()
	messages, errs := rdb.Subscribe(pubsubCtx, revisionKey)

	route := types.BackendRoute{
		RouteID:     "route-one",
		WorkspaceID: "buyer-one",
		PoolName:    "marketplace-one",
		MachineID:   "machine-one",
		WorkerID:    "worker-one",
		ContainerID: "container-one",
		Kind:        types.BackendRouteKindContainer,
		Port:        8001,
		Transport:   types.BackendRouteTransportTSNet,
		State:       types.BackendRouteStateOpening,
	}
	if err := repo.SetBackendRoute(ctx, route); err != nil {
		t.Fatal(err)
	}
	if rev := rdb.Get(ctx, revisionKey).Val(); rev != "1" {
		t.Fatalf("route machine id revision after create = %q, want 1", rev)
	}
	select {
	case message := <-messages:
		if message.Channel != revisionKey {
			t.Fatalf("route machine id event channel = %q, want %q", message.Channel, revisionKey)
		}
		if message.Payload != common.KeyOperationSet {
			t.Fatalf("route machine id event payload = %q, want %q", message.Payload, common.KeyOperationSet)
		}
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for route machine id event")
	}

	if err := repo.SetBackendRoute(ctx, types.BackendRoute{
		RouteID:     "route-two",
		WorkspaceID: "buyer-one",
		PoolName:    "marketplace-one",
		MachineID:   "machine-two",
		ContainerID: "container-two",
		Kind:        types.BackendRouteKindContainer,
		Port:        8001,
	}); err != nil {
		t.Fatal(err)
	}

	routes, err := repo.ListBackendRoutesByMachineID(ctx, "machine-one")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0].RouteID != route.RouteID {
		t.Fatalf("routes = %#v, want only %s", routes, route.RouteID)
	}
}

func TestDeleteBackendRoutesByMachineRemovesRoutesIndexedUnderBuyerWorkspace(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	route := types.BackendRoute{
		RouteID:     "route-one",
		WorkspaceID: "buyer-one",
		PoolName:    "marketplace-one",
		MachineID:   "machine-one",
		WorkerID:    "worker-one",
		ContainerID: "container-one",
		Kind:        types.BackendRouteKindContainer,
		Port:        8001,
		Transport:   types.BackendRouteTransportTSNet,
		State:       types.BackendRouteStateReady,
	}
	if err := repo.SetBackendRoute(ctx, route); err != nil {
		t.Fatal(err)
	}

	if err := repo.DeleteBackendRoutesByMachine(ctx, "seller-one", "marketplace-one", "machine-one"); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetBackendRoute(ctx, route.RouteID); err == nil {
		t.Fatal("route still exists after machine release")
	}
	routes, err := repo.ListBackendRoutesByMachineID(ctx, "machine-one")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 0 {
		t.Fatalf("machine id routes after delete = %#v, want empty", routes)
	}
	routes, err = repo.ListBackendRoutesByMachine(ctx, "buyer-one", "marketplace-one", "machine-one")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 0 {
		t.Fatalf("buyer workspace routes after delete = %#v, want empty", routes)
	}
}

func TestDeleteBackendRoutesByContainerIDKeepsSiblingContainerRoutesOnSameMachine(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	routeA := types.BackendRoute{
		RouteID:     types.BackendRouteID("machine-one", "worker-one", "container-a", types.BackendRouteKindContainer, 8765),
		WorkspaceID: "workspace-one",
		PoolName:    "pool-one",
		MachineID:   "machine-one",
		WorkerID:    "worker-one",
		ContainerID: "container-a",
		Kind:        types.BackendRouteKindContainer,
		Port:        8765,
		Transport:   types.BackendRouteTransportTSNet,
		ProxyTarget: "machine-one.tailnet:29443",
		State:       types.BackendRouteStateReady,
	}
	routeB := routeA
	routeB.RouteID = types.BackendRouteID("machine-one", "worker-one", "container-b", types.BackendRouteKindContainer, 8765)
	routeB.ContainerID = "container-b"

	if err := repo.SetBackendRoute(ctx, routeA); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetBackendRoute(ctx, routeB); err != nil {
		t.Fatal(err)
	}

	if err := repo.DeleteBackendRoutesByContainerID(ctx, routeA.ContainerID); err != nil {
		t.Fatal(err)
	}

	routes, err := repo.ListBackendRoutesByMachine(ctx, "workspace-one", "pool-one", "machine-one")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0].RouteID != routeB.RouteID {
		t.Fatalf("routes after deleting container-a = %#v, want only %s", routes, routeB.RouteID)
	}
	if _, err := repo.GetBackendRoute(ctx, routeB.RouteID); err != nil {
		t.Fatalf("sibling route was removed: %v", err)
	}
}

func testReadyWorkerRoute(machineID, workerID string) types.BackendRoute {
	return types.BackendRoute{
		RouteID:     types.BackendRouteID(machineID, workerID, "", types.BackendRouteKindWorker, 0),
		WorkspaceID: "workspace-one",
		PoolName:    "pool-one",
		MachineID:   machineID,
		WorkerID:    workerID,
		Kind:        types.BackendRouteKindWorker,
		Protocol:    types.BackendRouteProtocolTCP,
		Transport:   types.BackendRouteTransportTSNet,
		LocalTarget: "127.0.0.1:32000",
		ProxyTarget: machineID + ".tailnet:29443",
		State:       types.BackendRouteStateReady,
		UpdatedAt:   123,
	}
}

func TestUpdateContainerStatusStoppingRetriesConcurrencyReleaseAfterTransientFailure(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-release-retry-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-release-retry-second", "test-workspace", 100)
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(firstRequest.WorkspaceId)

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	if err := rdb.HSet(context.Background(), usageKey, "cpu", "not-an-int").Err(); err != nil {
		t.Fatal(err)
	}

	err = repo.UpdateContainerStatus(firstRequest.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending)
	if err == nil {
		t.Fatal("expected transient release error")
	}

	state, err := repo.GetContainerState(firstRequest.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != types.ContainerStatusStopping {
		t.Fatalf("expected status to be persisted as STOPPING, got %s", state.Status)
	}

	if err := rdb.HSet(context.Background(), usageKey, "cpu", firstRequest.Cpu).Err(); err != nil {
		t.Fatal(err)
	}

	if err := repo.UpdateContainerStatus(firstRequest.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending); err != nil {
		t.Fatalf("expected retry to release existing reservation, got %v", err)
	}

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected quota to be available after retry release, got %v", err)
	}

	indexed, err := rdb.SIsMember(
		context.Background(),
		common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(firstRequest.WorkspaceId),
		firstRequest.ContainerId,
	).Result()
	if err != nil {
		t.Fatal(err)
	}
	if indexed {
		t.Fatal("expected retry release to remove reservation index entry")
	}
}

func TestUpdateContainerStatusStoppingReleasesDuringCounterRepair(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-release-repair-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-release-repair-second", "test-workspace", 100)
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(firstRequest.WorkspaceId)

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	if err := rdb.HSet(context.Background(), usageKey, "initialized", concurrencyCounterRepairing).Err(); err != nil {
		t.Fatal(err)
	}

	if err := repo.UpdateContainerStatus(firstRequest.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending); err != nil {
		t.Fatalf("expected STOPPING release to wait for repair and succeed, got %v", err)
	}

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected quota to be available after repair-time release, got %v", err)
	}
}

func TestDeleteContainerStateReleasesConcurrencyReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-delete-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-delete-second", "test-workspace", 100)

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	if err := repo.DeleteContainerState(firstRequest.ContainerId); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.GetContainerState(firstRequest.ContainerId); err == nil {
		t.Fatal("expected deleted container state to be gone")
	}

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected quota to be released after delete, got %v", err)
	}

	indexed, err := rdb.SIsMember(
		context.Background(),
		common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(firstRequest.WorkspaceId),
		firstRequest.ContainerId,
	).Result()
	if err != nil {
		t.Fatal(err)
	}
	if indexed {
		t.Fatal("expected delete to remove reservation index entry")
	}
}

func TestDeleteContainerStateCleansIndexesByExitHistory(t *testing.T) {
	for _, test := range []struct {
		name              string
		exitCode          types.ContainerExitCode
		setExitCode       bool
		retainStubHistory bool
	}{
		{name: "pending cancellation without an exit lease"},
		{name: "non-failure stop", exitCode: types.ContainerExitCodeUser, setExitCode: true},
		{name: "failed exit", exitCode: types.ContainerExitCodeUnknownError, setExitCode: true, retainStubHistory: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			if err != nil {
				t.Fatal(err)
			}

			repo := NewContainerRedisRepositoryForTest(rdb)
			state := &types.ContainerState{
				ContainerId: "endpoint-" + strings.ReplaceAll(test.name, " ", "-"),
				StubId:      "endpoint-stub",
				WorkspaceId: "test-workspace",
				WorkerId:    "worker-one",
				Status:      types.ContainerStatusRunning,
			}
			if err := repo.SetContainerState(state.ContainerId, state); err != nil {
				t.Fatal(err)
			}
			stateKey := common.RedisKeys.SchedulerContainerState(state.ContainerId)
			workerIndexKey := common.RedisKeys.SchedulerContainerWorkerIndex(state.WorkerId)
			if err := rdb.SAdd(context.Background(), workerIndexKey, stateKey).Err(); err != nil {
				t.Fatal(err)
			}
			if test.setExitCode {
				if err := repo.SetContainerExitCode(state.ContainerId, int(test.exitCode)); err != nil {
					t.Fatal(err)
				}
			}

			if err := repo.DeleteContainerState(state.ContainerId); err != nil {
				t.Fatal(err)
			}

			stubIndexKey := common.RedisKeys.SchedulerContainerIndex(state.StubId)
			indexed, err := rdb.SIsMember(context.Background(), stubIndexKey, stateKey).Result()
			if err != nil {
				t.Fatal(err)
			}
			if indexed != test.retainStubHistory {
				t.Fatalf("stub history indexed = %v, want %v", indexed, test.retainStubHistory)
			}
			for _, indexKey := range []string{
				common.RedisKeys.SchedulerContainerWorkspaceIndex(state.WorkspaceId),
				workerIndexKey,
			} {
				indexed, err := rdb.SIsMember(context.Background(), indexKey, stateKey).Result()
				if err != nil {
					t.Fatal(err)
				}
				if indexed {
					t.Fatalf("deleted container state remained in non-history index %s", indexKey)
				}
			}
			exists, err := rdb.Exists(context.Background(), common.RedisKeys.SchedulerContainerExitCode(state.ContainerId)).Result()
			if err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if test.setExitCode {
				want = 1
			}
			if exists != want {
				t.Fatalf("exit-code lease exists = %d, want %d", exists, want)
			}

			failed, err := repo.GetFailedContainersByStubId(state.StubId)
			if err != nil {
				t.Fatal(err)
			}
			if test.retainStubHistory && (len(failed) != 1 || failed[0] != state.ContainerId) {
				t.Fatalf("failed container history = %v, want %s", failed, state.ContainerId)
			}
			if !test.retainStubHistory && len(failed) != 0 {
				t.Fatalf("successful container appeared in failure history: %v", failed)
			}
		})
	}
}

func TestCreateContainerStateWithConcurrencyLimitRepairsStaleConcurrencyCounter(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-stale-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-stale-second", "test-workspace", 100)

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	// Simulate a state TTL expiry or missed cleanup after a worker failure. The
	// hot path counter still says the workspace is full, but the active state
	// index used for repair no longer includes the first container.
	if err := rdb.Del(context.Background(), common.RedisKeys.SchedulerContainerState(firstRequest.ContainerId)).Err(); err != nil {
		t.Fatal(err)
	}

	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(firstRequest.WorkspaceId)
	if err := rdb.HSet(context.Background(), usageKey, "repaired_at", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(context.Background(),
		common.RedisKeys.WorkspaceConcurrencyLimitReservation(firstRequest.WorkspaceId, firstRequest.ContainerId),
		"created_at", time.Now().Add(-concurrencyReservationInFlightTTL-time.Second).Unix(),
	).Err(); err != nil {
		t.Fatal(err)
	}

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected stale counter repair to admit second request, got %v", err)
	}

	usedCPU, err := rdb.HGet(context.Background(), usageKey, "cpu").Int64()
	if err != nil {
		t.Fatal(err)
	}
	if usedCPU != secondRequest.Cpu {
		t.Fatalf("expected repaired counter to track only active request CPU, got %d", usedCPU)
	}

	indexed, err := rdb.SIsMember(
		context.Background(),
		common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(firstRequest.WorkspaceId),
		firstRequest.ContainerId,
	).Result()
	if err != nil {
		t.Fatal(err)
	}
	if indexed {
		t.Fatal("expected stale reservation index entry to be removed during repair")
	}
}

func TestCreateContainerStateWithConcurrencyLimitPreservesInFlightReservationDuringRepair(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-inflight-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-inflight-second", "test-workspace", 100)

	if err := repo.CreateContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	if err := rdb.Del(context.Background(), common.RedisKeys.SchedulerContainerState(firstRequest.ContainerId)).Err(); err != nil {
		t.Fatal(err)
	}

	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(firstRequest.WorkspaceId)
	if err := rdb.HSet(context.Background(), usageKey, "repaired_at", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}

	err = repo.CreateContainerStateWithConcurrencyLimit(quota, secondRequest)
	var throttled *types.ThrottledByConcurrencyLimitError
	if !errors.As(err, &throttled) {
		t.Fatalf("expected recent in-flight reservation to be preserved and throttle second request, got %v", err)
	}

	usedCPU, err := rdb.HGet(context.Background(), usageKey, "cpu").Int64()
	if err != nil {
		t.Fatal(err)
	}
	if usedCPU != firstRequest.Cpu {
		t.Fatalf("expected repaired counter to preserve in-flight CPU, got %d", usedCPU)
	}
}

func TestTryReserveContainerConcurrencyWaitsDuringRepair(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb).(*ContainerRedisRepository)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	request := testContainerRequest("sandbox-test-stub-repairing", "test-workspace", 100)

	if err := rdb.HSet(context.Background(), common.RedisKeys.WorkspaceConcurrencyLimitUsage(request.WorkspaceId),
		"gpu_count", 0,
		"cpu", 0,
		"initialized", concurrencyCounterRepairing,
		"updated_at", time.Now().Unix(),
	).Err(); err != nil {
		t.Fatal(err)
	}

	_, err = repo.tryReserveContainerConcurrency(quota, request)
	if !errors.Is(err, errConcurrencyCounterRepairing) {
		t.Fatalf("expected repair-in-progress error, got %v", err)
	}
}

func TestCreateContainerStateWithConcurrencyLimitReturnsReservationReleaseError(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	request := testContainerRequest("sandbox-test-stub-state-release-error", "test-workspace", 100)

	stubIndexKey := common.RedisKeys.SchedulerContainerIndex(request.StubId)
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(request.WorkspaceId)
	reservationKey := common.RedisKeys.WorkspaceConcurrencyLimitReservation(request.WorkspaceId, request.ContainerId)

	if err := rdb.HSet(context.Background(), usageKey,
		"gpu_count", 0,
		"cpu", 0,
		"initialized", concurrencyCounterInitialized,
		"updated_at", time.Now().Unix(),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), reservationKey, "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}
	// Let the initial duplicate check pass, then fail the transactional state
	// write on its stub-index SADD after concurrency has been reserved.
	if err := rdb.Set(context.Background(), stubIndexKey, "not-a-set", 0).Err(); err != nil {
		t.Fatal(err)
	}

	err = repo.CreateContainerStateWithConcurrencyLimit(quota, request)
	if err == nil {
		t.Fatal("expected state write and reservation release errors")
	}

	if !strings.Contains(err.Error(), "failed to set container state") {
		t.Fatalf("expected state write error, got %v", err)
	}
	if !strings.Contains(err.Error(), "failed to release concurrency reservation") {
		t.Fatalf("expected reservation release error, got %v", err)
	}
}

func TestGetWorkerAddressShortCallerDeadlineDoesNotReportScheduleFailure(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	_, err = repo.GetWorkerAddress(ctx, "sandbox-test-stub-waiting")
	if err == nil {
		t.Fatal("expected caller deadline error")
	}
	if strings.Contains(err.Error(), "failed to schedule") {
		t.Fatalf("short caller deadline should not be reported as scheduler failure: %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("short caller deadline waited too long: %s", elapsed)
	}
}

func TestGetWorkerAddressReturnsScheduleFailureWhenRequestFailed(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	containerId := "sandbox-test-stub-failed"
	if err := repo.SetContainerRequestStatus(containerId, types.ContainerRequestStatusFailed); err != nil {
		t.Fatal(err)
	}

	_, err = repo.GetWorkerAddress(context.Background(), containerId)
	if err == nil || !strings.Contains(err.Error(), "failed to schedule") {
		t.Fatalf("expected scheduler failure, got %v", err)
	}
}

func TestEndpointRequestTokensCapConcurrentAcquireAcrossRepositories(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo1 := NewContainerRedisRepositoryForTest(rdb)
	repo2 := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	const maxTokens = 5
	const attempts = 25

	var acquired atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		repo := repo1
		if i%2 == 1 {
			repo = repo2
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := repo.AcquireEndpointRequestToken(ctx, "workspace", "stub", "container-1", maxTokens, 30*time.Second)
			if err != nil {
				t.Errorf("acquire endpoint request token: %v", err)
				return
			}
			if ok {
				acquired.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := acquired.Load(); got != maxTokens {
		t.Fatalf("acquired tokens = %d, want %d", got, maxTokens)
	}

	tokens, err := repo1.GetEndpointRequestTokens(ctx, "workspace", "stub", "container-1", maxTokens, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if tokens != 0 {
		t.Fatalf("remaining tokens = %d, want 0", tokens)
	}
}

func TestEndpointRequestTokenReleaseIsIdempotentAcrossRepositories(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo1 := NewContainerRedisRepositoryForTest(rdb)
	repo2 := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
	const maxTokens = 2

	for _, repo := range []ContainerRepository{repo1, repo2} {
		ok, err := repo.AcquireEndpointRequestToken(ctx, "workspace", "stub", "container-1", maxTokens, 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected request token acquire")
		}
	}

	if err := repo1.ReleaseEndpointRequestToken(ctx, "workspace", "stub", "container-1", "task-1", maxTokens, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := repo1.ReleaseEndpointRequestToken(ctx, "workspace", "stub", "container-1", "task-1", maxTokens, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	tokens, err := repo1.GetEndpointRequestTokens(ctx, "workspace", "stub", "container-1", maxTokens, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if tokens != 1 {
		t.Fatalf("tokens after duplicate release = %d, want 1", tokens)
	}

	if err := repo2.ReleaseEndpointRequestToken(ctx, "workspace", "stub", "container-1", "task-2", maxTokens, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	tokens, err = repo1.GetEndpointRequestTokens(ctx, "workspace", "stub", "container-1", maxTokens, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if tokens != maxTokens {
		t.Fatalf("tokens after second release = %d, want %d", tokens, maxTokens)
	}
}

func TestContainerRepositoryKeepWarmLocksApplySharedSemantics(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()

	if err := repo.SetPodKeepWarmLock(ctx, "workspace", "stub", "container-1", 30); err != nil {
		t.Fatal(err)
	}
	exists, err := repo.PodKeepWarmLockExists(ctx, "workspace", "stub", "container-1")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected pod keep-warm lock")
	}

	if err := repo.SetPodKeepWarmLock(ctx, "workspace", "stub", "container-1", 0); err != nil {
		t.Fatal(err)
	}
	exists, err = repo.PodKeepWarmLockExists(ctx, "workspace", "stub", "container-1")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected pod keep-warm lock to be cleared")
	}

	if err := rdb.Set(ctx, podKeepWarmLockKey("workspace", "stub", "container-1"), 0, 0).Err(); err != nil {
		t.Fatal(err)
	}
	exists, err = repo.PodKeepWarmLockExists(ctx, "workspace", "stub", "container-1")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected zero-valued pod keep-warm lock to be ignored")
	}

	if err := repo.SetPodKeepWarmLock(ctx, "workspace", "stub", "container-1", -1); err != nil {
		t.Fatal(err)
	}
	exists, err = repo.PodKeepWarmLockExists(ctx, "workspace", "stub", "container-1")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("expected pod keep-warm lock")
	}

	ttl, err := rdb.TTL(ctx, podKeepWarmLockKey("workspace", "stub", "container-1")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl != -1 {
		t.Fatalf("pod keep-warm ttl = %s, want no expiration", ttl)
	}
}

func TestRefreshBuildContainerTTLDoesNotRecreateExpiredLease(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	const containerId = "build-expired"

	if err := repo.SetBuildContainerTTL(containerId, time.Minute); err != nil {
		t.Fatal(err)
	}
	refreshed, err := repo.RefreshBuildContainerTTL(containerId, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed {
		t.Fatal("expected existing lease to refresh")
	}

	if err := rdb.Del(context.Background(), common.RedisKeys.ImageBuildContainerTTL(containerId)).Err(); err != nil {
		t.Fatal(err)
	}
	refreshed, err = repo.RefreshBuildContainerTTL(containerId, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	exists, err := rdb.Exists(context.Background(), common.RedisKeys.ImageBuildContainerTTL(containerId)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if refreshed || exists != 0 {
		t.Fatal("expired lease was recreated")
	}
}

func testContainerRequest(containerId, workspaceId string, cpu int64) *types.ContainerRequest {
	return &types.ContainerRequest{
		ContainerId: containerId,
		StubId:      "test-stub",
		WorkspaceId: workspaceId,
		Cpu:         cpu,
		Memory:      128,
		Stub: types.StubWithRelated{
			Stub: types.Stub{Type: types.StubType(types.StubTypeSandbox)},
		},
	}
}
