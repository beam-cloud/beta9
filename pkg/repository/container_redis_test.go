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

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
)

func TestSetContainerStateWithConcurrencyLimitSkipsLockWithoutQuota(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	request := &types.ContainerRequest{
		ContainerId: "sandbox-test-stub-00000000",
		StubId:      "test-stub",
		WorkspaceId: "test-workspace",
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
	err = repo.SetContainerStateWithConcurrencyLimit(nil, request)
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
}

func TestSetContainerStateWithConcurrencyLimitUsesAtomicReservationAfterInit(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 100, CPUMillicoreLimit: 100_000}
	initRequest := testContainerRequest("sandbox-test-stub-init", "test-workspace", 100)
	if err := repo.SetContainerStateWithConcurrencyLimit(quota, initRequest); err != nil {
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
	err = repo.SetContainerStateWithConcurrencyLimit(quota, request)
	if err != nil {
		t.Fatalf("expected initialized quota path to avoid workspace lock, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("initialized quota path waited on workspace lock for %s", elapsed)
	}
}

func TestSetContainerStateWithConcurrencyLimitAllowsOnlyQuotaUnderParallelLoad(t *testing.T) {
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
			err := repo.SetContainerStateWithConcurrencyLimit(quota, request)
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

func TestUpdateContainerStatusStoppingReleasesConcurrencyReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-release-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-release-second", "test-workspace", 100)

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	err = repo.SetContainerStateWithConcurrencyLimit(quota, secondRequest)
	var throttled *types.ThrottledByConcurrencyLimitError
	if !errors.As(err, &throttled) {
		t.Fatalf("expected second request to be throttled, got %v", err)
	}

	err = repo.UpdateContainerStatus(firstRequest.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending)
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected quota to be released after STOPPING status, got %v", err)
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

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
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

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
		t.Fatalf("expected quota to be available after retry release, got %v", err)
	}
}

func TestSetContainerStateWithConcurrencyLimitReturnsReservationReleaseError(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	request := testContainerRequest("sandbox-test-stub-state-release-error", "test-workspace", 100)

	stateKey := common.RedisKeys.SchedulerContainerState(request.ContainerId)
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(request.WorkspaceId)
	reservationKey := common.RedisKeys.WorkspaceConcurrencyLimitReservation(request.WorkspaceId, request.ContainerId)

	if err := rdb.HSet(context.Background(), usageKey,
		"gpu_count", 0,
		"cpu", 0,
		"initialized", concurrencyUsageInitialized,
		"updated_at", time.Now().Unix(),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), reservationKey, "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), stateKey, "not-a-hash", 0).Err(); err != nil {
		t.Fatal(err)
	}

	err = repo.SetContainerStateWithConcurrencyLimit(quota, request)
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
