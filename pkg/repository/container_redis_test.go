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

func TestUpdateContainerStatusDoesNotMoveStoppingBackToRunning(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	request := testContainerRequest("sandbox-test-stub-stopping-wins", "test-workspace", 100)
	if err := repo.SetContainerStateWithConcurrencyLimit(nil, request); err != nil {
		t.Fatal(err)
	}

	if err := repo.UpdateContainerStatus(request.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateContainerStatus(request.ContainerId, types.ContainerStatusRunning, types.ContainerStateTtlS); err != nil {
		t.Fatal(err)
	}

	state, err := repo.GetContainerState(request.ContainerId)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != types.ContainerStatusStopping {
		t.Fatalf("expected STOPPING status to win over late RUNNING update, got %s", state.Status)
	}
}

func TestBackendRoutesAreIndexedByMachine(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	ctx := context.Background()
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
	revisionKey := common.RedisKeys.SchedulerBackendRouteMachineRevision("workspace-one", "pool-one", "machine-one")
	if rev := rdb.Get(ctx, revisionKey).Val(); rev != "1" {
		t.Fatalf("route machine revision after create = %q, want 1", rev)
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

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	if err := rdb.HSet(context.Background(), usageKey, "initialized", concurrencyCounterRepairing).Err(); err != nil {
		t.Fatal(err)
	}

	if err := repo.UpdateContainerStatus(firstRequest.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhilePending); err != nil {
		t.Fatalf("expected STOPPING release to wait for repair and succeed, got %v", err)
	}

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
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

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	if err := repo.DeleteContainerState(firstRequest.ContainerId); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.GetContainerState(firstRequest.ContainerId); err == nil {
		t.Fatal("expected deleted container state to be gone")
	}

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
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

func TestSetContainerStateWithConcurrencyLimitRepairsStaleConcurrencyCounter(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-stale-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-stale-second", "test-workspace", 100)

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
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

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
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

func TestSetContainerStateWithConcurrencyLimitPreservesInFlightReservationDuringRepair(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-inflight-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-inflight-second", "test-workspace", 100)

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
		t.Fatal(err)
	}

	if err := rdb.Del(context.Background(), common.RedisKeys.SchedulerContainerState(firstRequest.ContainerId)).Err(); err != nil {
		t.Fatal(err)
	}

	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(firstRequest.WorkspaceId)
	if err := rdb.HSet(context.Background(), usageKey, "repaired_at", time.Now().Add(-time.Minute).Unix()).Err(); err != nil {
		t.Fatal(err)
	}

	err = repo.SetContainerStateWithConcurrencyLimit(quota, secondRequest)
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
		"initialized", concurrencyCounterInitialized,
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
