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

func TestCheckContainerConcurrencyLimitRejectsWithoutReservation(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}

	repo := NewContainerRedisRepositoryForTest(rdb)
	quota := &types.ConcurrencyLimit{GPULimit: 0, CPUMillicoreLimit: 100}
	firstRequest := testContainerRequest("sandbox-test-stub-check-first", "test-workspace", 100)
	secondRequest := testContainerRequest("sandbox-test-stub-check-second", "test-workspace", 1)

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
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

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, firstRequest); err != nil {
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

	if err := repo.SetContainerStateWithConcurrencyLimit(quota, secondRequest); err != nil {
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
