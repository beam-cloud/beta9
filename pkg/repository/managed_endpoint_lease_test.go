package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestManagedEndpointLeaseSurvivesGatewayOutageWithoutLosingCapacity(t *testing.T) {
	for _, evictable := range []bool{false, true} {
		name := "protected"
		if evictable {
			name = "preemptible"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			server := miniredis.RunT(t)
			rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
			require.NoError(t, err)
			t.Cleanup(func() { _ = rdb.Close() })
			containers := NewContainerRedisRepositoryForTest(rdb)
			workers := NewWorkerRedisRepositoryForTest(rdb)
			replicas := NewManagedEndpointRedisRepository(rdb)
			worker := &types.Worker{Id: "worker-model", Status: types.WorkerStatusAvailable, Gpu: "RTX5090",
				TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 1}
			model := &types.ContainerState{ContainerId: "managed-qwen", StubId: "qwen", WorkspaceId: "admin",
				Cpu: 1000, Memory: 1000, Gpu: "RTX5090", GpuCount: 1, Evictable: evictable}
			seedRunningContainer(t, rdb, worker.Id, model)
			require.NoError(t, replicas.SaveReplica(ctx, &types.EndpointReplica{ID: "replica-qwen", EndpointID: "qwen/qwen3",
				ContainerID: model.ContainerId, WorkerID: worker.Id, Status: types.ReplicaStatusReady}))
			require.NoError(t, workers.AddWorker(worker))
			require.NoError(t, workers.SetWorkerKeepAlive(worker.Id, types.WorkerKeepAlive{}))
			require.NoError(t, containers.UpdateContainerStatus(model.ContainerId, types.ContainerStatusRunning, types.ContainerStateTtlS))
			assertContainerStateTTL(t, rdb, model.ContainerId, time.Duration(types.ContainerStateTtlSManagedEndpoint)*time.Second)

			// A gateway outage left the control plane unavailable for four minutes.
			// The worker lease expires, but the process and its GPU ownership survive.
			server.FastForward(4 * time.Minute)
			_, err = workers.GetWorkerById(worker.Id)
			require.Error(t, err)
			state, err := containers.GetContainerState(model.ContainerId)
			require.NoError(t, err)
			require.Equal(t, worker.Id, state.WorkerId)
			require.Equal(t, types.ContainerStatusRunning, state.Status)

			// Agent reconciliation re-registers its stable worker ID. AddWorker
			// must derive capacity from the preserved assignment, not all-free input.
			reconnected := *worker
			reconnected.FreeCpu, reconnected.FreeMemory, reconnected.FreeGpuCount = 4000, 8000, 1
			require.NoError(t, workers.AddWorker(&reconnected))
			require.EqualValues(t, 0, reconnected.FreeGpuCount)
			if evictable {
				require.EqualValues(t, 1, reconnected.EvictableGpuCount)
			} else {
				require.EqualValues(t, 0, reconnected.EvictableGpuCount)
			}
			require.NoError(t, containers.UpdateContainerStatus(model.ContainerId, types.ContainerStatusRunning, types.ContainerStateTtlS))
			request := &types.ContainerRequest{ContainerId: "serverless-after-recovery", Cpu: 500, Memory: 500, Gpu: "RTX5090", GpuCount: 1}
			setPendingContainerRequests(t, rdb, request)
			err = workers.ScheduleContainerRequest(&reconnected, request)
			if !evictable {
				require.Error(t, err, "protected GPU cannot be scheduled over after reconnect")
				require.Empty(t, request.EvictContainerIds)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []string{model.ContainerId}, request.EvictContainerIds)
			state, err = containers.GetContainerState(model.ContainerId)
			require.NoError(t, err)
			require.Equal(t, types.ContainerStatusStopping, state.Status)
			require.True(t, state.Evicting)
			// A delayed RUNNING heartbeat neither revives the victim nor restores
			// the long outage lease after the scheduler has committed an eviction.
			require.NoError(t, containers.UpdateContainerStatus(model.ContainerId, types.ContainerStatusRunning, types.ContainerStateTtlS))
			require.NoError(t, containers.UpdateContainerStatus(model.ContainerId, types.ContainerStatusStopping, types.ContainerStateTtlSWhileStopping))
			require.NoError(t, containers.UpdateContainerStatus(model.ContainerId, types.ContainerStatusRunning, types.ContainerStateTtlS))
			assertContainerStateTTL(t, rdb, model.ContainerId, time.Duration(types.ContainerStateTtlSWhileStopping)*time.Second)
		})
	}
}

func TestManagedEndpointLeaseRequiresLiveOwnedReplica(t *testing.T) {
	for _, test := range []struct {
		name      string
		container string
		worker    string
		status    types.ReplicaStatus
		register  bool
	}{
		{"ordinary serverless", "sandbox-example", "worker-model", types.ReplicaStatusReady, true},
		{"spoofed prefix", "managed-unregistered", "worker-model", types.ReplicaStatusReady, false},
		{"different owner", "managed-owner", "different-worker", types.ReplicaStatusReady, true},
		{"draining", "managed-draining", "worker-model", types.ReplicaStatusDraining, true},
		{"terminal", "managed-terminal", "worker-model", types.ReplicaStatusStopped, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			require.NoError(t, err)
			containers := NewContainerRedisRepositoryForTest(rdb)
			seedRunningContainer(t, rdb, "worker-model", &types.ContainerState{ContainerId: test.container})
			if test.register {
				require.NoError(t, NewManagedEndpointRedisRepository(rdb).SaveReplica(context.Background(), &types.EndpointReplica{
					ID: "replica", EndpointID: "qwen/model", ContainerID: test.container, WorkerID: test.worker, Status: test.status,
				}))
			}
			require.NoError(t, containers.UpdateContainerStatus(test.container, types.ContainerStatusRunning, types.ContainerStateTtlS))
			assertContainerStateTTL(t, rdb, test.container, time.Duration(types.ContainerStateTtlS)*time.Second)
		})
	}
}

func TestManagedEndpointLeaseNeverRecreatesMissingState(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	containers := NewContainerRedisRepositoryForTest(rdb)
	require.NoError(t, NewManagedEndpointRedisRepository(rdb).SaveReplica(context.Background(), &types.EndpointReplica{
		ID: "replica", EndpointID: "qwen/model", ContainerID: "managed-expired", WorkerID: "worker", Status: types.ReplicaStatusReady,
	}))
	seedRunningContainer(t, rdb, "worker", &types.ContainerState{ContainerId: "managed-expired"})
	require.NoError(t, containers.UpdateContainerStatus("managed-expired", types.ContainerStatusRunning, types.ContainerStateTtlS))
	server.FastForward(time.Duration(types.ContainerStateTtlSManagedEndpoint)*time.Second + time.Minute)
	err = containers.UpdateContainerStatus("managed-expired", types.ContainerStatusRunning, types.ContainerStateTtlS)
	var missing *types.ErrContainerStateNotFound
	require.ErrorAs(t, err, &missing)
	require.False(t, server.Exists(common.RedisKeys.SchedulerContainerState("managed-expired")))
}

type managedLeaseLookupBarrier struct {
	key     string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *managedLeaseLookupBarrier) DialHook(next redis.DialHook) redis.DialHook { return next }
func (b *managedLeaseLookupBarrier) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (b *managedLeaseLookupBarrier) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "get" && len(cmd.Args()) == 2 && cmd.Args()[1] == b.key {
			b.once.Do(func() { close(b.reached) })
			select {
			case <-b.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return next(ctx, cmd)
	}
}

func TestManagedEndpointHeartbeatCannotOverwriteConcurrentSchedulerEviction(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)
	containers := NewContainerRedisRepositoryForTest(rdb)
	workers := NewWorkerRedisRepositoryForTest(rdb)
	worker := &types.Worker{Id: "worker-race", Status: types.WorkerStatusAvailable, Gpu: "RTX5090",
		TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 1}
	model := &types.ContainerState{ContainerId: "managed-heartbeat-race", Cpu: 1000, Memory: 1000, Gpu: "RTX5090", GpuCount: 1, Evictable: true}
	seedRunningContainer(t, rdb, worker.Id, model)
	require.NoError(t, NewManagedEndpointRedisRepository(rdb).SaveReplica(context.Background(), &types.EndpointReplica{
		ID: "replica-race", EndpointID: "qwen/model", ContainerID: model.ContainerId, WorkerID: worker.Id, Status: types.ReplicaStatusReady,
	}))
	require.NoError(t, workers.AddWorker(worker))
	barrier := &managedLeaseLookupBarrier{key: meKey("replica_container", model.ContainerId), reached: make(chan struct{}), release: make(chan struct{})}
	rdb.AddHook(barrier)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(barrier.release) }) }
	t.Cleanup(release)
	heartbeat := make(chan error, 1)
	go func() {
		heartbeat <- containers.UpdateContainerStatus(model.ContainerId, types.ContainerStatusRunning, types.ContainerStateTtlS)
	}()
	select {
	case <-barrier.reached:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat did not reach the managed replica lookup")
	}
	request := &types.ContainerRequest{ContainerId: "serverless-race", Cpu: 500, Memory: 500, Gpu: "RTX5090", GpuCount: 1}
	setPendingContainerRequests(t, rdb, request)
	require.NoError(t, workers.ScheduleContainerRequest(worker, request))
	require.Equal(t, []string{model.ContainerId}, request.EvictContainerIds)
	release()
	select {
	case err := <-heartbeat:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat failed to finish after scheduler eviction")
	}
	state, err := containers.GetContainerState(model.ContainerId)
	require.NoError(t, err)
	require.Equal(t, types.ContainerStatusStopping, state.Status)
	require.True(t, state.Evicting)
	require.Equal(t, worker.Id, state.WorkerId)
	require.Equal(t, request.ContainerId, rdb.HGet(context.Background(), common.RedisKeys.SchedulerContainerState(model.ContainerId), "evicted_for").Val())
	assertContainerStateTTL(t, rdb, model.ContainerId, time.Duration(types.ContainerStateTtlSWhileStopping)*time.Second)
}
