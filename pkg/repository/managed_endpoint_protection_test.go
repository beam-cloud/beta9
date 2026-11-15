package repository

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type protectionFixture struct {
	rdb        *common.RedisClient
	replicas   ManagedEndpointRepository
	workers    WorkerRepository
	worker     *types.Worker
	replica    *types.EndpointReplica
	stateKey   string
	workerKey  string
	replicaKey string
}

func newProtectionFixture(t *testing.T) *protectionFixture {
	t.Helper()
	ctx := context.Background()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	f := &protectionFixture{rdb: rdb, replicas: NewManagedEndpointRedisRepository(rdb), workers: NewWorkerRedisRepositoryForTest(rdb)}
	f.worker = &types.Worker{Id: "protection-worker", Status: types.WorkerStatusAvailable, Gpu: "RTX5090", TotalCpu: 4000, TotalMemory: 8000, TotalGpuCount: 2}
	f.replica = &types.EndpointReplica{ID: "protection-replica", EndpointID: "qwen/model", ContainerID: "managed-protection", WorkerID: f.worker.Id, Status: types.ReplicaStatusReady,
		Config: types.ReplicaConfig{Revision: 9007199254740993}, EngineMetrics: json.RawMessage(`{"large":18446744073709551614}`), LastHeartbeat: time.Now().UTC()}
	f.stateKey = seedRunningContainer(t, rdb, f.worker.Id, &types.ContainerState{ContainerId: f.replica.ContainerID, Cpu: 1000, Memory: 1001, Gpu: "RTX5090", GpuCount: 1, Evictable: true, StartedAt: 123, DrainSeconds: 17})
	f.workerKey = common.RedisKeys.SchedulerWorkerState(f.worker.Id)
	f.replicaKey = meKey("replica", f.replica.ID)
	require.NoError(t, f.replicas.SaveReplica(ctx, f.replica))
	require.NoError(t, f.workers.AddWorker(f.worker))
	return f
}

func (f *protectionFixture) serverless(t *testing.T) *types.ContainerRequest {
	t.Helper()
	request := &types.ContainerRequest{ContainerId: "serverless-needs-protection-gpu", Cpu: 500, Memory: 500, Gpu: "RTX5090", GpuCount: 2}
	setPendingContainerRequests(t, f.rdb, request)
	return request
}

func TestReplicaProtectionPreservesAccountingFieldsPrecisionAndTTLs(t *testing.T) {
	f := newProtectionFixture(t)
	ctx := context.Background()
	require.NoError(t, f.rdb.HSet(ctx, f.workerKey, "resource_version", "9007199254740993", "last_schedule_batch_id", "existing-batch").Err())
	require.NoError(t, f.rdb.HSet(ctx, f.stateKey, "heartbeat_field", "preserve-me").Err())
	for _, key := range []string{f.workerKey, f.stateKey, f.replicaKey} {
		require.NoError(t, f.rdb.PExpire(ctx, key, 123456*time.Millisecond).Err())
	}
	beforeWorker := f.rdb.HGetAll(ctx, f.workerKey).Val()
	beforeState := f.rdb.HGetAll(ctx, f.stateKey).Val()
	got, err := f.replicas.SetReplicaProtection(ctx, f.replica.ID, true)
	require.NoError(t, err)
	require.True(t, got.Protected)
	require.Equal(t, f.replica.Config.Revision, got.Config.Revision)
	require.JSONEq(t, string(f.replica.EngineMetrics), string(got.EngineMetrics))
	require.Equal(t, f.replica.LastHeartbeat, got.LastHeartbeat)
	storedRaw := f.rdb.Get(ctx, f.replicaKey).Val()
	require.Contains(t, storedRaw, `"revision":9007199254740993`)
	require.Contains(t, storedRaw, `"large":18446744073709551614`)
	afterState := f.rdb.HGetAll(ctx, f.stateKey).Val()
	beforeState["evictable"] = "false"
	require.Equal(t, beforeState, afterState)
	afterWorker := f.rdb.HGetAll(ctx, f.workerKey).Val()
	beforeWorker["evictable_cpu"], beforeWorker["evictable_memory"], beforeWorker["evictable_gpu_count"] = "0", "0", "0"
	beforeWorker["resource_version"] = "9007199254740994"
	require.Equal(t, beforeWorker, afterWorker, "free/held capacity and unrelated worker fields are unchanged")
	for _, key := range []string{f.workerKey, f.stateKey, f.replicaKey} {
		require.Equal(t, 123456*time.Millisecond, f.rdb.PTTL(ctx, key).Val())
	}
	// Repeating the same promotion must not reserve again or advance the version.
	_, err = f.replicas.SetReplicaProtection(ctx, f.replica.ID, true)
	require.NoError(t, err)
	require.Equal(t, afterWorker, f.rdb.HGetAll(ctx, f.workerKey).Val())
	_, err = f.replicas.SetReplicaProtection(ctx, f.replica.ID, false)
	require.NoError(t, err)
	demoted, err := f.workers.GetWorkerById(f.worker.Id)
	require.NoError(t, err)
	require.EqualValues(t, 1000, demoted.EvictableCpu)
	require.EqualValues(t, 1252, demoted.EvictableMemory, "same rounded 1.25x overhead as scheduling")
	require.EqualValues(t, 1, demoted.EvictableGpuCount)
	require.Equal(t, f.worker.FreeCpu, demoted.FreeCpu)
	require.Equal(t, f.worker.FreeMemory, demoted.FreeMemory)
	require.Equal(t, f.worker.FreeGpuCount, demoted.FreeGpuCount)
}

func TestReplicaProtectionUpdatesQueuedPendingDeliveryAndReRegistration(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "queued"
		if pending {
			name = "pending"
		}
		t.Run(name, func(t *testing.T) {
			f := newProtectionFixture(t)
			ctx := context.Background()
			require.NoError(t, f.rdb.HSet(ctx, f.stateKey, "status", "PENDING", schedulerAssignmentIDField, "assignment").Err())
			raw := `{"container_id":"managed-protection","cpu":1000,"memory":1001,"gpu":"RTX5090","gpu_count":1,"evictable":true,"future_counter":18446744073709551614}`
			queue := common.RedisKeys.SchedulerWorkerRequests(f.worker.Id)
			require.NoError(t, f.rdb.RPush(ctx, queue, raw).Err())
			var delivered []*types.ContainerRequest
			var err error
			if pending {
				delivered, err = f.workers.GetNextContainerRequests(f.worker.Id, 1)
				require.NoError(t, err)
			}
			_, err = f.replicas.SetReplicaProtection(ctx, f.replica.ID, true)
			require.NoError(t, err)
			if pending {
				require.NoError(t, f.workers.RequeueContainerRequests(f.worker.Id, delivered))
			}
			updatedRaw := f.rdb.LIndex(ctx, queue, 0).Val()
			require.Contains(t, updatedRaw, `"future_counter":18446744073709551614`)
			delivered, err = f.workers.GetNextContainerRequests(f.worker.Id, 1)
			require.NoError(t, err)
			require.Len(t, delivered, 1)
			require.False(t, delivered[0].Evictable)
			// Even an already-delivered old request is no authority for capacity.
			reconnected := *f.worker
			reconnected.FreeGpuCount, reconnected.EvictableGpuCount = 2, 2
			require.NoError(t, f.workers.AddWorker(&reconnected))
			require.EqualValues(t, 1, reconnected.FreeGpuCount)
			require.Zero(t, reconnected.EvictableGpuCount)
			_, err = f.replicas.SetReplicaProtection(ctx, f.replica.ID, false)
			require.NoError(t, err)
			require.NoError(t, f.workers.RequeueContainerRequests(f.worker.Id, delivered))
			delivered, err = f.workers.GetNextContainerRequests(f.worker.Id, 1)
			require.NoError(t, err)
			require.True(t, delivered[0].Evictable)
		})
	}
}

func TestReplicaProtectionRejectsStaleAndStoppingAssignments(t *testing.T) {
	for _, change := range []string{"missing replica", "missing state", "missing worker", "unassigned", "wrong worker", "wrong reverse index", "missing worker index", "stopping", "evicting", "terminal"} {
		t.Run(change, func(t *testing.T) {
			f := newProtectionFixture(t)
			ctx := context.Background()
			switch change {
			case "missing replica":
				f.rdb.Del(ctx, f.replicaKey)
			case "missing state":
				f.rdb.Del(ctx, f.stateKey)
			case "missing worker":
				f.rdb.Del(ctx, f.workerKey)
			case "unassigned":
				f.rdb.HSet(ctx, f.stateKey, "worker_id", "")
			case "wrong worker":
				f.rdb.HSet(ctx, f.stateKey, "worker_id", "other")
			case "wrong reverse index":
				f.rdb.Set(ctx, meKey("replica_container", f.replica.ContainerID), "other", 0)
			case "missing worker index":
				f.rdb.SRem(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(f.worker.Id), f.stateKey)
			case "stopping":
				f.rdb.HSet(ctx, f.stateKey, "status", "STOPPING")
			case "evicting":
				f.rdb.HSet(ctx, f.stateKey, "evicting", "true")
			case "terminal":
				f.replica.Status = types.ReplicaStatusStopped
				require.NoError(t, f.replicas.SaveReplica(ctx, f.replica))
			}
			beforeWorker := f.rdb.HGetAll(ctx, f.workerKey).Val()
			beforeState := f.rdb.HGetAll(ctx, f.stateKey).Val()
			_, err := f.replicas.SetReplicaProtection(ctx, f.replica.ID, true)
			require.ErrorIs(t, err, ErrReplicaProtectionChanged)
			require.Equal(t, beforeWorker, f.rdb.HGetAll(ctx, f.workerKey).Val())
			require.Equal(t, beforeState, f.rdb.HGetAll(ctx, f.stateKey).Val())
		})
	}
}

type protectionCommitBarrier struct {
	hash    string
	fired   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (h *protectionCommitBarrier) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *protectionCommitBarrier) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *protectionCommitBarrier) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "evalsha" && len(cmd.Args()) > 1 && cmd.Args()[1] == h.hash && h.fired.CompareAndSwap(false, true) {
			close(h.reached)
			select {
			case <-h.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return next(ctx, cmd)
	}
}

func waitProtectionResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("protection operation timed out")
		return nil
	}
}

func TestReplicaProtectionSerializesWithSchedulerEviction(t *testing.T) {
	for _, first := range []string{"promotion", "scheduler"} {
		t.Run(first, func(t *testing.T) {
			f := newProtectionFixture(t)
			ctx := context.Background()
			request := f.serverless(t)
			script := setReplicaProtectionScript
			if first == "scheduler" {
				script = scheduleContainerRequestsScript
			}
			require.NoError(t, script.Load(ctx, f.rdb).Err())
			barrier := &protectionCommitBarrier{hash: script.Hash(), reached: make(chan struct{}), release: make(chan struct{})}
			f.rdb.AddHook(barrier)
			promoted, scheduled := make(chan error, 1), make(chan error, 1)
			promote := func() { _, err := f.replicas.SetReplicaProtection(ctx, f.replica.ID, true); promoted <- err }
			schedule := func() { scheduled <- f.workers.ScheduleContainerRequest(f.worker, request) }
			if first == "promotion" {
				go promote()
			} else {
				go schedule()
			}
			select {
			case <-barrier.reached:
			case <-time.After(2 * time.Second):
				t.Fatal("commit barrier not reached")
			}
			if first == "promotion" {
				go schedule()
			} else {
				go promote()
			}
			close(barrier.release)
			promotionErr, scheduleErr := waitProtectionResult(t, promoted), waitProtectionResult(t, scheduled)
			state := f.rdb.HGetAll(ctx, f.stateKey).Val()
			if first == "promotion" {
				require.NoError(t, promotionErr)
				require.ErrorIs(t, scheduleErr, ErrInsufficientEvictableCapacity)
				require.Equal(t, "RUNNING", state["status"])
				require.Equal(t, "false", state["evictable"])
			} else {
				require.NoError(t, scheduleErr)
				require.ErrorIs(t, promotionErr, ErrReplicaProtectionChanged)
				require.Equal(t, "STOPPING", state["status"])
				require.Equal(t, "true", state["evicting"])
			}
		})
	}
}

func TestReplicaProtectionCASPreservesConcurrentHeartbeatAndStops(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		name := "heartbeat"
		if stopping {
			name = "stopping"
		}
		t.Run(name, func(t *testing.T) {
			f := newProtectionFixture(t)
			ctx := context.Background()
			require.NoError(t, setReplicaProtectionScript.Load(ctx, f.rdb).Err())
			barrier := &protectionCommitBarrier{hash: setReplicaProtectionScript.Hash(), reached: make(chan struct{}), release: make(chan struct{})}
			f.rdb.AddHook(barrier)
			result := make(chan error, 1)
			go func() { _, err := f.replicas.SetReplicaProtection(ctx, f.replica.ID, true); result <- err }()
			select {
			case <-barrier.reached:
			case <-time.After(2 * time.Second):
				t.Fatal("commit barrier not reached")
			}
			if stopping {
				require.NoError(t, f.rdb.HSet(ctx, f.stateKey, "status", "STOPPING", "stop_reason", "USER").Err())
			} else {
				require.NoError(t, f.rdb.HSet(ctx, f.stateKey, "started_at", "456", "heartbeat_field", "fresh").Err())
				f.replica.LastHeartbeat = f.replica.LastHeartbeat.Add(time.Minute)
				require.NoError(t, f.replicas.SaveReplica(ctx, f.replica))
			}
			close(barrier.release)
			err := waitProtectionResult(t, result)
			if stopping {
				require.ErrorIs(t, err, ErrReplicaProtectionChanged)
				require.Equal(t, "STOPPING", f.rdb.HGet(ctx, f.stateKey, "status").Val())
				require.Equal(t, "true", f.rdb.HGet(ctx, f.stateKey, "evictable").Val())
			} else {
				require.NoError(t, err)
				got, err := f.replicas.GetReplica(ctx, f.replica.ID)
				require.NoError(t, err)
				require.True(t, got.Protected)
				require.Equal(t, f.replica.LastHeartbeat, got.LastHeartbeat)
				require.Equal(t, "fresh", f.rdb.HGet(ctx, f.stateKey, "heartbeat_field").Val())
				require.Equal(t, "456", f.rdb.HGet(ctx, f.stateKey, "started_at").Val())
			}
		})
	}
}

func TestReplicaProtectionLostReplyIsIdempotent(t *testing.T) {
	f := newProtectionFixture(t)
	ctx := context.Background()
	require.NoError(t, setReplicaProtectionScript.Load(ctx, f.rdb).Err())
	f.rdb.AddHook(&lostRedisScriptReplyHook{hash: setReplicaProtectionScript.Hash()})
	_, err := f.replicas.SetReplicaProtection(ctx, f.replica.ID, true)
	require.Error(t, err)
	before := f.rdb.HGetAll(ctx, f.workerKey).Val()
	got, err := f.replicas.SetReplicaProtection(ctx, f.replica.ID, true)
	require.NoError(t, err)
	require.True(t, got.Protected)
	require.Equal(t, before, f.rdb.HGetAll(ctx, f.workerKey).Val())
}

func TestReplicaProtectionRetriesConcurrentQueueDelivery(t *testing.T) {
	f := newProtectionFixture(t)
	ctx := context.Background()
	require.NoError(t, f.rdb.HSet(ctx, f.stateKey, "status", "PENDING", schedulerAssignmentIDField, "assignment").Err())
	raw := `{"container_id":"managed-protection","cpu":1000,"memory":1001,"gpu":"RTX5090","gpu_count":1,"evictable":true}`
	require.NoError(t, f.rdb.RPush(ctx, common.RedisKeys.SchedulerWorkerRequests(f.worker.Id), raw).Err())
	require.NoError(t, setReplicaProtectionScript.Load(ctx, f.rdb).Err())
	barrier := &protectionCommitBarrier{hash: setReplicaProtectionScript.Hash(), reached: make(chan struct{}), release: make(chan struct{})}
	f.rdb.AddHook(barrier)
	result := make(chan error, 1)
	go func() { _, err := f.replicas.SetReplicaProtection(ctx, f.replica.ID, true); result <- err }()
	select {
	case <-barrier.reached:
	case <-time.After(2 * time.Second):
		t.Fatal("commit barrier not reached")
	}
	delivered, err := f.workers.GetNextContainerRequests(f.worker.Id, 1)
	require.NoError(t, err)
	require.Len(t, delivered, 1)
	close(barrier.release)
	require.NoError(t, waitProtectionResult(t, result))
	// The queue moved while the first CAS was waiting. Its retry updates the
	// pending record, so reconnect/redelivery carries the committed protection.
	require.NoError(t, f.workers.RequeueContainerRequests(f.worker.Id, delivered))
	delivered, err = f.workers.GetNextContainerRequests(f.worker.Id, 1)
	require.NoError(t, err)
	require.False(t, delivered[0].Evictable)
	stored, err := f.workers.GetWorkerById(f.worker.Id)
	require.NoError(t, err)
	require.Zero(t, stored.EvictableGpuCount)
	require.Equal(t, f.worker.FreeGpuCount, stored.FreeGpuCount)
}

func TestReplicaProtectionCASDoesNotRecreateExpiredState(t *testing.T) {
	for _, record := range []string{"replica", "container", "worker"} {
		t.Run(record, func(t *testing.T) {
			f := newProtectionFixture(t)
			ctx := context.Background()
			require.NoError(t, setReplicaProtectionScript.Load(ctx, f.rdb).Err())
			barrier := &protectionCommitBarrier{hash: setReplicaProtectionScript.Hash(), reached: make(chan struct{}), release: make(chan struct{})}
			f.rdb.AddHook(barrier)
			result := make(chan error, 1)
			go func() { _, err := f.replicas.SetReplicaProtection(ctx, f.replica.ID, true); result <- err }()
			select {
			case <-barrier.reached:
			case <-time.After(2 * time.Second):
				t.Fatal("commit barrier not reached")
			}
			key := map[string]string{"replica": f.replicaKey, "container": f.stateKey, "worker": f.workerKey}[record]
			require.NoError(t, f.rdb.Del(ctx, key).Err())
			close(barrier.release)
			require.ErrorIs(t, waitProtectionResult(t, result), ErrReplicaProtectionChanged)
			require.Zero(t, f.rdb.Exists(ctx, key).Val())
		})
	}
}
