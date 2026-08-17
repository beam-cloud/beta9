package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	redis "github.com/redis/go-redis/v9"
)

const (
	containerStateLockTTL      = 10
	containerStateLockRetries  = 20
	containerStateLockInterval = 50 * time.Millisecond
)

const containerStateVolumePlanEnqueuedField = "state_volume_plan_enqueued"

var containerStateLockOptions = common.RedisLockOptions{
	TtlS:          containerStateLockTTL,
	Retries:       containerStateLockRetries,
	RetryInterval: containerStateLockInterval,
}

const (
	concurrencyCounterInitialized      = "1"
	concurrencyCounterRepairing        = "repairing"
	concurrencyReservationOK           = "ok"
	concurrencyReservationRepairing    = "repairing"
	concurrencyReservationGPUExceeded  = "gpu"
	concurrencyReservationCPUExceeded  = "cpu"
	concurrencyCounterInitTimeout      = 15 * time.Second
	concurrencyCounterInitPollInterval = 100 * time.Millisecond
	concurrencyCounterRepairInterval   = 5 * time.Second
	concurrencyReservationInFlightTTL  = 2 * time.Minute
	workerAddressWaitTimeout           = 5 * time.Minute
	workerAddressPollInterval          = 25 * time.Millisecond
)

var errConcurrencyCounterRepairing = errors.New("concurrency counter repair in progress")

// Opening worker routes are republished for every container startup. Once the
// agent has made an identical shared route ready, those registrations must not
// demote it or erase its proxy target. WorkspaceID is deliberately not part of
// the target identity because marketplace workers can serve buyer workspaces.
var setOpeningWorkerBackendRouteScript = redis.NewScript(`
local incoming = cjson.decode(ARGV[1])
local current = redis.call("GET", KEYS[1])

if current ~= false then
	local decoded, existing = pcall(cjson.decode, current)
	if decoded and
		existing.state == ARGV[2] and
		existing.proxy_target and
		existing.proxy_target ~= "" and
		existing.route_id == incoming.route_id and
		existing.pool_name == incoming.pool_name and
		existing.machine_id == incoming.machine_id and
		existing.worker_id == incoming.worker_id and
		existing.container_id == incoming.container_id and
		existing.kind == incoming.kind and
		existing.port == incoming.port and
		existing.protocol == incoming.protocol and
		existing.transport == incoming.transport and
		existing.local_target == incoming.local_target then
		return 0
	end
end

redis.call("SET", KEYS[1], ARGV[1])
return 1
`)

// Workspace concurrency accounting must stay O(1) during bursts. The old
// implementation scanned every active container while holding a workspace lock
// for each request. Instead, the first quota-bearing request rebuilds an
// aggregate counter from active container state, and each subsequent request
// atomically creates one reservation record while incrementing the aggregate.
var reserveConcurrencyReservationScript = redis.NewScript(`
local used_gpu = tonumber(redis.call("HGET", KEYS[1], "gpu_count") or "0")
local used_cpu = tonumber(redis.call("HGET", KEYS[1], "cpu") or "0")
local gpu_limit = tonumber(ARGV[1])
local cpu_limit = tonumber(ARGV[2])
local request_gpu = tonumber(ARGV[3])
local request_cpu = tonumber(ARGV[4])

if redis.call("EXISTS", KEYS[2]) == 1 then
	redis.call("SADD", KEYS[3], ARGV[6])
	return "ok"
end

if redis.call("HGET", KEYS[1], "initialized") ~= "1" then
	return "repairing"
end

if used_gpu + request_gpu > gpu_limit then
	return "gpu"
end

if used_cpu + request_cpu > cpu_limit then
	return "cpu"
end

redis.call("HINCRBY", KEYS[1], "gpu_count", request_gpu)
redis.call("HINCRBY", KEYS[1], "cpu", request_cpu)
redis.call("HSET", KEYS[1], "initialized", "1", "updated_at", ARGV[7])
redis.call("HSET", KEYS[2],
	"workspace_id", ARGV[5],
	"container_id", ARGV[6],
	"gpu_count", request_gpu,
	"cpu", request_cpu,
	"created_at", ARGV[7])
redis.call("SADD", KEYS[3], ARGV[6])
return "ok"
`)

var releaseConcurrencyReservationScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 0 then
	redis.call("SREM", KEYS[3], ARGV[2])
	return 0
end

if redis.call("HGET", KEYS[1], "initialized") ~= "1" then
	return "repairing"
end

local reserved_gpu = tonumber(redis.call("HGET", KEYS[2], "gpu_count") or "0")
local reserved_cpu = tonumber(redis.call("HGET", KEYS[2], "cpu") or "0")

local used_gpu = tonumber(redis.call("HGET", KEYS[1], "gpu_count") or "0")
local used_cpu = tonumber(redis.call("HGET", KEYS[1], "cpu") or "0")

if reserved_gpu ~= 0 then
	used_gpu = redis.call("HINCRBY", KEYS[1], "gpu_count", -reserved_gpu)
end
if reserved_cpu ~= 0 then
	used_cpu = redis.call("HINCRBY", KEYS[1], "cpu", -reserved_cpu)
end

if used_gpu < 0 then
	redis.call("HSET", KEYS[1], "gpu_count", 0)
end
if used_cpu < 0 then
	redis.call("HSET", KEYS[1], "cpu", 0)
end

redis.call("HSET", KEYS[1], "updated_at", ARGV[1])
redis.call("DEL", KEYS[2])
redis.call("SREM", KEYS[3], ARGV[2])
return 1
`)

var releaseUnadmittedConcurrencyReservationScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[4]) == 1 then
	local status = redis.call("HGET", KEYS[4], "status")
	if status == ARGV[3] or status == ARGV[4] or status == ARGV[5] then return 0 end
end
if redis.call("EXISTS", KEYS[2]) == 0 then
	redis.call("SREM", KEYS[3], ARGV[2])
	return 0
end
if redis.call("HGET", KEYS[1], "initialized") ~= "1" then return "repairing" end
local reserved_gpu = tonumber(redis.call("HGET", KEYS[2], "gpu_count") or "0")
local reserved_cpu = tonumber(redis.call("HGET", KEYS[2], "cpu") or "0")
local used_gpu = tonumber(redis.call("HGET", KEYS[1], "gpu_count") or "0")
local used_cpu = tonumber(redis.call("HGET", KEYS[1], "cpu") or "0")
if reserved_gpu ~= 0 then used_gpu = redis.call("HINCRBY", KEYS[1], "gpu_count", -reserved_gpu) end
if reserved_cpu ~= 0 then used_cpu = redis.call("HINCRBY", KEYS[1], "cpu", -reserved_cpu) end
if used_gpu < 0 then redis.call("HSET", KEYS[1], "gpu_count", 0) end
if used_cpu < 0 then redis.call("HSET", KEYS[1], "cpu", 0) end
redis.call("HSET", KEYS[1], "updated_at", ARGV[1])
redis.call("DEL", KEYS[2])
redis.call("SREM", KEYS[3], ARGV[2])
return 1
`)

type ContainerRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

type concurrencyReservation struct {
	WorkspaceId string `redis:"workspace_id"`
	ContainerId string `redis:"container_id"`
	GpuCount    int64  `redis:"gpu_count"`
	Cpu         int64  `redis:"cpu"`
	CreatedAt   int64  `redis:"created_at"`
}

func NewContainerRedisRepository(r *common.RedisClient) ContainerRepository {
	lock := common.NewRedisLock(r)
	return &ContainerRedisRepository{rdb: r, lock: lock}
}

func (cr *ContainerRedisRepository) GetContainerState(containerId string) (*types.ContainerState, error) {
	stateKey := common.RedisKeys.SchedulerContainerState(containerId)

	res, err := cr.rdb.HGetAll(context.TODO(), stateKey).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get container state: %w", err)
	}

	if len(res) == 0 {
		return nil, &types.ErrContainerStateNotFound{ContainerId: containerId}
	}

	state := &types.ContainerState{}
	if err = common.ToStruct(res, state); err != nil {
		return nil, fmt.Errorf("failed to deserialize container state <%v>: %v", stateKey, err)
	}

	return state, nil
}

func (cr *ContainerRedisRepository) SetStateRestoreReceipt(containerId, workerInstanceId string, receipt *types.StateRestoreReceipt, expectedAssignment *types.ContainerState) error {
	if receipt == nil || receipt.StateSnapshotId == "" || expectedAssignment == nil || expectedAssignment.WorkerId == "" ||
		expectedAssignment.MachineId == "" || workerInstanceId == "" || expectedAssignment.StateSnapshotId == "" || expectedAssignment.AssignmentId == "" {
		return fmt.Errorf("state restore receipt, snapshot id, and exact worker process assignment are required")
	}
	canonical := *receipt
	canonical.Generations = append([]types.StateGeneration(nil), receipt.Generations...)
	sort.Slice(canonical.Generations, func(i, j int) bool {
		if canonical.Generations[i].Root != canonical.Generations[j].Root {
			return canonical.Generations[i].Root
		}
		if canonical.Generations[i].VolumeId != canonical.Generations[j].VolumeId {
			return canonical.Generations[i].VolumeId < canonical.Generations[j].VolumeId
		}
		return canonical.Generations[i].GenerationId < canonical.Generations[j].GenerationId
	})
	payload, err := json.Marshal(&canonical)
	if err != nil {
		return fmt.Errorf("serialize state restore receipt: %w", err)
	}
	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	workerKey := common.RedisKeys.SchedulerWorkerState(expectedAssignment.WorkerId)
	result, err := setStateRestoreReceiptScript.Run(context.TODO(), cr.rdb, []string{stateKey, workerKey}, payload,
		expectedAssignment.WorkerId, expectedAssignment.MachineId, workerInstanceId, expectedAssignment.StateSnapshotId,
		expectedAssignment.StateVolumePlanId, expectedAssignment.StateVolumePlanHash, expectedAssignment.AssignmentId).Int()
	if err != nil {
		return err
	}
	switch result {
	case 0, 1:
		return nil
	case -1:
		return &types.ErrContainerStateNotFound{ContainerId: containerId}
	case -2:
		return fmt.Errorf("state restore receipt is immutable and conflicts with the stored worker outcome")
	case -3:
		return fmt.Errorf("state restore receipt assignment changed before its outcome could be persisted")
	case -4:
		return fmt.Errorf("state restore receipt worker process was superseded before its outcome could be persisted")
	default:
		return fmt.Errorf("unexpected state restore receipt CAS result %d", result)
	}
}

var setStateRestoreReceiptScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) ~= 1 then return -1 end
if redis.call("EXISTS", KEYS[2]) ~= 1 or
   (redis.call("HGET", KEYS[2], "instance_id") or "") ~= ARGV[4] or
   (redis.call("HGET", KEYS[2], "machine_id") or "") ~= ARGV[3] then return -4 end
if (redis.call("HGET", KEYS[1], "worker_id") or "") ~= ARGV[2] or
   (redis.call("HGET", KEYS[1], "machine_id") or "") ~= ARGV[3] or
   (redis.call("HGET", KEYS[1], "state_snapshot_id") or "") ~= ARGV[5] or
   (redis.call("HGET", KEYS[1], "state_volume_plan_id") or "") ~= ARGV[6] or
   (redis.call("HGET", KEYS[1], "state_volume_plan_hash") or "") ~= ARGV[7] or
   (redis.call("HGET", KEYS[1], "schedule_delivery_token") or "") ~= ARGV[8] then return -3 end
local stored = redis.call("HGET", KEYS[1], "state_restore_receipt")
local stored_assignment = redis.call("HGET", KEYS[1], "state_restore_receipt_assignment")
local stored_instance = redis.call("HGET", KEYS[1], "state_restore_receipt_worker_instance")
if stored and stored_assignment == ARGV[8] and stored_instance == ARGV[4] then
  if stored == ARGV[1] then return 0 end
  return -2
end
redis.call("HSET", KEYS[1], "state_restore_receipt", ARGV[1], "state_restore_receipt_assignment", ARGV[8],
  "state_restore_receipt_worker_instance", ARGV[4], "state_restore_receipt_storage_node", ARGV[3])
return 1
`)

func (cr *ContainerRedisRepository) GetStateRestoreReceipt(containerId string) (*types.StateRestoreReceipt, error) {
	state, err := cr.GetContainerState(containerId)
	if err != nil {
		return nil, err
	}
	value, err := getStateRestoreReceiptScript.Run(context.TODO(), cr.rdb,
		[]string{common.RedisKeys.SchedulerContainerState(containerId), common.RedisKeys.SchedulerWorkerState(state.WorkerId)},
		state.WorkerId).Result()
	if err != nil {
		return nil, err
	}
	payload, ok := value.(string)
	if !ok || payload == "" {
		return nil, redis.Nil
	}
	var receipt types.StateRestoreReceipt
	if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
		return nil, fmt.Errorf("deserialize state restore receipt: %w", err)
	}
	return &receipt, nil
}

var getStateRestoreReceiptScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) ~= 1 then return false end
if redis.call("EXISTS", KEYS[2]) ~= 1 or
   (redis.call("HGET", KEYS[1], "worker_id") or "") ~= ARGV[1] then return false end
local receipt = redis.call("HGET", KEYS[1], "state_restore_receipt")
if not receipt then return false end
if (redis.call("HGET", KEYS[1], "state_restore_receipt_assignment") or "") ~=
   (redis.call("HGET", KEYS[1], "schedule_delivery_token") or "") then return false end
if (redis.call("HGET", KEYS[1], "state_restore_receipt_worker_instance") or "") ~=
   (redis.call("HGET", KEYS[2], "instance_id") or "") or
   (redis.call("HGET", KEYS[1], "state_restore_receipt_storage_node") or "") ~=
   (redis.call("HGET", KEYS[2], "machine_id") or "") then return false end
local ok, decoded = pcall(cjson.decode, receipt)
if not ok or not decoded or (decoded.state_snapshot_id or "") ~=
   (redis.call("HGET", KEYS[1], "state_snapshot_id") or "") then return false end
return receipt
`)

func (cr *ContainerRedisRepository) GetContainerStatuses(containerIds []string) (map[string]types.ContainerStatus, error) {
	statuses := make(map[string]types.ContainerStatus, len(containerIds))
	ctx := context.TODO()
	pipe := cr.rdb.Pipeline()
	commands := make(map[string]*redis.StringCmd, len(containerIds))
	for _, containerId := range containerIds {
		if containerId == "" || commands[containerId] != nil {
			continue
		}
		commands[containerId] = pipe.HGet(ctx, common.RedisKeys.SchedulerContainerState(containerId), "status")
	}
	if len(commands) == 0 {
		return statuses, nil
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	for containerId, command := range commands {
		if command.Err() == nil {
			statuses[containerId] = types.ContainerStatus(command.Val())
		}
	}
	return statuses, nil
}

func (cr *ContainerRedisRepository) SetContainerState(containerId string, state *types.ContainerState) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), containerStateLockOptions)
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	return cr.setContainerState(containerId, state)
}

func (cr *ContainerRedisRepository) setContainerState(containerId string, state *types.ContainerState) error {
	ctx := context.TODO()
	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	stubIndexKey := common.RedisKeys.SchedulerContainerIndex(state.StubId)
	workspaceIndexKey := common.RedisKeys.SchedulerContainerWorkspaceIndex(state.WorkspaceId)

	// Commit state and indexes together so stop/list callers cannot miss a
	// newly-created container between the hash write and index writes.
	pipe := cr.rdb.TxPipeline()
	pipe.HSet(
		ctx, stateKey,
		"container_id", containerId,
		"status", string(state.Status),
		"scheduled_at", state.ScheduledAt,
		"stub_id", state.StubId,
		"workspace_id", state.WorkspaceId,
		"gpu", state.Gpu,
		"gpu_count", state.GpuCount,
		"nbd_devices", state.NbdDevices,
		"cpu", state.Cpu,
		"memory", state.Memory,
		"worker_id", state.WorkerId,
		"machine_id", state.MachineId,
		"state_snapshot_id", state.StateSnapshotId,
		"state_fork", state.StateFork,
		"schedule_delivery_token", state.AssignmentId,
		"state_volume_plan_id", state.StateVolumePlanId,
		"state_volume_plan_hash", state.StateVolumePlanHash,
	)
	pipe.Expire(ctx, stateKey, time.Duration(types.ContainerStateTtlSWhilePending)*time.Second)
	pipe.SAdd(ctx, stubIndexKey, stateKey)
	pipe.SAdd(ctx, workspaceIndexKey, stateKey)
	pipe.ZAdd(ctx, common.RedisKeys.SchedulerContainerStateIndex(), redis.Z{
		Score:  float64(time.Now().Add(time.Duration(types.ContainerStateTtlSWhilePending) * time.Second).Unix()),
		Member: stateKey,
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to set container state and indexes <%v>: %w", stateKey, err)
	}

	return nil
}

var setContainerStateWithStateVolumeOutboxScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[7]) == 1 then return -7 end
local state_exists = redis.call("EXISTS", KEYS[1])
if state_exists == 1 then
  local stored_plan = redis.call("HGET", KEYS[1], "state_volume_plan_id")
  local stored_hash = redis.call("HGET", KEYS[1], "state_volume_plan_hash")
  if stored_plan ~= ARGV[13] or stored_hash ~= ARGV[14] then return -2 end
  local status = redis.call("HGET", KEYS[1], "status")
  local worker_id = redis.call("HGET", KEYS[1], "worker_id") or ""
  if status ~= ARGV[2] or worker_id ~= "" then return -4 end
  if ARGV[20] == "1" then
    if redis.call("EXISTS", KEYS[6]) ~= 1 or
       redis.call("HGET", KEYS[6], "workspace_id") ~= ARGV[5] or
       redis.call("HGET", KEYS[6], "container_id") ~= ARGV[1] or
       tonumber(redis.call("HGET", KEYS[6], "gpu_count") or "-1") ~= tonumber(ARGV[23]) or
       tonumber(redis.call("HGET", KEYS[6], "cpu") or "-1") ~= tonumber(ARGV[24]) then return -6 end
  end
  local enqueued = redis.call("HGET", KEYS[1], ARGV[19])
  if enqueued then
    if enqueued == ARGV[13] then return 0 end
    return -2
  end
  if redis.call("EXISTS", KEYS[5]) ~= 1 then return -1 end
  if redis.call("HGET", KEYS[5], "plan_id") ~= ARGV[13] or
     redis.call("HGET", KEYS[5], "request_hash") ~= ARGV[14] or
     redis.call("HGET", KEYS[5], "container_id") ~= ARGV[1] or
     redis.call("HGET", KEYS[5], "payload") ~= ARGV[17] or
     redis.call("HGET", KEYS[5], "ready_at") ~= ARGV[18] then return -2 end
  return 0
end
if redis.call("EXISTS", KEYS[5]) == 1 then return -3 end
if ARGV[20] == "1" then
  if redis.call("HGET", KEYS[8], "initialized") ~= "1" then return -8 end
  if redis.call("EXISTS", KEYS[6]) == 1 then
    if redis.call("HGET", KEYS[6], "workspace_id") ~= ARGV[5] or
       redis.call("HGET", KEYS[6], "container_id") ~= ARGV[1] or
       tonumber(redis.call("HGET", KEYS[6], "gpu_count") or "-1") ~= tonumber(ARGV[23]) or
       tonumber(redis.call("HGET", KEYS[6], "cpu") or "-1") ~= tonumber(ARGV[24]) then return -6 end
  else
    local used_gpu = tonumber(redis.call("HGET", KEYS[8], "gpu_count") or "0")
    local used_cpu = tonumber(redis.call("HGET", KEYS[8], "cpu") or "0")
    if used_gpu + tonumber(ARGV[23]) > tonumber(ARGV[21]) then return -9 end
    if used_cpu + tonumber(ARGV[24]) > tonumber(ARGV[22]) then return -10 end
    redis.call("HINCRBY", KEYS[8], "gpu_count", ARGV[23])
    redis.call("HINCRBY", KEYS[8], "cpu", ARGV[24])
    redis.call("HSET", KEYS[8], "updated_at", ARGV[25])
    redis.call("HSET", KEYS[6], "workspace_id", ARGV[5], "container_id", ARGV[1],
      "gpu_count", ARGV[23], "cpu", ARGV[24], "created_at", ARGV[25])
  end
  redis.call("SADD", KEYS[9], ARGV[1])
end
redis.call("HSET", KEYS[1],
  "container_id", ARGV[1], "status", ARGV[2], "scheduled_at", ARGV[3],
  "stub_id", ARGV[4], "workspace_id", ARGV[5], "gpu", ARGV[6],
  "gpu_count", ARGV[7], "nbd_devices", ARGV[8], "cpu", ARGV[9],
  "memory", ARGV[10], "worker_id", ARGV[11], "machine_id", ARGV[12],
  "state_volume_plan_id", ARGV[13], "state_volume_plan_hash", ARGV[14],
  "state_snapshot_id", ARGV[26], "state_fork", ARGV[27])
redis.call("EXPIRE", KEYS[1], ARGV[15])
redis.call("SADD", KEYS[2], KEYS[1])
redis.call("SADD", KEYS[3], KEYS[1])
redis.call("ZADD", KEYS[4], ARGV[16], KEYS[1])
redis.call("HSET", KEYS[5], "plan_id", ARGV[13], "container_id", ARGV[1],
  "request_hash", ARGV[14], "payload", ARGV[17], "ready_at", ARGV[18])
redis.call("EXPIRE", KEYS[5], ARGV[15])
return 1
`)

func concurrencyGPULimit(quota *types.ConcurrencyLimit) int64 {
	if quota == nil {
		return 0
	}
	return int64(quota.GPULimit)
}

func concurrencyCPULimit(quota *types.ConcurrencyLimit) int64 {
	if quota == nil {
		return 0
	}
	return int64(quota.CPUMillicoreLimit)
}

func (cr *ContainerRedisRepository) setContainerStateWithStateVolumeOutbox(state *types.ContainerState, payload []byte, readyAt time.Time, quota *types.ConcurrencyLimit) error {
	if state == nil || state.ContainerId == "" || state.StateVolumePlanId == "" || state.StateVolumePlanHash == "" || len(payload) == 0 || readyAt.IsZero() {
		return fmt.Errorf("state-volume container admission is incomplete")
	}
	stateKey := common.RedisKeys.SchedulerContainerState(state.ContainerId)
	ttlSeconds := types.ContainerStateTtlSWhilePending
	expiresAt := time.Now().Add(time.Duration(ttlSeconds) * time.Second).Unix()
	quotaRequired := "0"
	if quota != nil {
		quotaRequired = "1"
	}
	result, err := setContainerStateWithStateVolumeOutboxScript.Run(context.TODO(), cr.rdb, []string{
		stateKey,
		common.RedisKeys.SchedulerContainerIndex(state.StubId),
		common.RedisKeys.SchedulerContainerWorkspaceIndex(state.WorkspaceId),
		common.RedisKeys.SchedulerContainerStateIndex(),
		common.RedisKeys.SchedulerStateVolumePlanOutbox(state.StateVolumePlanId),
		common.RedisKeys.WorkspaceConcurrencyLimitReservation(state.WorkspaceId, state.ContainerId),
		common.RedisKeys.SchedulerStateVolumePlanTombstone(state.StateVolumePlanId),
		common.RedisKeys.WorkspaceConcurrencyLimitUsage(state.WorkspaceId),
		common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(state.WorkspaceId),
	}, state.ContainerId, string(state.Status), state.ScheduledAt, state.StubId, state.WorkspaceId,
		state.Gpu, state.GpuCount, state.NbdDevices, state.Cpu, state.Memory, state.WorkerId, state.MachineId,
		state.StateVolumePlanId, state.StateVolumePlanHash, ttlSeconds, expiresAt, payload, readyAt.UnixNano(),
		containerStateVolumePlanEnqueuedField, quotaRequired, concurrencyGPULimit(quota), concurrencyCPULimit(quota),
		state.GpuCount, state.Cpu, time.Now().Unix(), state.StateSnapshotId, state.StateFork).Int()
	if err != nil {
		return fmt.Errorf("atomically admit state-volume container and outbox: %w", err)
	}
	switch result {
	case 0, 1:
		return nil
	case -4:
		return &types.ContainerAlreadyScheduledError{Msg: "state-volume container is already assigned or no longer pending"}
	case -2:
		return &types.ContainerAlreadyScheduledError{Msg: "container belongs to a different state-volume admission"}
	case -1:
		return fmt.Errorf("state-volume admission lost its exact outbox before promotion")
	case -3:
		return fmt.Errorf("state-volume admission found an outbox without its exact container state")
	case -7:
		return fmt.Errorf("state-volume attachment plan was durably aborted")
	case -6:
		return fmt.Errorf("state-volume concurrency reservation conflicts with the exact admission")
	case -8:
		return errConcurrencyCounterRepairing
	case -9:
		return &types.ThrottledByConcurrencyLimitError{Reason: "gpu quota exceeded"}
	case -10:
		return &types.ThrottledByConcurrencyLimitError{Reason: "cpu quota exceeded"}
	default:
		return fmt.Errorf("atomically admit state-volume container and outbox: unexpected result %d", result)
	}
}

func (cr *ContainerRedisRepository) SetContainerExitCode(containerId string, exitCode int) error {
	exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)
	ttl := types.ContainerExitCodeTTL
	if types.ContainerExitCode(exitCode).IsFailed() {
		ttl = types.ContainerFailureHistoryTTL
	}
	err := cr.rdb.SetEx(context.TODO(), exitCodeKey, exitCode, ttl).Err()
	if err != nil {
		return fmt.Errorf("failed to set exit code <%v> for container <%v>: %w", exitCodeKey, containerId, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) SetContainerFailureCooldown(containerIds []string) error {
	const clampTTL = `
if redis.call("PTTL", KEYS[1]) > tonumber(ARGV[1]) then
	return redis.call("PEXPIRE", KEYS[1], ARGV[1])
end
return 0`

	ctx := context.TODO()
	pipe := cr.rdb.Pipeline()
	for _, containerId := range containerIds {
		key := common.RedisKeys.SchedulerContainerExitCode(containerId)
		pipe.Eval(ctx, clampTTL, []string{key}, types.ContainerFailureCooldown.Milliseconds())
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to set container failure cooldown: %w", err)
	}

	return nil
}

func (cr *ContainerRedisRepository) GetContainerExitCode(containerId string) (int, error) {
	exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)
	exitCode, err := cr.rdb.Get(context.TODO(), exitCodeKey).Int()
	if err != nil {
		return -1, err
	}

	return exitCode, nil
}

func (cr *ContainerRedisRepository) UpdateContainerStatus(containerId string, requestedStatus types.ContainerStatus, expirySeconds int64) error {
	expiry := time.Duration(expirySeconds) * time.Second

	switch requestedStatus {
	case types.ContainerStatusPending, types.ContainerStatusRunning, types.ContainerStatusStopping:
		// continue
	default:
		return fmt.Errorf("invalid status: %s", requestedStatus)
	}

	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), containerStateLockOptions)
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	// Get current state
	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	res, err := cr.rdb.HGetAll(context.TODO(), stateKey).Result()
	if err != nil {
		return err
	}
	if len(res) == 0 {
		return &types.ErrContainerStateNotFound{ContainerId: containerId}
	}

	// Convert response to struct
	state := &types.ContainerState{}
	err = common.ToStruct(res, state)
	if err != nil {
		return fmt.Errorf("failed to deserialize container state: %v", err)
	}

	storedStatus := types.ContainerStatus(state.Status)
	if !containerStatusTransitionAllowed(storedStatus, requestedStatus) {
		// A delayed heartbeat must never move lifecycle state backward. In
		// particular, STOPPING is terminal until the state is deleted.
		return nil
	}

	// Update StartedAt if this is the first time we set container status to RUNNING
	if requestedStatus == types.ContainerStatusRunning && storedStatus != types.ContainerStatusRunning {
		state.StartedAt = time.Now().Unix()
	}

	// Update status
	state.Status = requestedStatus

	// Save state to database
	pipe := cr.rdb.TxPipeline()
	pipe.HSet(context.TODO(), stateKey, common.ToSlice(state))
	pipe.Expire(context.TODO(), stateKey, expiry)
	pipe.ZAdd(context.TODO(), common.RedisKeys.SchedulerContainerStateIndex(), redis.Z{
		Score:  float64(time.Now().Add(expiry).Unix()),
		Member: stateKey,
	})
	if _, err = pipe.Exec(context.TODO()); err != nil {
		return fmt.Errorf("failed to set container state ttl <%v>: %w", stateKey, err)
	}

	if requestedStatus == types.ContainerStatusStopping {
		// The release script is idempotent. Run it on every STOPPING update so
		// callers can retry if a previous release failed after status persisted.
		if err := cr.releaseContainerConcurrencyReservation(context.TODO(), state.WorkspaceId, containerId); err != nil {
			return err
		}
	}

	return nil
}

func containerStatusTransitionAllowed(storedStatus, requestedStatus types.ContainerStatus) bool {
	switch storedStatus {
	case types.ContainerStatusPending:
		return requestedStatus == types.ContainerStatusPending ||
			requestedStatus == types.ContainerStatusRunning ||
			requestedStatus == types.ContainerStatusStopping
	case types.ContainerStatusRunning:
		return requestedStatus == types.ContainerStatusRunning ||
			requestedStatus == types.ContainerStatusStopping
	case types.ContainerStatusStopping:
		return requestedStatus == types.ContainerStatusStopping
	default:
		return false
	}
}

var markPendingContainerStoppingIfUnassignedScript = redis.NewScript(`
if redis.call("HGET", KEYS[1], "status") ~= ARGV[1] then
	return 0
end
local worker_id = redis.call("HGET", KEYS[1], "worker_id")
if worker_id and worker_id ~= "" then
	return 0
end
redis.call("HSET", KEYS[1], "status", ARGV[2])
redis.call("EXPIRE", KEYS[1], ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[4], KEYS[1])
return 1
`)

const stateVolumePlanAbortingField = "state_volume_plan_aborting"

var fencePendingContainerStateVolumePlanScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[4]) == 1 then
  if redis.call("HGET", KEYS[4], "container_id") == ARGV[9] and
     redis.call("HGET", KEYS[4], "request_hash") == ARGV[6] then return 1 end
  return -2
end
if redis.call("EXISTS", KEYS[1]) ~= 1 then
  redis.call("HSET", KEYS[4], "container_id", ARGV[9], "request_hash", ARGV[6])
  return 1
end
if redis.call("HGET", KEYS[1], "state_volume_plan_id") ~= ARGV[5] or
   redis.call("HGET", KEYS[1], "state_volume_plan_hash") ~= ARGV[6] then return -2 end
local worker_id = redis.call("HGET", KEYS[1], "worker_id") or ""
if worker_id ~= "" then return -3 end
if redis.call("HGET", KEYS[1], ARGV[8]) then return -3 end
local status = redis.call("HGET", KEYS[1], "status")
if status ~= ARGV[1] then
  if status ~= ARGV[2] or redis.call("HGET", KEYS[1], ARGV[7]) ~= ARGV[5] then return -3 end
end
if redis.call("EXISTS", KEYS[3]) == 1 then
  if redis.call("HGET", KEYS[3], "plan_id") ~= ARGV[5] or
     redis.call("HGET", KEYS[3], "container_id") ~= ARGV[9] or
     redis.call("HGET", KEYS[3], "request_hash") ~= ARGV[6] then return -2 end
  redis.call("DEL", KEYS[3])
end
redis.call("HSET", KEYS[1], "status", ARGV[2], ARGV[7], ARGV[5])
redis.call("HSET", KEYS[4], "container_id", ARGV[9], "request_hash", ARGV[6])
redis.call("EXPIRE", KEYS[1], ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[4], KEYS[1])
return 1
`)

func (cr *ContainerRedisRepository) MarkPendingContainerStoppingIfUnassigned(containerId string, expirySeconds int64) (bool, error) {
	lockKey := common.RedisKeys.SchedulerContainerLock(containerId)
	if err := cr.lock.Acquire(context.TODO(), lockKey, containerStateLockOptions); err != nil {
		return false, err
	}
	defer cr.lock.Release(lockKey)

	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	marked, err := markPendingContainerStoppingIfUnassignedScript.Run(context.TODO(), cr.rdb, []string{
		stateKey,
		common.RedisKeys.SchedulerContainerStateIndex(),
	},
		string(types.ContainerStatusPending), string(types.ContainerStatusStopping), expirySeconds,
		time.Now().Add(time.Duration(expirySeconds)*time.Second).Unix(),
	).Bool()
	if err != nil {
		return false, fmt.Errorf("failed to stop unassigned pending container <%s>: %w", containerId, err)
	}
	return marked, nil
}

// FencePendingContainerStateVolumePlan installs an exact Redis tombstone
// before PostgreSQL aborts an unadmitted attachment plan. A concurrent exact
// retry can no longer recreate or dispatch the old request between stores.
func (cr *ContainerRedisRepository) FencePendingContainerStateVolumePlan(containerId, planId, requestHash string, expirySeconds int64) (bool, error) {
	if containerId == "" || planId == "" || requestHash == "" || expirySeconds <= 0 {
		return false, fmt.Errorf("container, state-volume plan identity, and positive expiry are required")
	}
	result, err := fencePendingContainerStateVolumePlanScript.Run(context.TODO(), cr.rdb, []string{
		common.RedisKeys.SchedulerContainerState(containerId),
		common.RedisKeys.SchedulerContainerStateIndex(),
		common.RedisKeys.SchedulerStateVolumePlanOutbox(planId),
		common.RedisKeys.SchedulerStateVolumePlanTombstone(planId),
	}, string(types.ContainerStatusPending), string(types.ContainerStatusStopping), expirySeconds,
		time.Now().Add(time.Duration(expirySeconds)*time.Second).Unix(), planId, requestHash,
		stateVolumePlanAbortingField, containerStateVolumePlanEnqueuedField, containerId).Int()
	if err != nil {
		return false, fmt.Errorf("fence pending state-volume container <%s>: %w", containerId, err)
	}
	switch result {
	case 0:
		return false, nil
	case 1:
		return true, nil
	case -2:
		return false, fmt.Errorf("pending container belongs to a different state-volume attachment plan")
	default:
		return false, fmt.Errorf("pending state-volume container is assigned, enqueued, or no longer abortable")
	}
}

func (cr *ContainerRedisRepository) DeleteContainerState(containerId string) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	ctx := context.TODO()
	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	state, err := cr.rdb.HMGet(ctx, stateKey, "workspace_id", "stub_id", "worker_id").Result()
	if err != nil {
		return fmt.Errorf("failed to get container indexes <%v>: %w", stateKey, err)
	}
	indexedValue := func(index int) string {
		if index >= len(state) || state[index] == nil {
			return ""
		}
		value, _ := state[index].(string)
		return value
	}
	workspaceId := indexedValue(0)
	stubId := indexedValue(1)
	workerId := indexedValue(2)

	if workspaceId != "" {
		if err := cr.rdb.HSet(ctx, stateKey, "status", string(types.ContainerStatusStopping)).Err(); err != nil {
			return fmt.Errorf("failed to mark container stopping before delete <%v>: %w", stateKey, err)
		}
		if err := cr.releaseContainerConcurrencyReservation(ctx, workspaceId, containerId); err != nil {
			return err
		}
	}

	// Failed exit codes intentionally keep their stub-index membership for the
	// autoscaler's bounded failure-history window. Successful and administrative
	// stops have no history consumer, so retaining their missing state keys would
	// leak a permanent stub index after a deployment is deleted.
	retainFailureHistory := false
	if exitCode, exitErr := cr.rdb.Get(ctx, common.RedisKeys.SchedulerContainerExitCode(containerId)).Int(); exitErr == nil {
		retainFailureHistory = types.ContainerExitCode(exitCode).IsFailed()
	} else if exitErr != redis.Nil {
		// A Redis read failure must not make failure history disappear. The normal
		// index reader can prune this member once the exit-code lease is gone.
		retainFailureHistory = true
	}

	addrKey := common.RedisKeys.SchedulerContainerAddress(containerId)
	addrMapKey := common.RedisKeys.SchedulerContainerAddressMap(containerId)
	workerAddrKey := common.RedisKeys.SchedulerWorkerAddress(containerId)
	pipe := cr.rdb.TxPipeline()
	pipe.Del(ctx, stateKey, addrKey, addrMapKey, workerAddrKey)
	pipe.ZRem(ctx, common.RedisKeys.SchedulerContainerStateIndex(), stateKey)
	if workspaceId != "" {
		pipe.SRem(ctx, common.RedisKeys.SchedulerContainerWorkspaceIndex(workspaceId), stateKey)
	}
	if workerId != "" {
		pipe.SRem(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(workerId), stateKey)
	}
	if stubId != "" && !retainFailureHistory {
		pipe.SRem(ctx, common.RedisKeys.SchedulerContainerIndex(stubId), stateKey)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to delete container state <%v>: %w", stateKey, err)
	}

	if err := cr.DeleteBackendRoutesByContainerID(context.TODO(), containerId); err != nil {
		return err
	}

	return nil
}

func (cr *ContainerRedisRepository) SetContainerAddress(containerId string, addr string) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerAddress(containerId), addr, 0).Err()
}

func (cr *ContainerRedisRepository) GetContainerAddress(containerId string) (string, error) {
	return cr.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerAddress(containerId)).Result()
}

func (cr *ContainerRedisRepository) SetBackendRoute(ctx context.Context, route types.BackendRoute) error {
	return cr.SetBackendRoutes(ctx, []types.BackendRoute{route})
}

func (cr *ContainerRedisRepository) SetBackendRoutes(ctx context.Context, routes []types.BackendRoute) error {
	if len(routes) == 0 {
		return nil
	}

	type encodedRoute struct {
		route types.BackendRoute
		data  []byte
	}
	type machineKey struct {
		workspaceID string
		poolName    string
		machineID   string
	}

	encoded := make([]encodedRoute, 0, len(routes))
	machines := make(map[machineKey]struct{})
	machineIDs := make(map[string]struct{})
	for _, route := range routes {
		if route.RouteID == "" {
			return errors.New("route id is required")
		}
		if route.UpdatedAt == 0 {
			route.UpdatedAt = time.Now().Unix()
		}
		data, err := json.Marshal(route)
		if err != nil {
			return fmt.Errorf("failed to marshal backend route %s: %w", route.RouteID, err)
		}
		encoded = append(encoded, encodedRoute{route: route, data: data})
		if route.WorkspaceID != "" && route.PoolName != "" && route.MachineID != "" {
			machines[machineKey{workspaceID: route.WorkspaceID, poolName: route.PoolName, machineID: route.MachineID}] = struct{}{}
		}
		if route.MachineID != "" {
			machineIDs[route.MachineID] = struct{}{}
		}
	}

	_, err := cr.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, item := range encoded {
			route := item.route
			routeKey := common.RedisKeys.SchedulerBackendRoute(route.RouteID)
			if route.Kind == types.BackendRouteKindWorker &&
				route.ContainerID == "" &&
				route.State == types.BackendRouteStateOpening {
				setOpeningWorkerBackendRouteScript.Eval(
					ctx,
					pipe,
					[]string{routeKey},
					item.data,
					types.BackendRouteStateReady,
				)
			} else {
				pipe.Set(ctx, routeKey, item.data, 0)
			}
			if route.ContainerID != "" {
				pipe.SAdd(ctx, common.RedisKeys.SchedulerBackendRouteIndex(route.ContainerID), route.RouteID)
			}
			if route.WorkspaceID != "" && route.PoolName != "" && route.MachineID != "" {
				pipe.SAdd(ctx, common.RedisKeys.SchedulerBackendRouteMachineIndex(route.WorkspaceID, route.PoolName, route.MachineID), route.RouteID)
			}
			if route.MachineID != "" {
				pipe.SAdd(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDIndex(route.MachineID), route.RouteID)
			}
		}
		for machine := range machines {
			key := common.RedisKeys.SchedulerBackendRouteMachineRevision(machine.workspaceID, machine.poolName, machine.machineID)
			pipe.Incr(ctx, key)
			pipe.Publish(ctx, key, common.KeyOperationSet)
		}
		for machineID := range machineIDs {
			key := common.RedisKeys.SchedulerBackendRouteMachineIDRevision(machineID)
			pipe.Incr(ctx, key)
			pipe.Publish(ctx, key, common.KeyOperationSet)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to set backend routes: %w", err)
	}
	return nil
}

func (cr *ContainerRedisRepository) GetBackendRoute(ctx context.Context, routeID string) (*types.BackendRoute, error) {
	data, err := cr.rdb.Get(ctx, common.RedisKeys.SchedulerBackendRoute(routeID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("backend route %s not found", routeID)
		}
		return nil, fmt.Errorf("failed to get backend route %s: %w", routeID, err)
	}
	var route types.BackendRoute
	if err := json.Unmarshal(data, &route); err != nil {
		return nil, fmt.Errorf("failed to unmarshal backend route %s: %w", routeID, err)
	}
	return &route, nil
}

func (cr *ContainerRedisRepository) ListBackendRoutesByMachine(ctx context.Context, workspaceID, poolName, machineID string) ([]types.BackendRoute, error) {
	routeIDs, err := cr.rdb.SMembers(ctx, common.RedisKeys.SchedulerBackendRouteMachineIndex(workspaceID, poolName, machineID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(routeIDs)
	routes, err := cr.routes(ctx, routeIDs)
	if err != nil {
		return nil, err
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].RouteID < routes[j].RouteID
	})
	return routes, nil
}

func (cr *ContainerRedisRepository) ListBackendRoutesByMachineID(ctx context.Context, machineID string) ([]types.BackendRoute, error) {
	routeIDs, err := cr.rdb.SMembers(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDIndex(machineID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(routeIDs)
	routes, err := cr.routes(ctx, routeIDs)
	if err != nil {
		return nil, err
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].RouteID < routes[j].RouteID
	})
	return routes, nil
}

func (cr *ContainerRedisRepository) DeleteBackendRoutesByContainerID(ctx context.Context, containerID string) error {
	indexKey := common.RedisKeys.SchedulerBackendRouteIndex(containerID)
	routeIDs, err := cr.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return err
	}
	routes, err := cr.routes(ctx, routeIDs)
	if err != nil {
		return err
	}

	type machineKey struct {
		workspaceID string
		poolName    string
		machineID   string
	}
	machines := map[machineKey]struct{}{}
	machineIDs := map[string]struct{}{}
	pipe := cr.rdb.Pipeline()
	for _, routeID := range routeIDs {
		pipe.Del(ctx, common.RedisKeys.SchedulerBackendRoute(routeID))
	}
	for _, route := range routes {
		if route.WorkspaceID != "" && route.PoolName != "" && route.MachineID != "" {
			pipe.SRem(ctx, common.RedisKeys.SchedulerBackendRouteMachineIndex(route.WorkspaceID, route.PoolName, route.MachineID), route.RouteID)
			machines[machineKey{workspaceID: route.WorkspaceID, poolName: route.PoolName, machineID: route.MachineID}] = struct{}{}
		}
		if route.MachineID != "" {
			pipe.SRem(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDIndex(route.MachineID), route.RouteID)
			machineIDs[route.MachineID] = struct{}{}
		}
	}
	for machine := range machines {
		pipe.Incr(ctx, common.RedisKeys.SchedulerBackendRouteMachineRevision(machine.workspaceID, machine.poolName, machine.machineID))
	}
	for machineID := range machineIDs {
		pipe.Incr(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDRevision(machineID))
	}
	pipe.Del(ctx, indexKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	for machine := range machines {
		if err := cr.publishBackendRouteMachine(ctx, machine.workspaceID, machine.poolName, machine.machineID); err != nil {
			return err
		}
	}
	for machineID := range machineIDs {
		if err := cr.publishBackendRouteMachineID(ctx, machineID); err != nil {
			return err
		}
	}
	return nil
}

// DeleteBackendRoutesByMachine removes all backend routes and index entries
// for a machine when it is released.
func (cr *ContainerRedisRepository) DeleteBackendRoutesByMachine(ctx context.Context, workspaceID, poolName, machineID string) error {
	if workspaceID == "" || poolName == "" || machineID == "" {
		return nil
	}
	indexKey := common.RedisKeys.SchedulerBackendRouteMachineIndex(workspaceID, poolName, machineID)
	scopedRouteIDs, err := cr.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return err
	}
	machineRouteIDs, err := cr.rdb.SMembers(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDIndex(machineID)).Result()
	if err != nil {
		return err
	}
	routeIDs := dedupeRouteIDs(append(scopedRouteIDs, machineRouteIDs...))
	routes, err := cr.routes(ctx, routeIDs)
	if err != nil {
		return err
	}

	type machineKey struct {
		workspaceID string
		poolName    string
		machineID   string
	}
	machines := map[machineKey]struct{}{}
	machineIDs := map[string]struct{}{}
	pipe := cr.rdb.Pipeline()
	for _, route := range routes {
		if route.MachineID != machineID || route.PoolName != poolName {
			continue
		}
		pipe.Del(ctx, common.RedisKeys.SchedulerBackendRoute(route.RouteID))
		if route.ContainerID != "" {
			pipe.SRem(ctx, common.RedisKeys.SchedulerBackendRouteIndex(route.ContainerID), route.RouteID)
		}
		if route.WorkspaceID != "" && route.PoolName != "" && route.MachineID != "" {
			pipe.SRem(ctx, common.RedisKeys.SchedulerBackendRouteMachineIndex(route.WorkspaceID, route.PoolName, route.MachineID), route.RouteID)
			machines[machineKey{workspaceID: route.WorkspaceID, poolName: route.PoolName, machineID: route.MachineID}] = struct{}{}
		}
		if route.MachineID != "" {
			pipe.SRem(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDIndex(route.MachineID), route.RouteID)
			machineIDs[route.MachineID] = struct{}{}
		}
	}
	pipe.Del(ctx, indexKey)
	machines[machineKey{workspaceID: workspaceID, poolName: poolName, machineID: machineID}] = struct{}{}
	for machine := range machines {
		pipe.Incr(ctx, common.RedisKeys.SchedulerBackendRouteMachineRevision(machine.workspaceID, machine.poolName, machine.machineID))
	}
	for id := range machineIDs {
		pipe.Incr(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDRevision(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	for machine := range machines {
		if err := cr.publishBackendRouteMachine(ctx, machine.workspaceID, machine.poolName, machine.machineID); err != nil {
			return err
		}
	}
	for id := range machineIDs {
		if err := cr.publishBackendRouteMachineID(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (cr *ContainerRedisRepository) publishBackendRouteMachine(ctx context.Context, workspaceID, poolName, machineID string) error {
	return cr.rdb.Publish(ctx, common.RedisKeys.SchedulerBackendRouteMachineRevision(workspaceID, poolName, machineID), common.KeyOperationSet).Err()
}

func (cr *ContainerRedisRepository) publishBackendRouteMachineID(ctx context.Context, machineID string) error {
	return cr.rdb.Publish(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDRevision(machineID), common.KeyOperationSet).Err()
}

func (cr *ContainerRedisRepository) routes(ctx context.Context, routeIDs []string) ([]types.BackendRoute, error) {
	if len(routeIDs) == 0 {
		return []types.BackendRoute{}, nil
	}
	keys := make([]string, 0, len(routeIDs))
	for _, routeID := range routeIDs {
		keys = append(keys, common.RedisKeys.SchedulerBackendRoute(routeID))
	}
	values, err := cr.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	routes := make([]types.BackendRoute, 0, len(values))
	for _, value := range values {
		data, ok := routeBytes(value)
		if !ok {
			continue
		}
		var route types.BackendRoute
		if err := json.Unmarshal(data, &route); err != nil {
			continue
		}
		routes = append(routes, route)
	}
	return routes, nil
}

func dedupeRouteIDs(routeIDs []string) []string {
	if len(routeIDs) < 2 {
		return routeIDs
	}
	seen := make(map[string]struct{}, len(routeIDs))
	out := make([]string, 0, len(routeIDs))
	for _, routeID := range routeIDs {
		if routeID == "" {
			continue
		}
		if _, ok := seen[routeID]; ok {
			continue
		}
		seen[routeID] = struct{}{}
		out = append(out, routeID)
	}
	return out
}

func routeBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}

func (cr *ContainerRedisRepository) SetContainerAddressMap(containerId string, addressMap map[int32]string) error {
	data, err := json.Marshal(addressMap)
	if err != nil {
		return fmt.Errorf("failed to marshal addressMap for container %s: %w", containerId, err)
	}

	err = cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerAddressMap(containerId), data, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to set container addressMap for container %s: %w", containerId, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) GetContainerAddressMap(containerId string) (map[int32]string, error) {
	data, err := cr.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerAddressMap(containerId)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get container addressMap for container %s: %w", containerId, err)
	}

	addressMap := make(map[int32]string)
	if err := json.Unmarshal(data, &addressMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal addressMap for container %s: %w", containerId, err)
	}

	return addressMap, nil
}

func (cr *ContainerRedisRepository) SetWorkerAddress(containerId string, addr string) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerWorkerAddress(containerId), addr, 0).Err()
}

func (cr *ContainerRedisRepository) GetWorkerAddress(ctx context.Context, containerId string) (string, error) {
	internalDeadline := time.Now().Add(workerAddressWaitTimeout)
	ctx, cancel := context.WithDeadline(ctx, internalDeadline)
	defer cancel()

	schedulingFailed := func() bool {
		requestStatus, err := cr.GetContainerRequestStatus(containerId)
		return err == nil && requestStatus == types.ContainerRequestStatusFailed
	}

	tryGetWorkerAddress := func() (addr string, found bool, err error) {
		hostname, err := cr.rdb.Get(ctx, common.RedisKeys.SchedulerWorkerAddress(containerId)).Result()
		if err == nil {
			return hostname, true, nil
		}

		if err != redis.Nil {
			return "", false, fmt.Errorf("failed to get worker addr for container %s: %w", containerId, err)
		}

		if schedulingFailed() {
			return "", false, fmt.Errorf("failed to schedule container, container id: %s", containerId)
		}

		return "", false, nil
	}

	if hostname, found, err := tryGetWorkerAddress(); found || err != nil {
		return hostname, err
	}

	ticker := time.NewTicker(workerAddressPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if time.Now().Before(internalDeadline) {
				return "", fmt.Errorf("context cancelled while trying to get worker addr, container id: %s", containerId)
			}
			if schedulingFailed() {
				return "", fmt.Errorf("failed to schedule container, container id: %s", containerId)
			}
			return "", fmt.Errorf("failed to schedule container, container id: %s", containerId)
		case <-ticker.C:
			if hostname, found, err := tryGetWorkerAddress(); found || err != nil {
				return hostname, err
			}
		}
	}
}

func (cr *ContainerRedisRepository) listContainerStateByIndex(indexKey string, keys []string) ([]types.ContainerState, error) {
	containerStates := make([]types.ContainerState, 0)

	for _, key := range keys {
		exists, err := cr.rdb.Exists(context.TODO(), key).Result()
		if err != nil {
			continue
		}
		if exists == 0 {
			containerId := strings.Split(key, ":")[len(strings.Split(key, ":"))-1]
			exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)

			exitCodeKeyExists, err := cr.rdb.Exists(context.TODO(), exitCodeKey).Result()
			if err != nil {
				continue
			}

			if exitCodeKeyExists > 0 {
				continue
			}

			// We don't have an exit code, or a state key, remove key from set
			cr.rdb.SRem(context.TODO(), indexKey, key)
			continue
		}

		res, err := cr.rdb.HGetAll(context.TODO(), key).Result()
		if err != nil {
			continue
		}

		var state types.ContainerState
		if err = common.ToStruct(res, &state); err != nil {
			continue
		}

		if state.ContainerId == "" {
			continue
		}

		containerStates = append(containerStates, state)
	}

	return containerStates, nil
}

func (cr *ContainerRedisRepository) GetActiveContainersByStubId(stubId string) ([]types.ContainerState, error) {
	indexKey := common.RedisKeys.SchedulerContainerIndex(stubId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve container state keys: %v", err)
	}

	return cr.listContainerStateByIndex(indexKey, keys)
}

func (cr *ContainerRedisRepository) GetActiveContainersByWorkspaceId(workspaceId string) ([]types.ContainerState, error) {
	indexKey := common.RedisKeys.SchedulerContainerWorkspaceIndex(workspaceId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve container state keys: %v", err)
	}

	return cr.listContainerStateByIndex(indexKey, keys)
}

func (cr *ContainerRedisRepository) GetActiveContainersByWorkerId(workerId string) ([]types.ContainerState, error) {
	indexKey := common.RedisKeys.SchedulerContainerWorkerIndex(workerId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve container state keys: %v", err)
	}

	return cr.listContainerStateByIndex(indexKey, keys)
}

func (cr *ContainerRedisRepository) GetFailedContainersByStubId(stubId string) ([]string, error) {
	indexKey := common.RedisKeys.SchedulerContainerIndex(stubId)
	keys, err := cr.rdb.SMembers(context.TODO(), indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve container state keys: %v", err)
	}

	// Retrieve the value (exit code) for each key
	failedContainerIds := make([]string, 0)
	for _, key := range keys {
		containerId := strings.Split(key, ":")[len(strings.Split(key, ":"))-1]
		exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)

		exitCode, err := cr.rdb.Get(context.Background(), exitCodeKey).Int()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to get value for key <%v>: %w", key, err)
		} else if err == redis.Nil {
			continue
		}

		// Check if the exit code is non-zero
		if types.ContainerExitCode(exitCode).IsFailed() {
			failedContainerIds = append(failedContainerIds, containerId)
		}
	}

	return failedContainerIds, nil
}

// ReserveContainerConcurrencyForPending applies workspace quota accounting to an
// already-created PENDING container without changing its state or TTL. Initial
// admissions, including serverless, use CreateContainerStateWithConcurrencyLimit;
// this transition is for a quota-exempt request rerouted to accounted capacity.
func (c *ContainerRedisRepository) ReserveContainerConcurrencyForPending(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error {
	if request == nil || request.ContainerId == "" {
		return errors.New("container request is required")
	}

	if quota == nil {
		_, err := c.tryReserveContainerConcurrencyForPending(nil, request)
		return err
	}

	return c.reserveContainerConcurrencyWithAttempt(quota, request, func() (string, error) {
		return c.tryReserveContainerConcurrencyForPending(quota, request)
	})
}

func (c *ContainerRedisRepository) tryReserveContainerConcurrencyForPending(quota *types.ConcurrencyLimit, request *types.ContainerRequest) (string, error) {
	lockKey := common.RedisKeys.SchedulerContainerLock(request.ContainerId)
	if err := c.lock.Acquire(context.TODO(), lockKey, containerStateLockOptions); err != nil {
		return "", err
	}
	defer c.lock.Release(lockKey)

	ctx := context.TODO()
	storedStatus, err := c.rdb.HGet(ctx, common.RedisKeys.SchedulerContainerState(request.ContainerId), "status").Result()
	if err == redis.Nil {
		return "", &types.ErrContainerStateNotFound{ContainerId: request.ContainerId}
	}
	if err != nil {
		return "", fmt.Errorf("failed to get container status: %w", err)
	}
	if types.ContainerStatus(storedStatus) != types.ContainerStatusPending {
		return "", fmt.Errorf("container <%s> is no longer pending (stored status: %s)", request.ContainerId, storedStatus)
	}
	if quota == nil {
		return "", nil
	}

	return c.tryReserveContainerConcurrencyWithContext(ctx, quota, request)
}

func (c *ContainerRedisRepository) CreateContainerStateWithConcurrencyLimit(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error {
	return c.createContainerState(quota, request, nil, time.Time{})
}

func (c *ContainerRedisRepository) CreateContainerStateWithConcurrencyLimitAndStateVolumeOutbox(quota *types.ConcurrencyLimit, request *types.ContainerRequest, payload []byte, readyAt time.Time) error {
	if request == nil || request.StateVolumePlanId == "" || request.StateVolumePlanHash == "" || len(payload) == 0 || readyAt.IsZero() {
		return fmt.Errorf("state-volume admission requires an exact plan identity, canonical request payload, and ready time")
	}
	return c.createContainerState(quota, request, payload, readyAt)
}

func (c *ContainerRedisRepository) createContainerState(quota *types.ConcurrencyLimit, request *types.ContainerRequest, stateVolumePayload []byte, stateVolumeReadyAt time.Time) error {
	lockKey := common.RedisKeys.SchedulerContainerLock(request.ContainerId)
	if err := c.lock.Acquire(context.TODO(), lockKey, containerStateLockOptions); err != nil {
		return err
	}
	defer c.lock.Release(lockKey)
	status, err := c.rdb.HGet(context.TODO(), common.RedisKeys.SchedulerContainerState(request.ContainerId), "status").Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to get container status: %w", err)
	}
	if status == string(types.ContainerStatusPending) ||
		status == string(types.ContainerStatusRunning) ||
		status == string(types.ContainerStatusStopping) {
		return &types.ContainerAlreadyScheduledError{Msg: "a container with this id is still active"}
	}
	if quota != nil && len(stateVolumePayload) == 0 {
		if err := c.reserveContainerConcurrency(quota, request); err != nil {
			return err
		}
	}
	state := &types.ContainerState{
		ContainerId:         request.ContainerId,
		StubId:              request.StubId,
		WorkspaceId:         request.WorkspaceId,
		Status:              types.ContainerStatusPending,
		ScheduledAt:         time.Now().Unix(),
		Gpu:                 request.Gpu,
		GpuCount:            request.GpuCount,
		NbdDevices:          request.RequiredNbdDevices(),
		Cpu:                 request.Cpu,
		Memory:              request.Memory,
		MachineId:           request.MachineId,
		StateSnapshotId:     request.StateSnapshotId,
		StateFork:           request.StateFork,
		StateVolumePlanId:   request.StateVolumePlanId,
		StateVolumePlanHash: request.StateVolumePlanHash,
	}
	if len(stateVolumePayload) != 0 {
		err = c.admitStateVolumeContainer(quota, request, state, stateVolumePayload, stateVolumeReadyAt)
	} else {
		err = c.setContainerState(request.ContainerId, state)
	}
	if err == nil {
		return nil
	}
	if quota != nil && len(stateVolumePayload) == 0 {
		if releaseErr := c.releaseContainerConcurrencyReservation(context.TODO(), request.WorkspaceId, request.ContainerId); releaseErr != nil {
			return errors.Join(err, fmt.Errorf("failed to release concurrency reservation after container state error: %w", releaseErr))
		}
	}
	return err
}

func (c *ContainerRedisRepository) admitStateVolumeContainer(quota *types.ConcurrencyLimit, request *types.ContainerRequest, state *types.ContainerState, payload []byte, readyAt time.Time) error {
	if quota != nil {
		if err := c.ensureWorkspaceConcurrencyCounter(request.WorkspaceId); err != nil {
			return err
		}
	}
	err := c.setContainerStateWithStateVolumeOutbox(state, payload, readyAt, quota)
	if errors.Is(err, errConcurrencyCounterRepairing) {
		if ensureErr := c.ensureWorkspaceConcurrencyCounter(request.WorkspaceId); ensureErr != nil {
			return ensureErr
		}
		return c.setContainerStateWithStateVolumeOutbox(state, payload, readyAt, quota)
	}
	var throttled *types.ThrottledByConcurrencyLimitError
	if quota == nil || !errors.As(err, &throttled) {
		return err
	}
	repaired, repairErr := c.repairWorkspaceConcurrencyCounterAfterThrottle(request.WorkspaceId)
	if repairErr != nil {
		return repairErr
	}
	if repaired {
		err = c.setContainerStateWithStateVolumeOutbox(state, payload, readyAt, quota)
	}
	if err != nil {
		metrics.RecordConcurrencyLimitThrottle(throttled.Reason, request)
	}
	return err
}

func (c *ContainerRedisRepository) CheckContainerConcurrencyLimit(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error {
	if quota == nil {
		return nil
	}

	if err := c.ensureWorkspaceConcurrencyCounter(request.WorkspaceId); err != nil {
		return err
	}

	reason, err := c.checkContainerConcurrencyLimit(quota, request)
	if errors.Is(err, errConcurrencyCounterRepairing) {
		if err := c.ensureWorkspaceConcurrencyCounter(request.WorkspaceId); err != nil {
			return err
		}
		reason, err = c.checkContainerConcurrencyLimit(quota, request)
	}
	if err == nil {
		return nil
	}

	var throttled *types.ThrottledByConcurrencyLimitError
	if !errors.As(err, &throttled) {
		return err
	}

	repaired, repairErr := c.repairWorkspaceConcurrencyCounterAfterThrottle(request.WorkspaceId)
	if repairErr != nil {
		return repairErr
	}
	if repaired {
		reason, err = c.checkContainerConcurrencyLimit(quota, request)
	}
	if err != nil && reason != "" {
		var finalThrottle *types.ThrottledByConcurrencyLimitError
		if !errors.As(err, &finalThrottle) {
			return err
		}
		metrics.RecordConcurrencyLimitThrottle(reason, request)
	}

	return err
}

func (c *ContainerRedisRepository) reserveContainerConcurrency(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error {
	return c.reserveContainerConcurrencyWithAttempt(quota, request, func() (string, error) {
		return c.tryReserveContainerConcurrency(quota, request)
	})
}

func (c *ContainerRedisRepository) reserveContainerConcurrencyWithAttempt(
	quota *types.ConcurrencyLimit,
	request *types.ContainerRequest,
	tryReserve func() (string, error),
) error {
	if err := c.ensureWorkspaceConcurrencyCounter(request.WorkspaceId); err != nil {
		return err
	}

	reason, err := tryReserve()
	if errors.Is(err, errConcurrencyCounterRepairing) {
		if err := c.ensureWorkspaceConcurrencyCounter(request.WorkspaceId); err != nil {
			return err
		}
		reason, err = tryReserve()
	}
	if err == nil {
		return nil
	}

	var throttled *types.ThrottledByConcurrencyLimitError
	if !errors.As(err, &throttled) {
		return err
	}

	repaired, repairErr := c.repairWorkspaceConcurrencyCounterAfterThrottle(request.WorkspaceId)
	if repairErr != nil {
		return repairErr
	}
	if !repaired {
		if reason != "" {
			metrics.RecordConcurrencyLimitThrottle(reason, request)
		}
		return err
	}

	reason, err = tryReserve()
	if err != nil && reason != "" {
		metrics.RecordConcurrencyLimitThrottle(reason, request)
	}

	return err
}

func (c *ContainerRedisRepository) checkContainerConcurrencyLimit(quota *types.ConcurrencyLimit, request *types.ContainerRequest) (string, error) {
	ctx := context.TODO()
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(request.WorkspaceId)

	initialized, err := c.rdb.HGet(ctx, usageKey, "initialized").Result()
	if err != nil && err != redis.Nil {
		return "", err
	}
	if err == redis.Nil || initialized != concurrencyCounterInitialized {
		return "repairing", errConcurrencyCounterRepairing
	}

	usedGpuCount, err := c.workspaceConcurrencyUsageValue(ctx, usageKey, "gpu_count")
	if err != nil {
		return "", err
	}

	usedCpu, err := c.workspaceConcurrencyUsageValue(ctx, usageKey, "cpu")
	if err != nil {
		return "", err
	}

	if usedGpuCount+int64(request.GpuCount) > int64(quota.GPULimit) {
		return "gpu", &types.ThrottledByConcurrencyLimitError{Reason: "gpu quota exceeded"}
	}

	if usedCpu+request.Cpu > int64(quota.CPUMillicoreLimit) {
		return "cpu", &types.ThrottledByConcurrencyLimitError{Reason: "cpu quota exceeded"}
	}

	return "", nil
}

func (c *ContainerRedisRepository) workspaceConcurrencyUsageValue(ctx context.Context, usageKey, field string) (int64, error) {
	value, err := c.rdb.HGet(ctx, usageKey, field).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return value, err
}

func (c *ContainerRedisRepository) ensureWorkspaceConcurrencyCounter(workspaceId string) error {
	ctx := context.TODO()
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(workspaceId)

	initialized, err := c.rdb.HGet(ctx, usageKey, "initialized").Result()
	if err == nil && initialized == concurrencyCounterInitialized {
		return nil
	}
	if err != nil && err != redis.Nil {
		return err
	}

	lock, err := redislock.Obtain(ctx, c.rdb, common.RedisKeys.WorkspaceConcurrencyLimitLock(workspaceId), concurrencyCounterInitTimeout, nil)
	if err != nil && err != redislock.ErrNotObtained {
		return err
	}
	if err == redislock.ErrNotObtained {
		return c.waitForWorkspaceConcurrencyCounter(ctx, usageKey, workspaceId)
	}
	defer lock.Release(ctx)

	initialized, err = c.rdb.HGet(ctx, usageKey, "initialized").Result()
	if err == nil && initialized == concurrencyCounterInitialized {
		return nil
	}
	if err != nil && err != redis.Nil {
		return err
	}

	return c.rebuildWorkspaceConcurrencyCounter(ctx, workspaceId)
}

func (c *ContainerRedisRepository) waitForWorkspaceConcurrencyCounter(ctx context.Context, usageKey, workspaceId string) error {
	deadline := time.After(concurrencyCounterInitTimeout)
	ticker := time.NewTicker(concurrencyCounterInitPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("concurrency limit usage initialization timed out for workspace %s", workspaceId)
		case <-ticker.C:
			initialized, err := c.rdb.HGet(ctx, usageKey, "initialized").Result()
			if err == nil && initialized == concurrencyCounterInitialized {
				return nil
			}
			if err != nil && err != redis.Nil {
				return err
			}
		}
	}
}

func (c *ContainerRedisRepository) rebuildWorkspaceConcurrencyCounter(ctx context.Context, workspaceId string) error {
	nowTime := time.Now()
	now := nowTime.Unix()
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(workspaceId)
	reservationIndexKey := common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(workspaceId)
	// Make repairs visible to lock-free reserve/release scripts before taking
	// the snapshot so they cannot race the final aggregate write.
	if err := c.rdb.HSet(ctx, usageKey,
		"initialized", concurrencyCounterRepairing,
		"repair_started_at", now,
	).Err(); err != nil {
		return err
	}

	containers, err := c.GetActiveContainersByWorkspaceId(workspaceId)
	if err != nil {
		return err
	}

	totalGpuCount := int64(0)
	totalCpu := int64(0)
	statesByContainerId := map[string]types.ContainerState{}

	pipe := c.rdb.TxPipeline()
	for _, container := range containers {
		statesByContainerId[container.ContainerId] = container
		if container.Status == types.ContainerStatusStopping {
			continue
		}

		totalGpuCount += int64(container.GpuCount)
		totalCpu += container.Cpu
		pipe.HSet(ctx, common.RedisKeys.WorkspaceConcurrencyLimitReservation(workspaceId, container.ContainerId),
			"workspace_id", workspaceId,
			"container_id", container.ContainerId,
			"gpu_count", container.GpuCount,
			"cpu", container.Cpu,
			"created_at", now,
		)
		pipe.SAdd(ctx, reservationIndexKey, container.ContainerId)
	}

	reservationIds, err := c.rdb.SMembers(ctx, reservationIndexKey).Result()
	if err != nil {
		return err
	}

	for _, reservationId := range reservationIds {
		if reservationId == "" {
			pipe.SRem(ctx, reservationIndexKey, reservationId)
			continue
		}
		reservationKey := common.RedisKeys.WorkspaceConcurrencyLimitReservation(workspaceId, reservationId)
		res, err := c.rdb.HGetAll(ctx, reservationKey).Result()
		if err != nil {
			return err
		}
		if len(res) == 0 {
			pipe.SRem(ctx, reservationIndexKey, reservationId)
			continue
		}

		var reservation concurrencyReservation
		if err := common.ToStruct(res, &reservation); err != nil {
			pipe.Del(ctx, reservationKey)
			pipe.SRem(ctx, reservationIndexKey, reservationId)
			continue
		}
		if reservation.ContainerId == "" {
			reservation.ContainerId = reservationId
		}

		state, stateExists := statesByContainerId[reservation.ContainerId]
		if stateExists {
			if state.Status == types.ContainerStatusStopping {
				pipe.Del(ctx, reservationKey)
				pipe.SRem(ctx, reservationIndexKey, reservation.ContainerId)
			}
			continue
		}

		if reservation.CreatedAt <= 0 || nowTime.Sub(time.Unix(reservation.CreatedAt, 0)) > concurrencyReservationInFlightTTL {
			pipe.Del(ctx, reservationKey)
			pipe.SRem(ctx, reservationIndexKey, reservation.ContainerId)
			continue
		}

		totalGpuCount += reservation.GpuCount
		totalCpu += reservation.Cpu
	}

	pipe.HSet(ctx, usageKey,
		"gpu_count", totalGpuCount,
		"cpu", totalCpu,
		"initialized", concurrencyCounterInitialized,
		"updated_at", now,
		"repaired_at", now,
	)

	_, err = pipe.Exec(ctx)
	return err
}

func (c *ContainerRedisRepository) repairWorkspaceConcurrencyCounterAfterThrottle(workspaceId string) (bool, error) {
	ctx := context.TODO()
	usageKey := common.RedisKeys.WorkspaceConcurrencyLimitUsage(workspaceId)

	needsRepair, err := c.workspaceConcurrencyCounterNeedsRepair(ctx, usageKey)
	if err != nil {
		return false, err
	}
	if !needsRepair {
		return false, nil
	}

	lock, err := redislock.Obtain(ctx, c.rdb, common.RedisKeys.WorkspaceConcurrencyLimitLock(workspaceId), concurrencyCounterInitTimeout, nil)
	if err != nil && err != redislock.ErrNotObtained {
		return false, err
	}
	if err == redislock.ErrNotObtained {
		return false, nil
	}
	defer lock.Release(ctx)

	needsRepair, err = c.workspaceConcurrencyCounterNeedsRepair(ctx, usageKey)
	if err != nil {
		return false, err
	}
	if !needsRepair {
		return false, nil
	}

	return true, c.rebuildWorkspaceConcurrencyCounter(ctx, workspaceId)
}

func (c *ContainerRedisRepository) workspaceConcurrencyCounterNeedsRepair(ctx context.Context, usageKey string) (bool, error) {
	repairedAt, err := c.rdb.HGet(ctx, usageKey, "repaired_at").Int64()
	if err == redis.Nil {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	return time.Since(time.Unix(repairedAt, 0)) >= concurrencyCounterRepairInterval, nil
}

func (c *ContainerRedisRepository) tryReserveContainerConcurrency(quota *types.ConcurrencyLimit, request *types.ContainerRequest) (string, error) {
	return c.tryReserveContainerConcurrencyWithContext(context.TODO(), quota, request)
}

func (c *ContainerRedisRepository) tryReserveContainerConcurrencyWithContext(ctx context.Context, quota *types.ConcurrencyLimit, request *types.ContainerRequest) (string, error) {
	result, err := reserveConcurrencyReservationScript.Run(ctx, c.rdb, []string{
		common.RedisKeys.WorkspaceConcurrencyLimitUsage(request.WorkspaceId),
		common.RedisKeys.WorkspaceConcurrencyLimitReservation(request.WorkspaceId, request.ContainerId),
		common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(request.WorkspaceId),
	},
		int64(quota.GPULimit),
		int64(quota.CPUMillicoreLimit),
		int64(request.GpuCount),
		request.Cpu,
		request.WorkspaceId,
		request.ContainerId,
		time.Now().Unix(),
	).Text()
	if err != nil {
		return "", err
	}

	switch result {
	case concurrencyReservationOK:
		return "", nil
	case concurrencyReservationRepairing:
		return "repairing", errConcurrencyCounterRepairing
	case concurrencyReservationGPUExceeded:
		return "gpu", &types.ThrottledByConcurrencyLimitError{Reason: "gpu quota exceeded"}
	case concurrencyReservationCPUExceeded:
		return "cpu", &types.ThrottledByConcurrencyLimitError{Reason: "cpu quota exceeded"}
	default:
		return "", fmt.Errorf("unexpected concurrency reservation result: %s", result)
	}
}

func (c *ContainerRedisRepository) releaseContainerConcurrencyReservation(ctx context.Context, workspaceId, containerId string) error {
	if workspaceId == "" || containerId == "" {
		return nil
	}

	for {
		result, err := releaseConcurrencyReservationScript.Run(ctx, c.rdb, []string{
			common.RedisKeys.WorkspaceConcurrencyLimitUsage(workspaceId),
			common.RedisKeys.WorkspaceConcurrencyLimitReservation(workspaceId, containerId),
			common.RedisKeys.WorkspaceConcurrencyLimitReservationIndex(workspaceId),
		}, time.Now().Unix(), containerId).Result()
		if err != nil {
			return err
		}
		if result != concurrencyReservationRepairing {
			return nil
		}
		if err := c.ensureWorkspaceConcurrencyCounter(workspaceId); err != nil {
			return err
		}
	}
}

func (cr *ContainerRedisRepository) GetStubState(stubId string) (string, error) {
	stateKey := common.RedisKeys.SchedulerStubState(stubId)
	state, err := cr.rdb.Get(context.TODO(), stateKey).Result()
	if err != nil {
		if err == redis.Nil {
			return types.StubStateHealthy, nil
		}
		return "", err
	}

	return state, nil
}

var unhealthyStateTTL = 10 * time.Minute

func (cr *ContainerRedisRepository) SetStubState(stubId, state string) error {
	stateKey := common.RedisKeys.SchedulerStubState(stubId)
	return cr.rdb.SetEx(context.TODO(), stateKey, state, unhealthyStateTTL).Err()
}

func (cr *ContainerRedisRepository) DeleteStubState(stubId string) error {
	stateKey := common.RedisKeys.SchedulerStubState(stubId)
	return cr.rdb.Del(context.TODO(), stateKey).Err()
}

func (cr *ContainerRedisRepository) SetContainerRequestStatus(containerId string, status types.ContainerRequestStatus) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.SchedulerContainerRequestStatus(containerId), string(status), types.ContainerRequestStatusTTL).Err()
}

func (cr *ContainerRedisRepository) GetContainerRequestStatus(containerId string) (types.ContainerRequestStatus, error) {
	status, err := cr.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerRequestStatus(containerId)).Result()
	if err != nil {
		return "", err
	}

	return types.ContainerRequestStatus(status), nil
}

func (cr *ContainerRedisRepository) SetBuildContainerTTL(containerId string, ttl time.Duration) error {
	return cr.rdb.Set(context.TODO(), common.RedisKeys.ImageBuildContainerTTL(containerId), "1", ttl).Err()
}

func (cr *ContainerRedisRepository) RefreshBuildContainerTTL(containerId string, ttl time.Duration) (bool, error) {
	return cr.rdb.ExpireXX(context.TODO(), common.RedisKeys.ImageBuildContainerTTL(containerId), ttl).Result()
}

func (c *ContainerRedisRepository) SetPodKeepWarmLock(ctx context.Context, workspaceName, stubId, containerId string, keepWarmSeconds int) error {
	return c.setKeepWarmLock(ctx, podKeepWarmLockKey(workspaceName, stubId, containerId), keepWarmSeconds)
}

func (c *ContainerRedisRepository) PodKeepWarmLockExists(ctx context.Context, workspaceName, stubId, containerId string) (bool, error) {
	return c.keepWarmLockExists(ctx, podKeepWarmLockKey(workspaceName, stubId, containerId))
}

func (c *ContainerRedisRepository) setKeepWarmLock(ctx context.Context, key string, keepWarmSeconds int) error {
	if key == "" {
		return nil
	}

	switch {
	case keepWarmSeconds < 0:
		return c.rdb.Set(ctx, key, 1, 0).Err()
	case keepWarmSeconds == 0:
		return c.rdb.Del(ctx, key).Err()
	default:
		return c.rdb.SetEx(ctx, key, 1, time.Duration(keepWarmSeconds)*time.Second).Err()
	}
}

func (c *ContainerRedisRepository) keepWarmLockExists(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, nil
	}

	keepWarm, err := c.rdb.Get(ctx, key).Int()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return keepWarm > 0, nil
}

func podKeepWarmLockKey(workspaceName, stubId, containerId string) string {
	if workspaceName == "" || stubId == "" || containerId == "" {
		return ""
	}
	return common.RedisKeys.PodKeepWarmLock(workspaceName, stubId, containerId)
}
