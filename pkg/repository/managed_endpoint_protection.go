package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

// ErrReplicaProtectionChanged is retryable after the controller refreshes its
// replica snapshot. A stopping, evicted, or unassigned container cannot be promoted.
var ErrReplicaProtectionChanged = errors.New("replica protection assignment changed; retry reconciliation")

// JSON is passed through as opaque strings: cjson would round config revisions,
// engine metrics, and other large numbers. Hash writes leave heartbeat fields
// and TTLs untouched. Every precondition is checked before the first write.
var setReplicaProtectionScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1]
	or redis.call("GET", KEYS[4]) ~= ARGV[3]
	or redis.call("SISMEMBER", KEYS[5], KEYS[2]) ~= 1 then return 0 end
local pos = 8
for key = 2, 3 do
	local count = tonumber(ARGV[pos]); pos = pos + 1
	for i = 1, count do
		local value = redis.call("HGET", KEYS[key], ARGV[pos])
		if (value or "") ~= ARGV[pos + 1] then return 0 end
		pos = pos + 2
	end
end
local queued = {}
local count = tonumber(ARGV[pos]); pos = pos + 1
for i = 1, count do
	if redis.call("LINDEX", KEYS[6], ARGV[pos]) ~= ARGV[pos + 1] then return 0 end
	table.insert(queued, {ARGV[pos], ARGV[pos + 2]}); pos = pos + 3
end
local pending = {}
count = tonumber(ARGV[pos]); pos = pos + 1
for i = 1, count do
	if redis.call("HGET", KEYS[7], ARGV[pos]) ~= ARGV[pos + 1] then return 0 end
	table.insert(pending, {ARGV[pos], ARGV[pos + 2]}); pos = pos + 3
end
redis.call("HINCRBY", KEYS[3], "resource_version", 1)
redis.call("HSET", KEYS[3], "evictable_cpu", ARGV[5], "evictable_memory", ARGV[6], "evictable_gpu_count", ARGV[7])
redis.call("HSET", KEYS[2], "evictable", ARGV[4])
redis.call("SET", KEYS[1], ARGV[2], "KEEPTTL")
for _, item in ipairs(queued) do redis.call("LSET", KEYS[6], item[1], item[2]) end
for _, item in ipairs(pending) do redis.call("HSET", KEYS[7], item[1], item[2]) end
return 1
`)

// SetReplicaProtection changes an assigned replica without replacing its process.
// Scheduling already takes this worker lease and checks each victim's live flag;
// protection adds no work to the ordinary serverless scheduling path.
func (r *ManagedEndpointRedisRepository) SetReplicaProtection(ctx context.Context, replicaID string, protected bool) (*types.EndpointReplica, error) {
	var result *types.EndpointReplica
	err := r.WithReplicaLock(ctx, replicaID, func(ctx context.Context) error {
		replica, err := r.GetReplica(ctx, replicaID)
		if err != nil {
			return err
		}
		if replica == nil || !replica.Alive() || replica.WorkerID == "" || replica.ContainerID == "" {
			return ErrReplicaProtectionChanged
		}
		return r.lock.WithLease(ctx, common.RedisKeys.SchedulerWorkerLock(replica.WorkerID), schedulerWorkerLockOptions, func(ctx context.Context) error {
			for attempt := 0; attempt < 3; attempt++ {
				result, err = r.setReplicaProtection(ctx, replicaID, replica.ContainerID, replica.WorkerID, protected)
				if !errors.Is(err, ErrReplicaProtectionChanged) {
					return err
				}
			}
			return err
		})
	})
	return result, err
}

func (r *ManagedEndpointRedisRepository) setReplicaProtection(ctx context.Context, replicaID, containerID, workerID string, protected bool) (*types.EndpointReplica, error) {
	replicaKey := meKey("replica", replicaID)
	containerKey := common.RedisKeys.SchedulerContainerState(containerID)
	workerKey := common.RedisKeys.SchedulerWorkerState(workerID)
	queueKey := common.RedisKeys.SchedulerWorkerRequests(workerID)
	pendingKey := common.RedisKeys.SchedulerWorkerPendingRequests(workerID)
	pipe := r.rdb.Pipeline()
	rawCmd := pipe.Get(ctx, replicaKey)
	stateCmd := pipe.HGetAll(ctx, containerKey)
	workerCmd := pipe.HGetAll(ctx, workerKey)
	queuedCmd := pipe.LRange(ctx, queueKey, 0, -1)
	pendingCmd := pipe.HGetAll(ctx, pendingKey)
	ownerCmd := pipe.Get(ctx, meKey("replica_container", containerID))
	indexedCmd := pipe.SIsMember(ctx, common.RedisKeys.SchedulerContainerWorkerIndex(workerID), containerKey)
	if _, err := pipe.Exec(ctx); err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrReplicaProtectionChanged
		}
		return nil, err
	}
	raw := rawCmd.Val()
	var replica types.EndpointReplica
	if err := json.Unmarshal([]byte(raw), &replica); err != nil {
		return nil, err
	}
	state, workerHash := stateCmd.Val(), workerCmd.Val()
	if replica.ID != replicaID || replica.ContainerID != containerID || replica.WorkerID != workerID || !replica.Alive() ||
		ownerCmd.Val() != replicaID || !indexedCmd.Val() ||
		state["container_id"] != containerID || state["worker_id"] != workerID || workerHash["id"] != workerID ||
		(state["status"] != string(types.ContainerStatusPending) && state["status"] != string(types.ContainerStatusRunning)) ||
		state["evicting"] == "true" || state["evicting"] == "1" ||
		(replica.MachineID != "" && replica.MachineID != state["machine_id"]) || state["machine_id"] != workerHash["machine_id"] {
		return nil, ErrReplicaProtectionChanged
	}
	evictable := state["evictable"] == "true" || state["evictable"] == "1"
	if replica.Protected == protected && evictable == !protected {
		return &replica, nil
	}
	worker := workerFromHash(workerKey, workerHash)
	if worker.ResourceVersion == math.MaxInt64 {
		return nil, errors.New("worker resource version exhausted")
	}
	workers := &WorkerRedisRepository{rdb: r.rdb, lock: r.lock}
	usage, err := workers.getWorkerReservedCapacity(ctx, workerID)
	if err != nil {
		return nil, err
	}
	if evictable != !protected {
		cpu, err := strconv.ParseInt(state["cpu"], 10, 64)
		if err != nil || cpu < 0 {
			return nil, ErrReplicaProtectionChanged
		}
		memory, err := strconv.ParseInt(state["memory"], 10, 64)
		if err != nil || memory < 0 || memory > math.MaxInt64/125 {
			return nil, ErrReplicaProtectionChanged
		}
		gpu, err := strconv.ParseUint(state["gpu_count"], 10, 32)
		if err != nil {
			return nil, ErrReplicaProtectionChanged
		}
		memory = capacityMemoryForRequest(&types.ContainerRequest{Memory: memory})
		gpuCount := gpuCountForCapacity(state["gpu"], nil, uint32(gpu))
		if protected {
			if usage.evictableCPU < cpu || usage.evictableMemory < memory || usage.evictableGPU < gpuCount {
				return nil, ErrReplicaProtectionChanged
			}
			usage.evictableCPU -= cpu
			usage.evictableMemory -= memory
			usage.evictableGPU -= gpuCount
		} else {
			usage.evictableCPU += cpu
			usage.evictableMemory += memory
			usage.evictableGPU += gpuCount
		}
	}
	updated, err := protectionJSONField(raw, "protected", protected)
	if err != nil {
		return nil, err
	}
	args := []interface{}{raw, updated, replicaID, strconv.FormatBool(!protected), usage.evictableCPU, usage.evictableMemory, usage.evictableGPU}
	args = protectionHashCAS(args, state, []string{"container_id", "worker_id", "machine_id", "status", "evictable", "evicting", "cpu", "memory", "gpu", "gpu_count", schedulerAssignmentIDField, schedulerDeliveryTokenField})
	args = protectionHashCAS(args, workerHash, []string{"id", "machine_id", "resource_version", "evictable_cpu", "evictable_memory", "evictable_gpu_count"})
	var queueChanges, pendingChanges []interface{}
	for index, rawRequest := range queuedCmd.Val() {
		updated, changed, err := protectionRequestJSON(rawRequest, containerID, !protected)
		if err != nil {
			return nil, err
		}
		if changed {
			queueChanges = append(queueChanges, index, rawRequest, updated)
		}
	}
	for assignment, rawPending := range pendingCmd.Val() {
		var pending pendingContainerRequest
		if err := json.Unmarshal([]byte(rawPending), &pending); err != nil {
			return nil, err
		}
		updated, changed, err := protectionRequestJSON(pending.Request, containerID, !protected)
		if err != nil {
			return nil, err
		}
		if changed {
			updatedPending, err := protectionJSONField(rawPending, "request", updated)
			if err != nil {
				return nil, err
			}
			pendingChanges = append(pendingChanges, assignment, rawPending, updatedPending)
		}
	}
	args = append(args, len(queueChanges)/3)
	args = append(args, queueChanges...)
	args = append(args, len(pendingChanges)/3)
	args = append(args, pendingChanges...)
	committed, err := setReplicaProtectionScript.Run(ctx, r.rdb, []string{
		replicaKey, containerKey, workerKey, meKey("replica_container", containerID), common.RedisKeys.SchedulerContainerWorkerIndex(workerID), queueKey, pendingKey,
	}, args...).Int()
	if err != nil {
		return nil, err
	}
	if committed == 0 {
		return nil, ErrReplicaProtectionChanged
	}
	replica.Protected = protected
	return &replica, nil
}

func protectionHashCAS(args []interface{}, fields map[string]string, keys []string) []interface{} {
	args = append(args, len(keys))
	for _, key := range keys {
		args = append(args, key, fields[key])
	}
	return args
}

func protectionJSONField(raw, key string, value any) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	fields[key] = encoded
	updated, err := json.Marshal(fields)
	return string(updated), err
}

func protectionRequestJSON(raw, containerID string, evictable bool) (string, bool, error) {
	var request struct {
		ContainerID string `json:"container_id"`
		Evictable   bool   `json:"evictable"`
	}
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		return "", false, fmt.Errorf("read queued protection state: %w", err)
	}
	if request.ContainerID != containerID || request.Evictable == evictable {
		return raw, false, nil
	}
	updated, err := protectionJSONField(raw, "evictable", evictable)
	return updated, err == nil, err
}
