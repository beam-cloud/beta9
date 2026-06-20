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
	workerAddressPollInterval          = 250 * time.Millisecond
)

var errConcurrencyCounterRepairing = errors.New("concurrency counter repair in progress")

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
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 2})
	if err != nil {
		return nil, err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

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

func (cr *ContainerRedisRepository) SetContainerState(containerId string, state *types.ContainerState) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

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
		"cpu", state.Cpu,
		"memory", state.Memory,
	)
	pipe.Expire(ctx, stateKey, time.Duration(types.ContainerStateTtlSWhilePending)*time.Second)
	pipe.SAdd(ctx, stubIndexKey, stateKey)
	pipe.SAdd(ctx, workspaceIndexKey, stateKey)
	if _, err = pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to set container state and indexes <%v>: %w", stateKey, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) SetContainerExitCode(containerId string, exitCode int) error {
	exitCodeKey := common.RedisKeys.SchedulerContainerExitCode(containerId)
	err := cr.rdb.SetEx(context.TODO(), exitCodeKey, exitCode, time.Duration(types.ContainerExitCodeTtlS)*time.Second).Err()
	if err != nil {
		return fmt.Errorf("failed to set exit code <%v> for container <%v>: %w", exitCodeKey, containerId, err)
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

func (cr *ContainerRedisRepository) UpdateContainerStatus(containerId string, status types.ContainerStatus, expirySeconds int64) error {
	expiry := time.Duration(expirySeconds) * time.Second

	switch status {
	case types.ContainerStatusPending, types.ContainerStatusRunning, types.ContainerStatusStopping:
		// continue
	default:
		return fmt.Errorf("invalid status: %s", status)
	}

	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
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

	// Convert response to struct
	state := &types.ContainerState{}
	err = common.ToStruct(res, state)
	if err != nil {
		return fmt.Errorf("failed to deserialize container state: %v", err)
	}

	previousStatus := types.ContainerStatus(state.Status)
	if status == types.ContainerStatusRunning && previousStatus == types.ContainerStatusStopping {
		return nil
	}

	// Update StartedAt if this is the first time we set container status to RUNNING
	if status == types.ContainerStatusRunning && previousStatus != types.ContainerStatusRunning {
		state.StartedAt = time.Now().Unix()
	}

	// Update status
	state.Status = status

	// Save state to database
	err = cr.rdb.HSet(context.TODO(), stateKey, common.ToSlice(state)).Err()
	if err != nil {
		return fmt.Errorf("failed to update container state status <%v>: %w", stateKey, err)
	}

	// Set ttl on state
	err = cr.rdb.Expire(context.TODO(), stateKey, expiry).Err()
	if err != nil {
		return fmt.Errorf("failed to set container state ttl <%v>: %w", stateKey, err)
	}

	if status == types.ContainerStatusStopping {
		// The release script is idempotent. Run it on every STOPPING update so
		// callers can retry if a previous release failed after status persisted.
		if err := cr.releaseContainerConcurrencyReservation(context.TODO(), state.WorkspaceId, containerId); err != nil {
			return err
		}
	}

	return nil
}

func (cr *ContainerRedisRepository) UpdateAssignedContainerGPU(containerId string, gpuType string) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
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

	// Convert response to struct
	state := &types.ContainerState{}
	err = common.ToStruct(res, state)
	if err != nil {
		return fmt.Errorf("failed to deserialize container state: %v", err)
	}

	// Update GPU
	state.Gpu = gpuType

	// Save state to database
	err = cr.rdb.HSet(context.TODO(), stateKey, common.ToSlice(state)).Err()
	if err != nil {
		return fmt.Errorf("failed to update container state gpu <%v>: %w", stateKey, err)
	}

	return nil
}

func (cr *ContainerRedisRepository) DeleteContainerState(containerId string) error {
	err := cr.lock.Acquire(context.TODO(), common.RedisKeys.SchedulerContainerLock(containerId), common.RedisLockOptions{TtlS: 10, Retries: 5})
	if err != nil {
		return err
	}
	defer cr.lock.Release(common.RedisKeys.SchedulerContainerLock(containerId))

	stateKey := common.RedisKeys.SchedulerContainerState(containerId)
	workspaceId, err := cr.rdb.HGet(context.TODO(), stateKey, "workspace_id").Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to get container workspace <%v>: %w", stateKey, err)
	}

	if workspaceId != "" {
		if err := cr.rdb.HSet(context.TODO(), stateKey, "status", string(types.ContainerStatusStopping)).Err(); err != nil {
			return fmt.Errorf("failed to mark container stopping before delete <%v>: %w", stateKey, err)
		}
		if err := cr.releaseContainerConcurrencyReservation(context.TODO(), workspaceId, containerId); err != nil {
			return err
		}
	}

	err = cr.rdb.Del(context.TODO(), stateKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete container state <%v>: %w", stateKey, err)
	}

	addrKey := common.RedisKeys.SchedulerContainerAddress(containerId)
	err = cr.rdb.Del(context.TODO(), addrKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete container addr <%v>: %w", addrKey, err)
	}

	addrMapKey := common.RedisKeys.SchedulerContainerAddressMap(containerId)
	err = cr.rdb.Del(context.TODO(), addrMapKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete container addrMap <%v>: %w", addrMapKey, err)
	}

	workerAddrKey := common.RedisKeys.SchedulerWorkerAddress(containerId)
	err = cr.rdb.Del(context.TODO(), workerAddrKey).Err()
	if err != nil {
		return fmt.Errorf("failed to delete worker addr <%v>: %w", workerAddrKey, err)
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
	if route.RouteID == "" {
		return fmt.Errorf("route id is required")
	}
	if route.UpdatedAt == 0 {
		route.UpdatedAt = time.Now().Unix()
	}
	data, err := json.Marshal(route)
	if err != nil {
		return fmt.Errorf("failed to marshal backend route %s: %w", route.RouteID, err)
	}
	if err := cr.rdb.Set(ctx, common.RedisKeys.SchedulerBackendRoute(route.RouteID), data, 0).Err(); err != nil {
		return fmt.Errorf("failed to set backend route %s: %w", route.RouteID, err)
	}
	if route.ContainerID != "" {
		if err := cr.rdb.SAdd(ctx, common.RedisKeys.SchedulerBackendRouteIndex(route.ContainerID), route.RouteID).Err(); err != nil {
			return fmt.Errorf("failed to index backend route %s: %w", route.RouteID, err)
		}
	}
	if route.WorkspaceID != "" && route.PoolName != "" && route.MachineID != "" {
		if err := cr.rdb.SAdd(ctx, common.RedisKeys.SchedulerBackendRouteMachineIndex(route.WorkspaceID, route.PoolName, route.MachineID), route.RouteID).Err(); err != nil {
			return fmt.Errorf("failed to index backend route %s by machine: %w", route.RouteID, err)
		}
		if err := cr.touchBackendRouteMachine(ctx, route.WorkspaceID, route.PoolName, route.MachineID); err != nil {
			return fmt.Errorf("failed to update backend route machine revision %s: %w", route.RouteID, err)
		}
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
	pipe := cr.rdb.Pipeline()
	for _, routeID := range routeIDs {
		pipe.Del(ctx, common.RedisKeys.SchedulerBackendRoute(routeID))
	}
	for _, route := range routes {
		if route.WorkspaceID == "" || route.PoolName == "" || route.MachineID == "" {
			continue
		}
		pipe.SRem(ctx, common.RedisKeys.SchedulerBackendRouteMachineIndex(route.WorkspaceID, route.PoolName, route.MachineID), route.RouteID)
		machines[machineKey{workspaceID: route.WorkspaceID, poolName: route.PoolName, machineID: route.MachineID}] = struct{}{}
	}
	for machine := range machines {
		pipe.Incr(ctx, common.RedisKeys.SchedulerBackendRouteMachineRevision(machine.workspaceID, machine.poolName, machine.machineID))
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
	return nil
}

// DeleteBackendRoutesByMachine removes all backend routes and index entries
// for a machine when it is released.
func (cr *ContainerRedisRepository) DeleteBackendRoutesByMachine(ctx context.Context, workspaceID, poolName, machineID string) error {
	if workspaceID == "" || poolName == "" || machineID == "" {
		return nil
	}
	indexKey := common.RedisKeys.SchedulerBackendRouteMachineIndex(workspaceID, poolName, machineID)
	routeIDs, err := cr.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return err
	}
	routes, err := cr.routes(ctx, routeIDs)
	if err != nil {
		return err
	}

	pipe := cr.rdb.Pipeline()
	for _, routeID := range routeIDs {
		pipe.Del(ctx, common.RedisKeys.SchedulerBackendRoute(routeID))
	}
	for _, route := range routes {
		if route.ContainerID != "" {
			pipe.SRem(ctx, common.RedisKeys.SchedulerBackendRouteIndex(route.ContainerID), route.RouteID)
		}
	}
	pipe.Del(ctx, indexKey)
	pipe.Incr(ctx, common.RedisKeys.SchedulerBackendRouteMachineRevision(workspaceID, poolName, machineID))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return cr.publishBackendRouteMachine(ctx, workspaceID, poolName, machineID)
}

func (cr *ContainerRedisRepository) touchBackendRouteMachine(ctx context.Context, workspaceID, poolName, machineID string) error {
	if workspaceID == "" || poolName == "" || machineID == "" {
		return nil
	}
	if err := cr.rdb.Incr(ctx, common.RedisKeys.SchedulerBackendRouteMachineRevision(workspaceID, poolName, machineID)).Err(); err != nil {
		return err
	}
	return cr.publishBackendRouteMachine(ctx, workspaceID, poolName, machineID)
}

func (cr *ContainerRedisRepository) publishBackendRouteMachine(ctx context.Context, workspaceID, poolName, machineID string) error {
	return cr.rdb.Publish(ctx, common.RedisKeys.SchedulerBackendRouteMachineRevision(workspaceID, poolName, machineID), common.KeyOperationSet).Err()
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

func (c *ContainerRedisRepository) SetContainerStateWithConcurrencyLimit(quota *types.ConcurrencyLimit, request *types.ContainerRequest) error {
	reservedConcurrency := quota != nil
	if reservedConcurrency {
		if err := c.reserveContainerConcurrency(quota, request); err != nil {
			return err
		}
	}

	err := c.SetContainerState(request.ContainerId, &types.ContainerState{
		ContainerId: request.ContainerId,
		StubId:      request.StubId,
		Status:      types.ContainerStatusPending,
		WorkspaceId: request.WorkspaceId,
		ScheduledAt: time.Now().Unix(),
		StartedAt:   0,
		Gpu:         request.Gpu,
		GpuCount:    request.GpuCount,
		Cpu:         request.Cpu,
		Memory:      request.Memory,
	})
	if err != nil {
		if reservedConcurrency {
			if releaseErr := c.releaseContainerConcurrencyReservation(context.TODO(), request.WorkspaceId, request.ContainerId); releaseErr != nil {
				return errors.Join(err, fmt.Errorf("failed to release concurrency reservation after container state error: %w", releaseErr))
			}
		}
		return err
	}

	return nil
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
	if err := c.ensureWorkspaceConcurrencyCounter(request.WorkspaceId); err != nil {
		return err
	}

	reason, err := c.tryReserveContainerConcurrency(quota, request)
	if errors.Is(err, errConcurrencyCounterRepairing) {
		if err := c.ensureWorkspaceConcurrencyCounter(request.WorkspaceId); err != nil {
			return err
		}
		reason, err = c.tryReserveContainerConcurrency(quota, request)
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

	reason, err = c.tryReserveContainerConcurrency(quota, request)
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
	ctx := context.TODO()
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

func (cr *ContainerRedisRepository) HasBuildContainerTTL(containerId string) bool {
	return cr.rdb.Exists(context.TODO(), common.RedisKeys.ImageBuildContainerTTL(containerId)).Val() != 0
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
