package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

type RequestBacklog struct {
	rdb   *common.RedisClient
	ready chan struct{}
}

var (
	ErrStateVolumePlanOutboxMissing = errors.New("state-volume attachment plan outbox is missing")
	ErrStateVolumePlanIdentity      = errors.New("state-volume attachment plan identity mismatch")
)

const stateVolumePlanEnqueuedField = "state_volume_plan_enqueued"
const stateVolumeProcessingLease = 2 * time.Minute

var promoteStateVolumePlanScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) ~= 1 then return -3 end
if redis.call("HGET", KEYS[2], "state_volume_plan_id") ~= ARGV[1] then return -2 end
if redis.call("HGET", KEYS[1], "request_hash") ~= redis.call("HGET", KEYS[2], "state_volume_plan_hash") then return -2 end
local enqueued = redis.call("HGET", KEYS[2], ARGV[2])
if enqueued then
  if enqueued == ARGV[1] then return 0 end
  return -2
end
local payload = redis.call("HGET", KEYS[1], "payload")
local ready_at = redis.call("HGET", KEYS[1], "ready_at")
if not payload or not ready_at then return -1 end
redis.call("ZADD", KEYS[3], "NX", ready_at, payload)
redis.call("HSET", KEYS[4], payload, ARGV[1])
redis.call("HSET", KEYS[2], ARGV[2], ARGV[1])
return 1
`)

var popReadyBacklogScript = redis.NewScript(`
local requests = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[2])
if #requests == 0 then
	return requests
end
redis.call("ZREM", KEYS[1], unpack(requests))
for _, payload in ipairs(requests) do
  local plan_id = redis.call("HGET", KEYS[2], payload)
  if plan_id then redis.call("ZADD", KEYS[3], ARGV[3], plan_id) end
end
return requests
`)

var requeueStateVolumePlanScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) ~= 1 then return -3 end
if redis.call("HGET", KEYS[2], "state_volume_plan_id") ~= ARGV[1] then return -2 end
if redis.call("HGET", KEYS[1], "plan_id") ~= ARGV[1] or
   redis.call("HGET", KEYS[1], "container_id") ~= ARGV[2] or
   redis.call("HGET", KEYS[1], "request_hash") ~= redis.call("HGET", KEYS[2], "state_volume_plan_hash") then return -2 end
local status = redis.call("HGET", KEYS[2], "status")
local worker_id = redis.call("HGET", KEYS[2], "worker_id") or ""
if status ~= ARGV[3] or worker_id ~= "" then return -4 end
local payload = redis.call("HGET", KEYS[1], "payload")
if not payload then return -1 end
redis.call("ZADD", KEYS[3], ARGV[4], payload)
redis.call("HSET", KEYS[4], payload, ARGV[1])
redis.call("ZREM", KEYS[5], ARGV[1])
return 1
`)

var recoverStateVolumePlanScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) ~= 1 then return -3 end
if redis.call("HGET", KEYS[2], "state_volume_plan_id") ~= ARGV[1] or
   redis.call("HGET", KEYS[2], "state_volume_plan_hash") ~= ARGV[3] then return -2 end
local status = redis.call("HGET", KEYS[2], "status")
local worker_id = redis.call("HGET", KEYS[2], "worker_id") or ""
if status ~= ARGV[4] or worker_id ~= "" then return -4 end
if redis.call("HGET", KEYS[2], ARGV[5]) ~= ARGV[1] then return -2 end
if redis.call("HGET", KEYS[1], "plan_id") ~= ARGV[1] or
   redis.call("HGET", KEYS[1], "container_id") ~= ARGV[2] or
   redis.call("HGET", KEYS[1], "request_hash") ~= ARGV[3] then return -2 end
local payload = redis.call("HGET", KEYS[1], "payload")
if not payload then return -1 end
if redis.call("ZSCORE", KEYS[3], payload) then return 0 end
local processing_until = tonumber(redis.call("ZSCORE", KEYS[5], ARGV[1]) or "0")
if processing_until > tonumber(ARGV[6]) then return 2 end
redis.call("ZADD", KEYS[3], ARGV[6], payload)
redis.call("HSET", KEYS[4], payload, ARGV[1])
redis.call("ZREM", KEYS[5], ARGV[1])
return 1
`)

func NewRequestBacklog(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb, ready: make(chan struct{}, 1)}
}

func (rb *RequestBacklog) PromoteStateVolumePlan(planId, containerId string) (bool, error) {
	if rb == nil || rb.rdb == nil || planId == "" || containerId == "" {
		return false, ErrStateVolumePlanIdentity
	}
	result, err := promoteStateVolumePlanScript.Run(context.TODO(), rb.rdb, []string{
		common.RedisKeys.SchedulerStateVolumePlanOutbox(planId),
		common.RedisKeys.SchedulerContainerState(containerId),
		common.RedisKeys.SchedulerContainerRequests(),
		common.RedisKeys.SchedulerStateVolumePayloadPlans(),
	}, planId, stateVolumePlanEnqueuedField).Int()
	if err != nil {
		return false, err
	}
	switch result {
	case 0:
		return false, nil
	case 1:
		select {
		case rb.ready <- struct{}{}:
		default:
		}
		metrics.RecordSchedulerBacklogDepth(rb.rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val())
		return true, nil
	case -1:
		return false, ErrStateVolumePlanOutboxMissing
	case -3:
		return false, &types.ErrContainerStateNotFound{ContainerId: containerId}
	default:
		return false, ErrStateVolumePlanIdentity
	}
}

func (rb *RequestBacklog) RequeueStateVolumePlan(planId, containerId string, delay time.Duration) error {
	if rb == nil || rb.rdb == nil || planId == "" || containerId == "" {
		return ErrStateVolumePlanIdentity
	}
	readyAt := time.Now().Add(delay).UnixNano()
	result, err := requeueStateVolumePlanScript.Run(context.TODO(), rb.rdb, []string{
		common.RedisKeys.SchedulerStateVolumePlanOutbox(planId),
		common.RedisKeys.SchedulerContainerState(containerId),
		common.RedisKeys.SchedulerContainerRequests(),
		common.RedisKeys.SchedulerStateVolumePayloadPlans(),
		common.RedisKeys.SchedulerStateVolumeProcessing(),
	}, planId, containerId, string(types.ContainerStatusPending), readyAt).Int()
	if err != nil {
		return err
	}
	if result != 1 {
		return fmt.Errorf("requeue state-volume attachment plan: result %d", result)
	}
	if delay <= 0 {
		select {
		case rb.ready <- struct{}{}:
		default:
		}
	}
	return nil
}

// RecoverStateVolumePlan requeues only after the destructive-pop processing
// lease expired and Redis still proves the exact plan is pending/unassigned.
func (rb *RequestBacklog) RecoverStateVolumePlan(planId, containerId, requestHash string, now time.Time) (bool, error) {
	if rb == nil || rb.rdb == nil || planId == "" || containerId == "" || requestHash == "" {
		return false, ErrStateVolumePlanIdentity
	}
	result, err := recoverStateVolumePlanScript.Run(context.TODO(), rb.rdb, []string{
		common.RedisKeys.SchedulerStateVolumePlanOutbox(planId),
		common.RedisKeys.SchedulerContainerState(containerId),
		common.RedisKeys.SchedulerContainerRequests(),
		common.RedisKeys.SchedulerStateVolumePayloadPlans(),
		common.RedisKeys.SchedulerStateVolumeProcessing(),
	}, planId, containerId, requestHash, string(types.ContainerStatusPending), stateVolumePlanEnqueuedField, now.UnixNano()).Int()
	if err != nil {
		return false, err
	}
	switch result {
	case 0, 2:
		return false, nil
	case 1:
		select {
		case rb.ready <- struct{}{}:
		default:
		}
		return true, nil
	case -1:
		return false, ErrStateVolumePlanOutboxMissing
	case -3:
		return false, &types.ErrContainerStateNotFound{ContainerId: containerId}
	default:
		return false, ErrStateVolumePlanIdentity
	}
}

func (rb *RequestBacklog) AcknowledgeStateVolumePlanDispatch(planId string) error {
	if rb == nil || rb.rdb == nil || planId == "" {
		return ErrStateVolumePlanIdentity
	}
	return rb.rdb.ZRem(context.TODO(), common.RedisKeys.SchedulerStateVolumeProcessing(), planId).Err()
}

func (rb *RequestBacklog) StateVolumePlanOutboxExists(planId, containerId string) (bool, error) {
	if rb == nil || rb.rdb == nil || planId == "" || containerId == "" {
		return false, ErrStateVolumePlanIdentity
	}
	values, err := rb.rdb.HMGet(context.TODO(), common.RedisKeys.SchedulerStateVolumePlanOutbox(planId),
		"plan_id", "container_id", "request_hash", "payload", "ready_at").Result()
	if err != nil {
		return false, err
	}
	if len(values) != 5 || values[0] == nil {
		return false, nil
	}
	storedPlan, planOK := values[0].(string)
	storedContainer, containerOK := values[1].(string)
	requestHash, hashOK := values[2].(string)
	payload, payloadOK := values[3].(string)
	readyAt, readyOK := values[4].(string)
	if !planOK || !containerOK || !hashOK || !payloadOK || !readyOK || storedPlan != planId || storedContainer != containerId || requestHash == "" || payload == "" || readyAt == "" {
		return false, ErrStateVolumePlanIdentity
	}
	return true, nil
}

// Pushes a new container request into the sorted set
func (rb *RequestBacklog) Push(request *types.ContainerRequest) error {
	return rb.PushAfter(request, 0)
}

func (rb *RequestBacklog) PushAfter(request *types.ContainerRequest, delay time.Duration) error {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return err
	}

	readyAt := time.Now().Add(delay)
	if delay == 0 && !request.Timestamp.IsZero() {
		readyAt = request.Timestamp
	}

	if err := rb.rdb.ZAdd(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), redis.Z{Score: float64(readyAt.UnixNano()), Member: jsonData}).Err(); err != nil {
		return err
	}

	if delay <= 0 {
		select {
		case rb.ready <- struct{}{}:
		default:
		}
	}
	metrics.RecordSchedulerBacklogDepth(rb.rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val())
	return nil
}

// Pops the oldest container request from the sorted set
func (rb *RequestBacklog) Pop() (*types.ContainerRequest, error) {
	requests, err := rb.PopN(1)
	if err != nil {
		return nil, err
	}

	return requests[0], nil
}

// Pops the oldest container requests from the sorted set.
func (rb *RequestBacklog) PopN(count int64) ([]*types.ContainerRequest, error) {
	result, err := popReadyBacklogScript.Run(
		context.TODO(),
		rb.rdb,
		[]string{common.RedisKeys.SchedulerContainerRequests(), common.RedisKeys.SchedulerStateVolumePayloadPlans(), common.RedisKeys.SchedulerStateVolumeProcessing()},
		time.Now().UnixNano(),
		count,
		time.Now().Add(stateVolumeProcessingLease).UnixNano(),
	).Result()
	if err != nil {
		return nil, err
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected backlog pop result: %T", result)
	}

	if len(items) == 0 {
		return nil, errors.New("backlog empty")
	}

	requests := make([]*types.ContainerRequest, 0, len(items))
	for _, item := range items {
		member, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected backlog request type: %T", item)
		}

		var poppedItem types.ContainerRequest
		err = json.Unmarshal([]byte(member), &poppedItem)
		if err != nil {
			return nil, err
		}
		requests = append(requests, &poppedItem)
	}

	metrics.RecordSchedulerBacklogDepth(rb.rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val())
	return requests, nil
}

// Gets the length of the sorted set
func (rb *RequestBacklog) Len() int64 {
	return rb.rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val()
}
