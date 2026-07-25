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

var popReadyBacklogScript = redis.NewScript(`
local requests = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[2])
if #requests == 0 then
	return requests
end
redis.call("ZREM", KEYS[1], unpack(requests))
return requests
`)

func NewRequestBacklog(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{
		rdb:   rdb,
		ready: make(chan struct{}, 1),
	}
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

	ctx := context.TODO()
	pipe := rb.rdb.Pipeline()
	add := pipe.ZAdd(ctx, common.RedisKeys.SchedulerContainerRequests(), redis.Z{Score: float64(readyAt.UnixNano()), Member: jsonData})
	depth := pipe.ZCard(ctx, common.RedisKeys.SchedulerContainerRequests())
	_, _ = pipe.Exec(ctx)
	if err := add.Err(); err != nil {
		return err
	}

	if delay <= 0 {
		rb.signalReady()
	}
	if err := depth.Err(); err == nil {
		metrics.RecordSchedulerBacklogDepth(depth.Val())
	}
	return nil
}

func (rb *RequestBacklog) signalReady() {
	if rb == nil || rb.ready == nil {
		return
	}
	select {
	case rb.ready <- struct{}{}:
	default:
	}
}

func (rb *RequestBacklog) readySignal() <-chan struct{} {
	if rb == nil {
		return nil
	}
	return rb.ready
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
		[]string{common.RedisKeys.SchedulerContainerRequests()},
		time.Now().UnixNano(),
		count,
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
