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

// popReadyBacklogScript fills the batch from the foreground lane first, then the background lane.
var popReadyBacklogScript = redis.NewScript(`
local limit = tonumber(ARGV[2])
local out = {}
for _, key in ipairs(KEYS) do
	if limit <= 0 then
		break
	end
	local requests = redis.call("ZRANGEBYSCORE", key, "-inf", ARGV[1], "LIMIT", 0, limit)
	if #requests > 0 then
		redis.call("ZREM", key, unpack(requests))
		for _, request in ipairs(requests) do
			out[#out + 1] = request
		end
		limit = limit - #requests
	end
end
return out
`)

// lane is the backlog sorted set a request waits in.
func lane(request *types.ContainerRequest) string {
	if request.Evictable || request.OpportunisticOnly {
		return common.RedisKeys.SchedulerBackgroundRequests()
	}
	return common.RedisKeys.SchedulerContainerRequests()
}

func NewRequestBacklog(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb, ready: make(chan struct{}, 1)}
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

	if err := rb.rdb.ZAdd(context.TODO(), lane(request), redis.Z{Score: float64(readyAt.UnixNano()), Member: jsonData}).Err(); err != nil {
		return err
	}

	if delay <= 0 {
		select {
		case rb.ready <- struct{}{}:
		default:
		}
	}
	metrics.RecordSchedulerBacklogDepth(rb.Len())
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
		[]string{common.RedisKeys.SchedulerContainerRequests(), common.RedisKeys.SchedulerBackgroundRequests()},
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

	metrics.RecordSchedulerBacklogDepth(rb.Len())
	return requests, nil
}

// Len is the number of requests waiting in both lanes.
func (rb *RequestBacklog) Len() int64 {
	ctx := context.TODO()
	return rb.rdb.ZCard(ctx, common.RedisKeys.SchedulerContainerRequests()).Val() +
		rb.rdb.ZCard(ctx, common.RedisKeys.SchedulerBackgroundRequests()).Val()
}
