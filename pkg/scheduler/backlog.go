package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type RequestBacklog struct {
	rdb   *common.RedisClient
	ready chan struct{}
	// now is the clock delays are measured on. Nil means the wall clock.
	now func() time.Time
}

func (rb *RequestBacklog) clockNow() time.Time {
	if rb.now == nil {
		return time.Now()
	}
	return rb.now()
}

// popReadyBacklogScript fills the batch from the foreground lane first, then
// the background lane. The last element is the remaining depth of both lanes.
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
local depth = 0
for _, key in ipairs(KEYS) do
	depth = depth + redis.call("ZCARD", key)
end
out[#out + 1] = depth
return out
`)

func NewRequestBacklog(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb, ready: make(chan struct{}, 1)}
}

// PushAfter puts a request in its lane, to be taken up once delay has
// passed. With no delay it takes the place its timestamp gives it.
func (rb *RequestBacklog) PushAfter(request *types.ContainerRequest, delay time.Duration) error {
	// A request that waits for a worker is not on a machine: only a pin its
	// caller set limits where it runs next.
	waiting := *request
	waiting.ReleaseMachine()
	jsonData, err := json.Marshal(&waiting)
	if err != nil {
		return err
	}

	readyAt := rb.clockNow().Add(delay)
	if delay == 0 && !request.Timestamp.IsZero() {
		readyAt = request.Timestamp
	}

	ctx := context.TODO()
	pipe := rb.rdb.Pipeline()
	pipe.ZAdd(ctx, repo.BacklogLane(request), redis.Z{Score: float64(readyAt.UnixNano()), Member: jsonData})
	foreground := pipe.ZCard(ctx, common.RedisKeys.SchedulerContainerRequests())
	background := pipe.ZCard(ctx, common.RedisKeys.SchedulerBackgroundRequests())
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	if delay <= 0 {
		select {
		case rb.ready <- struct{}{}:
		default:
		}
	}
	metrics.RecordSchedulerBacklogDepth(foreground.Val() + background.Val())
	return nil
}

// PopN takes up to count requests that are ready, the oldest first. It
// returns none, and no error, when none is ready.
func (rb *RequestBacklog) PopN(count int64) ([]*types.ContainerRequest, error) {
	result, err := popReadyBacklogScript.Run(
		context.TODO(),
		rb.rdb,
		[]string{common.RedisKeys.SchedulerContainerRequests(), common.RedisKeys.SchedulerBackgroundRequests()},
		rb.clockNow().UnixNano(),
		count,
	).Result()
	if err != nil {
		return nil, err
	}

	items, ok := result.([]interface{})
	if !ok || len(items) == 0 {
		return nil, fmt.Errorf("unexpected backlog pop result: %T", result)
	}

	depth, ok := items[len(items)-1].(int64)
	if !ok {
		return nil, fmt.Errorf("unexpected backlog depth type: %T", items[len(items)-1])
	}
	items = items[:len(items)-1]
	metrics.RecordSchedulerBacklogDepth(depth)

	// The script has taken these out of the backlog, so each is read on its
	// own: one that cannot be read is lost, and the rest are not lost with
	// it.
	var requests []*types.ContainerRequest
	for _, item := range items {
		request, err := decodeBacklogEntry(item)
		if err != nil {
			// The entry itself stays out of the log: a request can hold its
			// workspace's secrets.
			requestLog(log.Error(), request).Err(err).Msg("dropped a backlog entry that could not be read")
			continue
		}
		requests = append(requests, request)
	}

	return requests, nil
}

// decodeBacklogEntry reads one member of a backlog lane. With an error it
// still returns the fields it could read, which name the request in the log.
func decodeBacklogEntry(item interface{}) (*types.ContainerRequest, error) {
	request := &types.ContainerRequest{}
	member, ok := item.(string)
	if !ok {
		return request, fmt.Errorf("unexpected backlog request type: %T", item)
	}
	return request, json.Unmarshal([]byte(member), request)
}
