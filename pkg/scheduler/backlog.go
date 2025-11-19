package scheduler

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

type RequestBacklog struct {
	rdb *common.RedisClient
}

func NewRequestBacklog(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb}
}

// Redis sorted set, no mutex needed
func (rb *RequestBacklog) Push(request *types.ContainerRequest) error {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return err
	}
	timestamp := float64(request.Timestamp.UnixNano())
	return rb.rdb.ZAdd(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), redis.Z{Score: timestamp, Member: jsonData}).Err()
}

func (rb *RequestBacklog) Pop() (*types.ContainerRequest, error) {
	result, err := rb.rdb.ZPopMin(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 1).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New("backlog empty")
	}

	var req types.ContainerRequest
	if err := json.Unmarshal([]byte(result[0].Member.(string)), &req); err != nil {
		return nil, err
	}

	return &req, nil
}

// PopBatch pops up to count requests from backlog atomically
func (rb *RequestBacklog) PopBatch(count int64) ([]*types.ContainerRequest, error) {
	result, err := rb.rdb.ZPopMin(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), count).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}

	requests := make([]*types.ContainerRequest, 0, len(result))
	for _, z := range result {
		var req types.ContainerRequest
		if err := json.Unmarshal([]byte(z.Member.(string)), &req); err != nil {
			return nil, err
		}
		requests = append(requests, &req)
	}
	return requests, nil
}

func (rb *RequestBacklog) Len() int64 {
	return rb.rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val()
}
