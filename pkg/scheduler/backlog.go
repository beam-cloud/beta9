package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

type RequestBacklog struct {
	rdb *common.RedisClient
	mu  sync.Mutex
}

func NewRequestBacklog(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb}
}

// Pushes a new container request into the sorted set
func (rb *RequestBacklog) Push(request *types.ContainerRequest) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	jsonData, err := json.Marshal(request)
	if err != nil {
		return err
	}

	// Use the timestamp as the score for sorting
	timestamp := float64(request.Timestamp.UnixNano())
	return rb.rdb.ZAdd(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), redis.Z{Score: timestamp, Member: jsonData}).Err()
}

// Pops the oldest container request from the sorted set
func (rb *RequestBacklog) Pop() (*types.ContainerRequest, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	result, err := rb.rdb.ZPopMin(context.TODO(), common.RedisKeys.SchedulerContainerRequests(), 1).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, errors.New("backlog empty")
	}

	var poppedItem types.ContainerRequest
	err = json.Unmarshal([]byte(result[0].Member.(string)), &poppedItem)
	if err != nil {
		return nil, err
	}

	return &poppedItem, nil
}

// Gets the length of the sorted set
func (rb *RequestBacklog) Len() int64 {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return rb.rdb.ZCard(context.TODO(), common.RedisKeys.SchedulerContainerRequests()).Val()
}
