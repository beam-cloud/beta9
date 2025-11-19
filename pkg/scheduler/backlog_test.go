package scheduler

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func NewRequestBacklogForTest(rdb *common.RedisClient) *RequestBacklog {
	return &RequestBacklog{rdb: rdb}
}

func TestRequestBacklogOrdering(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)

	// Create some container requests with increasing timestamps
	req1 := &types.ContainerRequest{Timestamp: time.Unix(1, 0)}
	req2 := &types.ContainerRequest{Timestamp: time.Unix(2, 0)}
	req3 := &types.ContainerRequest{Timestamp: time.Unix(3, 0)}

	// Push them in out of order
	if err := rb.Push(req2); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req1); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}
	if err := rb.Push(req3); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}

	// Pop them and verify the order based on timestamp
	var poppedReq *types.ContainerRequest

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req1.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req1.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req2.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req2.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}

	poppedReq, err = rb.Pop()
	if err != nil {
		t.Fatalf("Could not pop request: %v", err)
	}
	if poppedReq.Timestamp.Unix() != req3.Timestamp.Unix() {
		t.Errorf("Expected timestamp %v, got %v", req3.Timestamp.Unix(), poppedReq.Timestamp.Unix())
	}
}

func TestPopBatchWithInvalidJSON(t *testing.T) {
	s, err := miniredis.Run()
	assert.NotNil(t, s)
	assert.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	assert.NotNil(t, redisClient)
	assert.NoError(t, err)

	rb := NewRequestBacklogForTest(redisClient)

	req1 := &types.ContainerRequest{Timestamp: time.Unix(1, 0)}
	if err := rb.Push(req1); err != nil {
		t.Fatalf("Could not push request: %v", err)
	}

	s.ZAdd(common.RedisKeys.SchedulerContainerRequests(), 2.0, "invalid json")

	_, err = rb.PopBatch(2)
	assert.Error(t, err)
}
