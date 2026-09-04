package repository

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type lifecyclePushClient struct {
	pb.WorkerRepositoryServiceClient
	mu       sync.Mutex
	calls    int
	requests []*pb.PushContainerLifecycleEventsRequest
}

func (c *lifecyclePushClient) PushContainerLifecycleEvents(_ context.Context, request *pb.PushContainerLifecycleEventsRequest, _ ...grpc.CallOption) (*pb.PushContainerLifecycleEventsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.requests = append(c.requests, request)
	if c.calls == 1 {
		return nil, errors.New("gateway unavailable")
	}
	return &pb.PushContainerLifecycleEventsResponse{Ok: true}, nil
}

func (c *lifecyclePushClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestWorkerLifecycleRelayRetriesUnacknowledgedBatch(t *testing.T) {
	client := &lifecyclePushClient{}
	relay := &workerLifecycleRelay{
		client:        client,
		workerID:      "worker-1",
		events:        make(chan types.EventContainerLifecycleSchema, 1),
		retryInterval: 10 * time.Millisecond,
		retryBudget:   time.Second,
	}
	go relay.run()

	require.True(t, relay.push(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleImageLoad,
		ContainerID: "container-1",
		StartTime:   time.Now(),
	}))
	require.Eventually(t, func() bool { return client.callCount() == 2 }, 2*time.Second, 10*time.Millisecond)

	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.requests, 2)
	require.Equal(t, client.requests[0].Events, client.requests[1].Events)
}

type poisonLifecyclePushClient struct {
	pb.WorkerRepositoryServiceClient
	mu           sync.Mutex
	acknowledged []*pb.PushContainerLifecycleEventsRequest
}

func (c *poisonLifecyclePushClient) PushContainerLifecycleEvents(_ context.Context, request *pb.PushContainerLifecycleEventsRequest, _ ...grpc.CallOption) (*pb.PushContainerLifecycleEventsResponse, error) {
	if len(request.Events) > 0 && strings.Contains(string(request.Events[0]), "container-poison") {
		return &pb.PushContainerLifecycleEventsResponse{Ok: false, ErrorMsg: "unauthorized worker lifecycle event"}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acknowledged = append(c.acknowledged, request)
	return &pb.PushContainerLifecycleEventsResponse{Ok: true}, nil
}

func (c *poisonLifecyclePushClient) acknowledgedContainer(containerID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, request := range c.acknowledged {
		for _, data := range request.Events {
			if strings.Contains(string(data), containerID) {
				return true
			}
		}
	}
	return false
}

func TestWorkerLifecycleRelayDropsBatchAfterRetryBudget(t *testing.T) {
	client := &poisonLifecyclePushClient{}
	relay := &workerLifecycleRelay{
		client:        client,
		workerID:      "worker-1",
		events:        make(chan types.EventContainerLifecycleSchema, 16),
		retryInterval: 5 * time.Millisecond,
		retryBudget:   50 * time.Millisecond,
	}
	go relay.run()

	require.True(t, relay.push(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleImageLoad,
		ContainerID: "container-poison",
		StartTime:   time.Now(),
	}))
	time.Sleep(100 * time.Millisecond)
	require.True(t, relay.push(types.EventContainerLifecycleSchema{
		ID:          types.ContainerLifecycleImageLoad,
		ContainerID: "container-good",
		StartTime:   time.Now(),
	}))

	require.Eventually(t, func() bool { return client.acknowledgedContainer("container-good") }, 3*time.Second, 10*time.Millisecond)
}

func TestWorkerLifecycleRelayRejectsFullQueue(t *testing.T) {
	relay := &workerLifecycleRelay{
		workerID: "worker-1",
		events:   make(chan types.EventContainerLifecycleSchema, 1),
	}
	relay.events <- types.EventContainerLifecycleSchema{}

	require.False(t, relay.push(types.EventContainerLifecycleSchema{}))
}
