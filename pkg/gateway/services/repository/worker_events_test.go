package repository_services

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newWorkerEventBrokerForTest(t *testing.T) (*workerEventBroker, *common.EventBus) {
	t.Helper()

	server, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(server.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	broker := &workerEventBroker{
		ctx:      context.Background(),
		eventBus: common.NewEventBus(rdb),
		rdb:      rdb,
		sinks:    map[uint64]*workerEventSink{},
	}

	return broker, common.NewEventBus(rdb)
}

func TestWorkerEventBrokerFansOutStopContainerEvents(t *testing.T) {
	broker, eventBus := newWorkerEventBrokerForTest(t)

	sinkAID, sinkA := broker.register("worker-a")
	defer broker.unregister(sinkAID)
	sinkBID, sinkB := broker.register("worker-b")
	defer broker.unregister(sinkBID)

	args, err := types.StopContainerArgs{
		ContainerId: "container-1",
		Force:       true,
		Reason:      types.StopContainerReasonUser,
	}.ToMap()
	require.NoError(t, err)

	eventID, err := eventBus.Send(&common.Event{
		Type:          common.EventTypeStopContainer,
		Args:          args,
		LockAndDelete: false,
	})
	require.NoError(t, err)

	broker.handleRedisEventID(eventID)

	for _, sink := range []<-chan *pb.WorkerEvent{sinkA, sinkB} {
		event := receiveWorkerEvent(t, sink)
		require.Equal(t, eventID, event.EventId)
		stop := event.GetStopContainer()
		require.NotNil(t, stop)
		require.Equal(t, "container-1", stop.ContainerId)
		require.True(t, stop.Force)
		require.Equal(t, string(types.StopContainerReasonUser), stop.Reason)
	}
}

func TestWorkerEventBrokerConvertsStopBuildEvents(t *testing.T) {
	broker, eventBus := newWorkerEventBrokerForTest(t)

	sinkID, sink := broker.register("worker-a")
	defer broker.unregister(sinkID)

	eventID, err := eventBus.Send(&common.Event{
		Type:          common.EventTypeStopBuild,
		Args:          map[string]any{"container_id": "build-1"},
		LockAndDelete: false,
	})
	require.NoError(t, err)

	broker.handleRedisEventID(eventID)

	event := receiveWorkerEvent(t, sink)
	require.Equal(t, eventID, event.EventId)
	stopBuild := event.GetStopBuild()
	require.NotNil(t, stopBuild)
	require.Equal(t, "build-1", stopBuild.ContainerId)
}

func TestStreamWorkerEventsRejectsInvalidRequests(t *testing.T) {
	service := &WorkerRepositoryService{ctx: context.Background()}

	err := service.StreamWorkerEvents(&pb.StreamWorkerEventsRequest{}, nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	err = service.StreamWorkerEvents(&pb.StreamWorkerEventsRequest{WorkerId: "worker-a"}, nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func receiveWorkerEvent(t *testing.T, events <-chan *pb.WorkerEvent) *pb.WorkerEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker event")
		return nil
	}
}
