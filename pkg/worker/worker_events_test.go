package worker

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestHandleWorkerEventStopsOwnedContainer(t *testing.T) {
	worker := &Worker{
		workerId:           "worker-1",
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		stopContainerChan:  make(chan stopContainerEvent, 1),
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		Id: "container-1",
		Request: &types.ContainerRequest{
			ContainerId: "container-1",
		},
	})

	worker.handleWorkerEvent(&pb.WorkerEvent{
		EventId: "event-1",
		Event: &pb.WorkerEvent_StopContainer{
			StopContainer: &pb.StopContainerEvent{
				ContainerId: "container-1",
				Force:       true,
				Reason:      string(types.StopContainerReasonUser),
			},
		},
	})

	instance, ok := worker.containerInstances.Get("container-1")
	require.True(t, ok)
	require.Equal(t, types.StopContainerReasonUser, instance.StopReason)

	select {
	case event := <-worker.stopContainerChan:
		require.Equal(t, "container-1", event.ContainerId)
		require.True(t, event.Kill)
	default:
		t.Fatal("expected stop container event")
	}
}

func TestHandleWorkerEventIgnoresUnknownContainerStop(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		stopContainerChan:  make(chan stopContainerEvent, 1),
	}

	worker.handleWorkerEvent(&pb.WorkerEvent{
		EventId: "event-1",
		Event: &pb.WorkerEvent_StopContainer{
			StopContainer: &pb.StopContainerEvent{
				ContainerId: "container-1",
				Force:       true,
			},
		},
	})

	select {
	case event := <-worker.stopContainerChan:
		t.Fatalf("unexpected stop container event: %+v", event)
	default:
	}
}

func TestHandleWorkerEventIgnoresHeartbeat(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		stopContainerChan:  make(chan stopContainerEvent, 1),
	}

	worker.handleWorkerEvent(&pb.WorkerEvent{
		EventId: types.WorkerEventHeartbeatID,
	})

	select {
	case event := <-worker.stopContainerChan:
		t.Fatalf("unexpected stop container event: %+v", event)
	default:
	}
}

func TestHandleWorkerEventCancelsMatchingBuild(t *testing.T) {
	worker := &Worker{
		buildCancels: common.NewSafeMap[context.CancelFunc](),
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker.registerBuildCancel("build-1", cancel)

	worker.handleWorkerEvent(&pb.WorkerEvent{
		EventId: "event-1",
		Event: &pb.WorkerEvent_StopBuild{
			StopBuild: &pb.StopBuildEvent{ContainerId: "build-1"},
		},
	})

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected build context to be cancelled")
	}
}

func TestCancelBuildIfAlreadyStoppingCancelsContext(t *testing.T) {
	repoClient := &fakeContainerRepoClient{
		state: &pb.ContainerState{
			ContainerId: "build-1",
			Status:      string(types.ContainerStatusStopping),
		},
	}
	worker := &Worker{containerRepoClient: repoClient}
	ctx, cancel := context.WithCancel(context.Background())

	worker.cancelBuildIfAlreadyStopping(ctx, cancel, "build-1")

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected build context to be cancelled")
	}
}
