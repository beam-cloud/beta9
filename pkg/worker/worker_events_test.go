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
		containerCancels:   common.NewSafeMap[context.CancelFunc](),
		stopContainerChan:  make(chan stopContainerEvent, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker.registerContainerCancel("container-1", cancel)
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
	require.ErrorIs(t, ctx.Err(), context.Canceled)

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
		containerCancels: common.NewSafeMap[context.CancelFunc](),
	}
	ctx, cancel := context.WithCancel(context.Background())
	worker.registerContainerCancel("build-1", cancel)

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

func TestReconnectCancelsLocallyStoppingContainers(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		containerCancels:   common.NewSafeMap[context.CancelFunc](),
	}
	first, cancelFirst := context.WithCancel(context.Background())
	second, cancelSecond := context.WithCancel(context.Background())
	worker.registerContainerCancel("container-1", cancelFirst)
	worker.registerContainerCancel("container-2", cancelSecond)
	worker.containerInstances.Set("container-1", &ContainerInstance{ExitCode: -1, StopReason: types.StopContainerReasonUser})
	worker.containerInstances.Set("container-2", &ContainerInstance{ExitCode: -1, StopReason: types.StopContainerReasonUser})

	worker.cancelStoppingContainers()

	for _, done := range []<-chan struct{}{first.Done(), second.Done()} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("expected container context to be cancelled")
		}
	}
}

func TestStartupRegistrationRaceUsesLocalStopState(t *testing.T) {
	worker := &Worker{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
	}
	worker.containerInstances.Set("container-1", &ContainerInstance{
		ExitCode:   -1,
		StopReason: types.StopContainerReasonUser,
	})
	ctx, cancel := context.WithCancel(context.Background())

	worker.cancelContainerIfAlreadyStopping(cancel, "container-1")

	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
