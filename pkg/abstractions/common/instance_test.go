package abstractions

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestConsumeScaleResultDoesNotBlockWhenChannelIsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &AutoscaledInstance{
		Ctx:            ctx,
		ScaleEventChan: make(chan int, 1),
		Stub:           &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeEndpointDeployment)}},
		StubConfig:     &types.StubConfigV1{Autoscaler: &types.Autoscaler{}},
	}
	instance.ScaleEventChan <- 1

	done := make(chan struct{})
	go func() {
		defer close(done)
		instance.ConsumeScaleResult(&AutoscalerResult{DesiredContainers: 3, ResultValid: true})
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ConsumeScaleResult blocked on a full scale channel")
	}

	select {
	case got := <-instance.ScaleEventChan:
		if got != 3 {
			t.Fatalf("scale event = %d, want latest desired container count", got)
		}
	default:
		t.Fatal("expected latest scale event to be queued")
	}
}

func TestConsumeScaleResultHonorsPodDeploymentMinimum(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &AutoscaledInstance{
		Ctx:            ctx,
		ScaleEventChan: make(chan int, 1),
		Stub:           &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypePodDeployment)}},
		StubConfig:     &types.StubConfigV1{Autoscaler: &types.Autoscaler{MinContainers: 2}},
	}

	instance.ConsumeScaleResult(&AutoscalerResult{DesiredContainers: 0, ResultValid: true})

	select {
	case got := <-instance.ScaleEventChan:
		if got != 2 {
			t.Fatalf("scale event = %d, want min container count", got)
		}
	default:
		t.Fatal("expected scale event to be queued")
	}
}

func TestConsumeScaleResultLetsServeScaleToZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &AutoscaledInstance{
		Ctx:            ctx,
		ScaleEventChan: make(chan int, 1),
		Stub:           &types.StubWithRelated{Stub: types.Stub{Type: types.StubType(types.StubTypeASGIServe)}},
		StubConfig:     &types.StubConfigV1{Autoscaler: &types.Autoscaler{MinContainers: 2}},
	}

	instance.ConsumeScaleResult(&AutoscalerResult{DesiredContainers: 0, ResultValid: true})

	select {
	case got := <-instance.ScaleEventChan:
		if got != 0 {
			t.Fatalf("scale event = %d, want serve to scale to zero", got)
		}
	default:
		t.Fatal("expected scale event to be queued")
	}
}

func TestConsumeContainerEventDoesNotBlockWhenChannelIsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := &AutoscaledInstance{
		Ctx:                ctx,
		ContainerEventChan: make(chan types.ContainerEvent, 1),
	}
	instance.ContainerEventChan <- types.ContainerEvent{ContainerId: "old", Change: 1}

	done := make(chan struct{})
	go func() {
		defer close(done)
		instance.ConsumeContainerEvent(types.ContainerEvent{ContainerId: "new", Change: 1})
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ConsumeContainerEvent blocked on a full event channel")
	}

	<-instance.ContainerEventChan

	select {
	case got := <-instance.ContainerEventChan:
		if got.ContainerId != "new" {
			t.Fatalf("container event = %q, want async queued event", got.ContainerId)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("async container event was not queued after channel space became available")
	}
}
